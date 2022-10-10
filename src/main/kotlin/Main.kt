import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.*
import kotlinx.coroutines.Dispatchers.IO
import java.lang.Integer.max
import java.lang.Integer.min
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.StandardProtocolFamily.*
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer.*
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.DatagramChannel
import java.nio.channels.ServerSocketChannel
import java.time.Instant.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import kotlin.experimental.or
import kotlin.math.abs
import kotlin.random.Random
import kotlin.random.Random.Default.nextInt
import kotlin.text.Charsets.UTF_8
import kotlin.time.Duration.Companion.milliseconds

const val BROADCAST = "230.0.0.0"
const val SIZE = 12

const val OP_PROPOSE = 1L
const val OP_STATE = 2L
const val OP_VOTE = 3L
const val OP_LOST = 4L

const val STATE_ZERO = (OP_STATE shl 6).toByte()
const val STATE_ONE = (OP_STATE shl 6 or 32).toByte()

const val VOTE_ZERO = (OP_VOTE shl 6).toByte()
const val VOTE_ONE = (OP_VOTE shl 6 or 32).toByte()
const val VOTE_LOST = (OP_LOST shl 6).toByte()

const val MASK_MID = (0b11111L shl 58).inv()

suspend fun Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Int, Long) -> (Unit),
    messages: suspend () -> (Long),
    slot: suspend () -> (Int),
) = withContext(dispatcher) {
    val f = (n / 2) - 1
    val channel = UDP(address, port, SIZE * n * 50)
    val broadcast = InetSocketAddress(BROADCAST, port)
    val buffer = allocateDirect(SIZE)
    val majority = (n / 2) + 1
    val proposals = LongArray(majority)
    var random = Random(0)
    println("N: $n F: $f Majority: $majority")
    fun phase(p: Byte, state: Byte, common: Long): Long {
        buffer.clear().put(state or p)
        channel.send(buffer.flip(), broadcast)
        var zero = 0; var one = 0; var lost = 0;
        while ((zero + one) < majority) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                STATE_ONE or p -> ++one
                STATE_ZERO or p -> ++zero
            }
        }
        buffer.clear().put(when {
            zero >= majority -> VOTE_ZERO
            one >= majority -> VOTE_ONE
            else -> VOTE_LOST
        } or p)
        channel.send(buffer.flip(), broadcast)
        zero = 0; one = 0
        //TODO can we reduce the amount we wait for here.
        while ((zero + one + lost) < (n - f)) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                VOTE_ONE or p -> ++one
                VOTE_ZERO or p -> ++zero
                VOTE_LOST or p -> ++lost
            }
        }
        return if (zero >= f + 1) -1
        else if (one >= f + 1) common and MASK_MID
        else phase((p + 1).toByte(), when {
            zero > 0 -> STATE_ZERO
            one > 0 -> STATE_ONE
            else -> if (random.nextBoolean())
                STATE_ZERO else STATE_ONE
        }, common)
    }
    outer@ while (channel.isOpen) {
        val proposed = OP_PROPOSE shl 58 or messages()
        var current = slot()
        buffer.clear().putLong(proposed).putInt(current)
        channel.send(buffer.flip(), broadcast)
        var index = 0
        //create this lazily
        random = Random(current)
        while (index < majority) {
            channel.receive(buffer.clear())
            proposals[index] = buffer.getLong(0)
            if (proposals[index] shr 58 == OP_PROPOSE) {
                val depth = buffer.getInt(8)
                if (current < depth) continue
                if (current > depth) current = depth
                var count = 1
                for (i in 0 until index) {
                    if (proposals[i] == proposals[index]) {
                        if (++count >= majority) {
                            val state = if (proposals[i] == proposed) STATE_ONE else STATE_ZERO
                            commit(current, phase(0, state, proposals[i])); continue@outer
                        }
                    }
                }
                ++index;
            }
        }
        commit(current, phase(0, STATE_ZERO, -1))
    }
}

typealias Node = PriorityBlockingQueue<Long>

fun UDP(
    address: InetAddress,
    port: Int, size: Int
) = DatagramChannel.open(INET).apply {
    val network = NetworkInterface.getByInetAddress(address)
    setOption(SO_REUSEADDR, true)
    setOption(IP_MULTICAST_LOOP, true)
    setOption(IP_MULTICAST_IF, network)
    setOption(SO_SNDBUF, size)
    setOption(SO_RCVBUF, size)
    bind(InetSocketAddress(address, port))
    join(getByName(BROADCAST), network)
}

val executor: ExecutorService = Executors.newCachedThreadPool()
val dispatcher = executor.asCoroutineDispatcher()

suspend fun SMR(
    address: InetAddress, n: Int,
    port: Int, vararg pipes: Int,
    commit: (String) -> (Unit)
) = withContext(dispatcher) {
    val log = LongArray(65536) //Filled with NONE
    val messages = ConcurrentHashMap<Long, String>()
    var committed = -1 //probably at least volatile int.
    val nodes = Array(pipes.size) { Node(10, compareBy { it shr 32 }).apply {
        launch { try {
            var last = -1L; var slot = it
            Node(pipes[it], address, n, { depth, id ->
//                println("Depth: $depth Message: ${messages[id]}")
                assert(depth > slot) { "Trying to recommit!" }
                assert(depth % pipes.size == it) { "Trying to pipe mix!" }
                if (depth > slot) TODO("start catchup process.")
                if (id != last) offer(last) else {
                    log[depth] = id
                    if (depth == committed + 1) {
                        val message = messages[id]
                        if (message != null) {
                            commit(message)
                            committed = depth
                        } else TODO("Request message data!")
                    } else if (depth > committed) {
                        TODO("Do this iterative commit!")
                        //if we have everything before
                        //attempt to commit up to here.
                    }
                    //could potentially move slot forward by more than one increment
                    slot = depth + pipes.size
                }
            }, { take().also { last = it } }, { slot })
        } catch (reason: Throwable) { reason.printStackTrace() }}
    } }
    launch { try {
        val buffer = allocateDirect(64)
        val channel = UDP(address, port, buffer.capacity())
        while (channel.isOpen) {
            channel.receive(buffer.clear())
            val id = buffer.flip().long
            val bytes = ByteArray(buffer.int)
            buffer.get(bytes)
            messages[id] = bytes.toString(UTF_8)
            nodes[abs(id.hashCode()) % nodes.size].offer(id)
        }
    } catch (reason: Throwable) { reason.printStackTrace() } }
    launch { try {
        val group = AsynchronousChannelGroup.withThreadPool(executor)
        val provider = SocketProvider(65536, group)
        while (provider.isOpen) {
            provider.accept(InetSocketAddress(address, port)).apply {
                launch { while (isOpen) {
                    val start = read.int()
                    val end = read.int()
                    for (i in max(0, start)..min(end, log.size)) {
                        val message = messages[log[i]]
                        if (message != null) {
                            write.long(log[i])
                            val bytes = message.toByteArray(UTF_8)
                            write.short(bytes.size.toShort())
                            write.bytes(bytes)
                        }
                    }
                } }
            }
        }
    } catch (reason: Throwable) { reason.printStackTrace()} }
}

fun main() {
    runBlocking {
        val address = getLocalHost()
        for (i in 0 until 2) {
            //create a node that takes messages on 1000
            //and runs weak mvc instances on 2000-2002
            var index = 0
            SMR(address, 5, 1000, 2000) {
                println("${index++}: $it")
            }
        }
        val broadcast = InetSocketAddress(BROADCAST, 1000)
        val buffer = allocateDirect(64)
        val channel = UDP(address, 5000, buffer.capacity())

        val EPOCH = 1664855176503
        fun submit(message: String): Long {
            val bytes = message.toByteArray(UTF_8)
            val time = now().toEpochMilli() - EPOCH
            val random = nextInt().toLong() shl 32

            buffer.clear().putLong((time or random) and MASK_MID)
            buffer.putInt(bytes.size).put(bytes)
            channel.send(buffer.flip(), broadcast)
            return time
        }

//        val result = (0..4).map { i -> delay(1.milliseconds); submit("hello $i") }
//        if (result != result.distinct()) println("No ordering!")
    }
    println("Done!")
}