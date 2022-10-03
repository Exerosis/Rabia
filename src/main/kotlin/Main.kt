import kotlinx.coroutines.*
import kotlinx.coroutines.Dispatchers.IO
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.StandardProtocolFamily.*
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer.*
import java.nio.channels.DatagramChannel
import java.time.Instant.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue
import kotlin.experimental.or
import kotlin.math.abs
import kotlin.random.Random
import kotlin.random.Random.Default.nextInt
import kotlin.random.Random.Default.nextLong
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

const val CORRUPTS = 0

data class MID(val least: Long, val most: Int)
suspend fun Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (MID?) -> (Unit),
    messages: suspend () -> (MID),
) = withContext(IO) {
    val random = Random(port)
    val f = (n / 2) - 1
    println("F: $f")
    val channel = UDP(address, port, SIZE * n * 500)
    val broadcast = InetSocketAddress(BROADCAST, port)
    val buffer = allocateDirect(SIZE)
    val heads = LongArray(n)
    val tails = IntArray(n)
    val majority = (n / 2) + 1
    println("Majority: $majority")
    fun phase(p: Byte, state: Byte, common: Int): MID? {
        buffer.clear().put(state or p)
        channel.send(buffer.flip(), broadcast)
        var zero = 0; var one = 0; var lost = 0;
        while ((zero + one) < majority) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                STATE_ONE or p -> ++one
                STATE_ZERO or p -> ++zero
            }
            println("States: $zero - $one")
        }
        buffer.clear().put(when {
            zero >= majority -> VOTE_ZERO
            one >= majority -> VOTE_ONE
            else -> VOTE_LOST
        } or p)
        channel.send(buffer.flip(), broadcast)
        zero = 0; one = 0
        //TODO can we reduce the amount we send here.
        while ((zero + one + lost) < (n - f)) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                VOTE_ONE or p -> ++one
                VOTE_ZERO or p -> ++zero
                VOTE_LOST or p -> ++lost
            }
        }
        println("Votes: $zero - $one - $lost")
        return if (zero >= f + 1) null
        else if (one >= f + 1) MID(heads[common] and MASK_MID, tails[common])
        else phase((p + 1).toByte(), when {
            zero > 0 -> STATE_ZERO
            one > 0 -> STATE_ONE
            else -> if (random.nextBoolean())
                STATE_ZERO else STATE_ONE
        }, common)
    }
    outer@ while (channel.isOpen) {
        val message = messages()
        val send = if (nextInt(100) >= CORRUPTS) message else
            MID(nextLong() and MASK_MID, nextInt())
        val propose = OP_PROPOSE shl 58 or send.least
        buffer.clear().putLong(propose).putInt(send.most)
        channel.send(buffer.flip(), broadcast)
        var index = 0;
        while (index <= majority) {
            channel.receive(buffer.clear())
            heads[index] = buffer.getLong(0)
            if (heads[index] shr 58 == OP_PROPOSE) {
                tails[index] = buffer.getInt(8)
                println("Got: ${heads[index]}")
                println("Have: ${send.least}")
                var count = 1
                for (i in 0 until index) {
                    if (heads[i] == heads[index] && tails[i] == tails[index]) {
                        println("Count: ${count}")
                        if (++count >= majority) {
                            val one = heads[i] and MASK_MID == send.least && tails[i] == send.most
                            println("Found and: $one")
                            commit(phase(0, if (one) STATE_ONE else STATE_ZERO, i))
                            continue@outer
                        }
                    }
                }
                ++index;
            }
        }
        println("Hopefully we aren't here!")
        //Here we could find no majority, so maybe we can shortcut.
        commit(phase(0, STATE_ZERO, -1))
    }
}

typealias Node = PriorityBlockingQueue<MID>

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

suspend fun CoroutineScope.SMR(
    address: InetAddress, n: Int,
    port: Int, vararg pipes: Int
) {
    val messages = ConcurrentHashMap<MID, String>()
    var committed = 0 //probably at least volatile int.
    val nodes = Array(pipes.size) { Node(10, compareBy { it.least }).apply {
        launch(IO) { try {
            var last: MID? = null; var slot = it
            Node(pipes[it], address, n, {
                if (it != last) offer(last!!) else {
                    println("Slot: $slot Message: ${messages[it]}")
                    //if we don't have the original message panic and go find it.
                    //commit in slot or something?
                    //if we are too far behind to commit catchup first.
                    if (slot <= committed + 1) {
                        //actually do the commit
                        //check for slots above us that
                        //have filled but not committed because of us.
                        committed = slot
                    }
                    slot += pipes.size
                }
            }, { take().also { last = it } })
        } catch (reason: Throwable) { reason.printStackTrace() }}
    } }
    launch(IO) { try {
        val buffer = allocateDirect(64)
        val channel = UDP(address, port, buffer.capacity())
        while (channel.isOpen) {
            channel.receive(buffer.clear())
            val id = MID(buffer.flip().long, buffer.int)
            println("Test: $id")
            val bytes = ByteArray(buffer.int)
            buffer.get(bytes)
            messages[id] = bytes.toString(UTF_8)
            nodes[abs(id.hashCode()) % nodes.size].offer(id)
        }
    } catch (reason: Throwable) { reason.printStackTrace() } }
}

fun main() {
    runBlocking(IO) {
        val address = getLocalHost()
        for (i in 0 until 1) {
            //create a node that takes messages on 1000
            //and runs weak mvc instances on 2000-2002
            SMR(address, 2, 1000, 2000)
        }
        val broadcast = InetSocketAddress(BROADCAST, 1000)
        val buffer = allocateDirect(64)
        val channel = UDP(address, 5000, buffer.capacity())

        suspend fun submit(message: String) = withContext(IO) {
            val bytes = message.toByteArray(UTF_8)
            val time = now().toEpochMilli()
            buffer.clear().putLong(time and MASK_MID).putInt(nextInt())
            buffer.putInt(bytes.size).put(bytes)
            channel.send(buffer.flip(), broadcast)
            return@withContext time
        }

        val result = (0..0).map { i -> delay(1.milliseconds); submit("hello $i") }
        if (result != result.distinct()) println("No ordering!")
    }
    println("Done!")
}