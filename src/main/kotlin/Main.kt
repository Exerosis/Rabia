import kotlinx.coroutines.*
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.Dispatchers.IO
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.StandardProtocolFamily.*
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.time.Instant
import java.time.Instant.*
import java.util.concurrent.PriorityBlockingQueue
import kotlin.experimental.or
import kotlin.random.Random
import kotlin.text.Charsets.UTF_8
import kotlin.time.Duration.Companion.milliseconds

const val PORT = 5000
const val BROADCAST = "230.0.0.0"
const val SIZE = 65

const val OP_PROPOSE = 1
const val OP_STATE = 2
const val OP_VOTE = 3
const val OP_LOST = 4

const val STATE_ZERO = (OP_STATE shl 6).toByte()
const val STATE_ONE = (OP_STATE shl 6 or 32).toByte()

const val VOTE_ZERO = (OP_VOTE shl 6).toByte()
const val VOTE_ONE = (OP_VOTE shl 6 or 32).toByte()
const val VOTE_LOST = (OP_LOST shl 6).toByte()

object Network {
    fun allows() = Random.nextInt(100) < 100
    fun corrupts() = Random.nextInt(100) < 0
}
suspend fun Node(
    port: Int, address: InetAddress,
    n: Int, f: Int, random: Random,
    commit: suspend (ByteArray?) -> (Unit),
    commands: suspend () -> (ByteArray),
) = withContext(IO) {
    val network = NetworkInterface.getByInetAddress(address)
    val channel = DatagramChannel.open(INET)
    channel.setOption(SO_REUSEADDR, true)
    channel.setOption(IP_MULTICAST_LOOP, true)
    channel.setOption(IP_MULTICAST_IF, network)
    channel.setOption(SO_SNDBUF, SIZE * n * 20)
    channel.setOption(SO_RCVBUF, SIZE * n * 20)
    channel.bind(InetSocketAddress(address, port))
    channel.join(getByName(BROADCAST), network)
    val broadcast = InetSocketAddress(BROADCAST, port)
    val buffer = ByteBuffer.allocateDirect(SIZE)
    val proposals = Array(n) { ByteArray(SIZE) }
    val majority = (n / 2) + 1
    val proposal = ByteArray(SIZE)
    var index: Int
    fun phase(
        p: Byte, state: Byte,
        common: ByteArray?
    ): ByteArray? {
        buffer.clear().put(state or p)
        if (Network.allows())
            channel.send(buffer.flip(), broadcast)
        var zero = 0; var one = 0; var lost = 0;
        while ((zero + one) < (n - f)) {
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
        if (Network.allows())
            channel.send(buffer.flip(), broadcast)
        zero = 0; one = 0
        while ((zero + one + lost) < (n - f)) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                VOTE_ONE or p -> ++one
                VOTE_ZERO or p -> ++zero
                VOTE_LOST or p -> ++lost
            }
        }
        return if (zero >= f + 1) null
        else if (one >= f + 1) common
        else phase((p + 1).toByte(), when {
            zero > 0 -> STATE_ZERO
            one > 0 -> STATE_ONE
            else -> if (random.nextBoolean())
                STATE_ZERO else STATE_ONE
        }, common)
    }
    outer@ while (channel.isOpen) {
        //Send proposals
        val command = commands().clone()
        assert(command.size <= 64) { "Message too large!" }
        val propose = (OP_PROPOSE shl 6) or command.size
        if (Network.corrupts()) {
            command.shuffle()
            println("Corrupted!")
        }
        buffer.clear().put(propose.toByte()).put(command)
        buffer.get(0, proposal)
        if (Network.allows())
            channel.send(buffer.flip(), broadcast)
        index = 0
        while (index < (n - f)) {
            channel.receive(buffer.clear())
            buffer.get(0, proposals[index])
            val mask = OP_PROPOSE shl 6
            if (proposals[index][0].toInt() and mask != 0)
                ++index
        }
        var count = 0
        while (index-- > 0) {
            if (proposals[index].contentEquals(proposal))
                if (++count >= majority) {
                    commit(phase(0, STATE_ONE, command))
                    continue@outer
                }
        }
        commit(phase(0, STATE_ZERO, command))
    }
}

typealias Node = PriorityBlockingQueue<Pair<Instant, String>>

fun main() {
    runBlocking {
        val random = 0L
        val n = 5; val f = (n / 2) - 1
        val nodes = (0 until n).map { i ->
            Node(10, compareBy { it.first }).apply {
                launch(Default) { try {
                    var runs = 0
                    var order = 0
                    var last: Pair<Instant, String>? = null
                    val loopback = getLoopbackAddress()
                    Node(PORT, loopback, n, f, Random(random), {
                        runs++
                        if (it?.toString(UTF_8) != last!!.second)
                            offer(last!!)
                        else
                            println("Node: $i - ${it.toString(UTF_8)} - ${order++} - $runs")
                    }, {
                        last = take()
                        last!!.second.toByteArray(UTF_8)
                    })
                } catch (reason: Throwable) {
                    reason.printStackTrace()
                } }
            }
        }

        fun submit(message: String): Instant {
            val entry = now() to message
            nodes.forEach { it.offer(entry) }
            return entry.first
        }

        val result = (0..5).map { i -> delay(1.milliseconds); submit("hello $i") }
        if (result != result.distinct()) println("No ordering!")
    }
    println("Done!")
}