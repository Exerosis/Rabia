import kotlinx.coroutines.*
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.Dispatchers.IO
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.StandardProtocolFamily
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
import kotlin.time.Duration.Companion.seconds

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

suspend fun Node(
    n: Int, f: Int, random: Random,
    commit: suspend (ByteArray?) -> (Unit),
    commands: suspend () -> (ByteArray)
) = withContext(IO) {
    val channel = DatagramChannel.open(INET)
    val loopback = NetworkInterface.getByName("lo")
    channel.setOption(SO_REUSEADDR, true)
    channel.setOption(IP_MULTICAST_LOOP, true)
    channel.setOption(IP_MULTICAST_IF, loopback)
    channel.bind(InetSocketAddress("127.0.0.1", PORT))
    channel.join(getByName(BROADCAST), loopback)
    val broadcast = InetSocketAddress(BROADCAST, PORT)
    val buffer = ByteBuffer.allocateDirect(SIZE)
    val proposals = Array(n) { ByteArray(SIZE) }
    val majority = (n / 2) + 1
    var index: Int
    fun phase(
        p: Byte, state: Byte,
        common: ByteArray?
    ): ByteArray? {
        buffer.clear().put(state or p)
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
        println("Zero: $zero One: $one")
        return if (zero >= f + 1) null
        else if (one >= f + 1) common
        else phase((p + 1).toByte(), when {
            zero > 0 -> STATE_ZERO
            one > 0 -> STATE_ONE
            else -> if (random.nextBoolean())
                STATE_ZERO else STATE_ONE
        }, common)
    }
    while (channel.isOpen) {
        //Send proposals
        val command = commands()
        assert(command.size <= 64) { "Message too large!" }
        val propose = (OP_PROPOSE shl 6) or command.size
        buffer.clear().put(propose.toByte()).put(command)
        channel.send(buffer.flip(), broadcast)
        index = 0
        while (index < (n - f)) {
            channel.receive(buffer.clear())
            buffer.get(0, proposals[index])
            val mask = OP_PROPOSE shl 6
            if (proposals[index][0].toInt() and mask != 0)
                ++index
        }
        val request = (0 until index).map { proposals[it] }.find {
            proposals.count(it::contentEquals) >= majority
        }?.let { it.copyOfRange(1, (it[0].toInt() and 0b111111) + 1) }
        val state = if (request != null) STATE_ONE else STATE_ZERO
        commit(phase(0, state, request))
    }
}

fun main() = runBlocking {
    val random = Random(0L)
    val n = 10; val f = (n / 2) - 1
    val nodes = (0 until n).map { i ->
        PriorityBlockingQueue<Pair<Instant, String>>(10, compareBy { it.first }).apply {
            launch(Default) { try {
                Node(n, f, random, {
                    println("Node: $i - ${it?.toString(UTF_8)}")
                }, { take().second.toByteArray(UTF_8) })
            } catch (reason: Throwable) {
                reason.printStackTrace()
            } }
        }
    }
    (0..10).forEach { i ->
        nodes.forEach {
            it.offer(now() to "hello world $i")
        }
        delay(1.seconds)
    }
    println("Done!")
}