package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.StandardProtocolFamily.*
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer.*
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.DatagramChannel
import java.time.Instant.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.experimental.or
import kotlin.math.abs
import kotlin.random.Random
import kotlin.random.Random.Default.nextInt
import kotlin.text.Charsets.UTF_8
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

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

const val MASK_MID = (0b11L shl 62).inv()

val TESTTEST = ArrayList<Long>()

suspend fun Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Int, Long) -> (Unit),
    messages: suspend () -> (Long),
    slot: suspend () -> (Int),
) = withContext(dispatcher) {
    val f = (n / 2) - 1
    val channel = UDP(address, port,  65527)
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
        withTimeout(10.milliseconds) {
            val proposed = OP_PROPOSE shl 62 or messages()
            var current = slot()
            buffer.clear().putLong(proposed).putInt(current)
            channel.send(buffer.flip(), broadcast)
            var index = 0
            //create this lazily
            random = Random(current)
            while (index < majority) {
                channel.receive(buffer.clear())
                proposals[index] = buffer.getLong(0)
                if (proposals[index] shr 62 == OP_PROPOSE) {
                    val depth = buffer.getInt(8)
                    if (current < depth) current = depth
                    var count = 1
                    for (i in 0 until index) {
                        if (proposals[i] == proposals[index]) {
                            if (++count >= majority) {
                                val state = if (proposals[i] == proposed) STATE_ONE else STATE_ZERO
                                commit(current, phase(0, state, proposals[i])); return@withTimeout
                                //continue@outer
                            }
                        }
                    }
                    ++index;
                }
            }
            commit(current, phase(0, STATE_ZERO, -1))
        }
//        delay(1.milliseconds)
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
val COMPARATOR = compareBy<Long> { it and 0xFFFFFFFF }.thenBy { it shr 32 }
fun CoroutineScope.SMR(
    n: Int, nodes: Array<InetSocketAddress>,
    address: InetAddress, port: Int, tcp: Int,
    vararg pipes: Int,
    commit: (String) -> (Unit)
) {
    val log = AtomicLongArray(65536) //Filled with NONE
    val messages = ConcurrentHashMap<Long, String>()
    val committed = AtomicInteger(-1)
    val highest = AtomicInteger(-1)
    val using = ConcurrentSkipListSet<Int>()
    val instances = Array(pipes.size) { Node(10, COMPARATOR).apply {
        launch { try {
            delay(5.seconds)
            println("Size: $size Full: ${TESTTEST.size}")
            val mark = System.nanoTime()
            var last = -1L; var slot = it
            Node(pipes[it], address, n, { depth, id ->
//                println("$depth - $id != $last")
                println("Depth: $depth Pipe: $it")
                if (id != last)
                    println("Slightly out of sync!")
                if (id == 0L) { error("Trying to erase!") }
                if (depth < slot) { error("Trying to reinsert!") } //is this actually an issue?
                if (depth % pipes.size != it) { error("Trying to pipe mix!") }
                if (id != last) offer(last) else {
                    log[depth % log.length()] = id
                    //Update the highest index that contains a value.
                    var current: Int; do { current = highest.get() }
                    while (current < depth && !highest.compareAndSet(current, depth))
                    //Could potentially move slot forward by more than one increment
                    using.remove(slot)
                    slot = depth + pipes.size
                    //Will this be enough to keep the logs properly cleared?
                    messages.remove(log[slot])
                    log[slot] = 0L
                }
            }, {
                if (isEmpty()) println("Done in: ${(System.nanoTime() - mark).nanoseconds}")
                while ((slot - committed.get()) >= log.length()) {}
                take().also<Long> { last = it }
            }, {
                using.add(slot)
                println("Slot: $slot")
                slot
            })
        } catch (reason: Throwable) { reason.printStackTrace() } }
    } }
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val provider = SocketProvider(65536, group)
    val others = nodes
    suspend fun repair(start: Int, end: Int) {
        println("Repair: $start - $end")
        others.shuffle()
        others.firstOrNull { try {
            withTimeout(5.seconds) {
                provider.connect(it).apply {
                    write.int(start); write.int(end)
                    for (i in start..end) {
                        val id = read.long()
                        val bytes = read.bytes(read.short().toInt())
                        if (bytes.isNotEmpty())
                            messages[id] = bytes.toString(UTF_8)
                        log[i % log.length()] = id
                    }; close()
                }; true
            }
        } catch (_: Throwable) { false } }
    }
    suspend fun catchup() {
        val to = using.minOrNull()?.minus(1) ?: highest.get()
//        println("InUse: $using Committed: $committed To: $to Highest: ${highest.get()}")
        for (i in (committed.get() + 1)..to) {
            if (log[i] == -1L) continue
            val message = messages[log[i]]
            if (message != null) {
                commit(message)
                committed.set(i)
                continue
            }
            for (j in to downTo i + 1)
                if (log[j] == 0L || messages[log[j]] == null)
                    return repair(i, j)
            break
        }
    }
    launch { try {
        while (isActive) {
            delay(1.seconds)
            catchup()
        }
    } catch (reason: Throwable) { reason.printStackTrace() } }
    launch { try {
        val buffer = allocateDirect(64)
        val channel = UDP(address, port, 65527)
        while (channel.isOpen) {
            channel.receive(buffer.clear())
            val id = buffer.flip().long
            val bytes = ByteArray(buffer.int)
            buffer.get(bytes)
            messages[id] = bytes.toString(UTF_8)
            instances[abs(id % instances.size).toInt()].offer(id)
        }
    } catch (reason: Throwable) { reason.printStackTrace() } }
    launch { try {
        val thing = InetSocketAddress(address, tcp)
        while (provider.isOpen) {
            provider.accept(thing).apply { launch {
                val start = read.int()
                val end = read.int()
                for (i in start..end) {
                    val id = log[i % log.length()]
                    write.long(id)
                    val message = messages[id].orEmpty()
                    val bytes = message.toByteArray(UTF_8)
                    write.short(bytes.size.toShort())
                    write.bytes(bytes)
                }
            } }
        }
    } catch (reason: Throwable) { reason.printStackTrace()} }
}

fun main() {
    runBlocking(dispatcher) {
//        val test =  DatagramChannel.open(INET)
//        test.bind(InetSocketAddress(1000))
//        test.setOption(SO_SNDBUF, 65000)
//        test.setOption(SO_RCVBUF, 65000)
//        val times = LongArray(1000)
//        val client = true
//        val address = if (client) InetSocketAddress("192.168.10.54", 1000)
//        else InetSocketAddress("192.168.10.38", 1000)
//        //loopback
//        if (!client) {
//            val buffer = allocateDirect(12)
//            while (isActive) {
//                test.receive(buffer.clear())
//                test.send(buffer, address)
//            }
//        } else {
//            launch {
//                val buffer = allocateDirect(12)
//                var i = 0
//                while (isActive) {
//                    test.receive(buffer.clear())
//                    buffer.getInt(0)
//                    times[i++ % times.size] = System.nanoTime() - buffer.getLong(4)
//                }
//            }
//            launch {
//                while (isActive) {
//                    delay(1.seconds)
//                    println("Avg: ${times.average().nanoseconds}")
//                }
//            }
//            val buffer = allocateDirect(12)
//            while (isActive) {
//                delay(5.nanoseconds)
//                buffer.clear().putInt(10).putLong(System.nanoTime())
//                test.send(buffer.flip(), address)
//            }
//        }
//        println("Done!")
        val hostname = getLocalHost().hostName.split('.')[0]
        val address = getByName("192.168.1.${hostname.split('-')[1]}")
        println("localhost: $address")
        println("HOSTNAME: $hostname")
//        val nodes = Array(1) {
//            InetSocketAddress("192.168.10.54", 1000 + it)
//        }
        val nodes = emptyArray<InetSocketAddress>()
        for (i in 0 until 1) {
            //create a node that takes messages on 1000
            //and runs weak mvc instances on 2000-2002
            var index = 0
            SMR(3, nodes, address, 1000, 1000 + i, 2000) {
                println("${index++}: $it")
            }
        }
        val broadcast = InetSocketAddress(BROADCAST, 1000)
        val buffer = allocateDirect(64)
        val channel = UDP(address, 5000, 65000)

        val EPOCH = 1666745204552
        var test = 0
        fun submit(message: String): Long {
            val bytes = message.toByteArray(UTF_8)
            val time = now().toEpochMilli() - EPOCH
            val random = nextInt().toLong() shl 32
            test += bytes.size
            test += 8
            test += 4
            buffer.clear().putLong((time or random) and MASK_MID)
            buffer.putInt(bytes.size).put(bytes)
            channel.send(buffer.flip(), broadcast)
            TESTTEST.add((time or random) and MASK_MID)
            return time
        }
        delay(1.seconds)
        println("Starting!")
        if (hostname == "node-1") {
            val result = (0 until 100).map { i -> submit("") }
            println("Sent: $test")
            if (result != result.distinct()) println("No ordering!")
        }
    }
    println("Done!")
}