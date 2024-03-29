package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.*
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.abs
import kotlin.math.roundToInt
import kotlin.text.Charsets.UTF_8
import kotlin.time.Duration.Companion.ZERO
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit.SECONDS
import kotlin.time.ExperimentalTime
import kotlin.time.TimeMark
import kotlin.time.TimeSource.Monotonic.markNow

typealias Node = PriorityBlockingQueue<Long>

@OptIn(ExperimentalTime::class)
class Profiler(period: Int, val name: String) {
    private val times = Array(period) { ZERO }
    private var mark: TimeMark? = null
    private var i = 0

    fun start() { mark = markNow() }
    fun end() {
        if (i % times.size == 0) println("$name: $this")
        if (mark != null)
            times[i++ % times.size] = mark!!.elapsedNow()
    }
    fun average() = times.reduce { acc, it -> it + acc } / times.size
    override fun toString() = "${average()}"
}

val COMPARATOR = compareBy<Long> { it and 0xFFFFFFFF }.thenBy { it shr 32 }
@OptIn(ExperimentalTime::class)
fun CoroutineScope.SMR(
    n: Int, address: InetAddress,
    nodes: Array<InetAddress>,
    queue: Int, repair: Int,
    vararg pipes: Int,
    commit: (String) -> (Unit)
) {
    val log = LongArray(65536 / pipes.size * pipes.size)
    val messages = ConcurrentHashMap<Long, String>()
    val committed = AtomicInteger(-1)
    val highest = AtomicInteger(-1)
    val using = ConcurrentSkipListSet<Int>()
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val provider = SocketProvider(65536, group)
    val instances = Array(pipes.size) { Node(10, COMPARATOR) }

    for (i in 0..COUNT) {
        val id = i.toLong()
        messages[id] = ""
        instances[abs(id % instances.size).toInt()].offer(id)
    }
//    instances.forEach { println(it.size) }

    suspend fun repair(start: Int, end: Int) {
        warn("Repair: $start - $end")
        val repairs = nodes.map { InetSocketAddress(it, repair) }.toMutableList()
        repairs.shuffle()
        repairs.firstOrNull {
            try {
                withTimeout(5.seconds) {
                    provider.connect(it).apply {
                        write.int(start); write.int(end)
                        for (i in start..end) {
                            val id = read.long()
                            val bytes = read.bytes(read.short().toInt())
                            if (bytes.isNotEmpty())
                                messages[id] = bytes.toString(UTF_8);
                            instances[abs(id % instances.size).toInt()].remove(id)
                            log[i % log.size] = id
                        }; close()
                    }; true
                }
            } catch (_: Throwable) {
                false
            }
        }
    }
    suspend fun catchup() {
        val to = using.minOrNull()?.minus(1) ?: highest.get()
//        println("InUse: $using Committed: $committed To: $to Highest: ${highest.get()}")
        for (i in (committed.get() + 1)..to) {
            val message = messages[log[i % log.size]]
            if (message != null) {
                commit(message)
                committed.set(i)
                continue
            }
            for (j in (to - pipes.size) downTo i + 1)
                if (log[j % log.size] == 0L || messages[log[j % log.size]] == null)
                    return repair(i, j)
            break
        }
    }
    val mark = AtomicReference(markNow())
    val count = AtomicInteger(0)
    val state = State(log.size, n)
    val bean = ManagementFactory.getPlatformMXBean(
        com.sun.management.OperatingSystemMXBean::class.java
    )
    instances.forEachIndexed { i, it -> it.apply {
        launch(CoroutineName("Pipe-%02d".format(i)) + dispatcher) { try {
            var last = -1L; var slot = i
            state.Node(i, pipes[i], address, n, { id ->
                if (isEmpty()) println("Done!")
                //TODO look into summing from a stats thread
                var amount = count.incrementAndGet()
                while (amount >= AVERAGE && !count.compareAndSet(amount, 0))
                    amount = count.get()
                if (amount >= AVERAGE) {
                    val duration = mark.getAndSet(markNow()).elapsedNow()
                    val throughput = amount / duration.toDouble(SECONDS)
                    val system = bean.cpuLoad
                    warn("%,d - %.2f - $size on ${executor.activeCount} of ${executor.poolSize}".format(throughput.roundToInt(), system))
                }
                if (id != last) {
                    error("Bad Sync: $id != $last")
                    offer(last)
                    if (id != 0L) return@Node
                    else repair(slot, slot)
                } else {
                    log[slot % log.size] = id
                    messages.remove(id)
                }

//                var current: Int; do { current = highest.get() }
//                while (current < slot && !highest.compareAndSet(current, slot))

                slot += pipes.size
                log[slot % log.size] = 0L
            }, {
                debug("Size: $size")
//                while (isActive && (slot - committed.get()) >= log.size) {}
                take().also<Long> { last = it }
            }, {
//                using.add(slot)
                slot
            }, *nodes)
        } catch (reason: Throwable) {
            reason.printStackTrace()
        }
            info("No Longer Running")
            cancel("Please everything die")
        }
    } }

//    launch { try {
//        while (isActive) {
//            delay(1.seconds)
////            catchup()
//        }
//        println("No Longer Catching Up")
//    } catch (reason: Throwable) { reason.printStackTrace() } }
//    launch { try {
//        val socket = InetSocketAddress(address, queue)
//        while (provider.isOpen && isActive) {
//            provider.accept(socket).apply { launch {
//                while (isActive && isOpen) {
//                    val id = read.long()
//                    messages[id] = read.bytes(read.int()).toString(UTF_8)
//                    val instance = instances[abs(id % instances.size).toInt()]
//                    instance.offer(id)
//                }; close()
//            } }
//        }
//        println("No Longer Accepting Messages")
//    } catch (reason: Throwable) { reason.printStackTrace() } }
//    launch { try {
//        val socket = InetSocketAddress(address, repair)
//        while (provider.isOpen) {
//            provider.accept(socket).apply { launch {
//                val start = read.int()
//                val end = read.int()
//                for (i in start..end) {
//                    val id = log[i % log.size]
//                    write.long(id)
//                    val message = messages[id].orEmpty()
//                    val bytes = message.toByteArray(UTF_8)
//                    write.short(bytes.size.toShort())
//                    write.bytes(bytes)
//                }
//            } }
//        }
//        println("No Longer Avaliable For Repair")
//    } catch (reason: Throwable) { reason.printStackTrace()} }
}