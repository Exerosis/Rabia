package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.abs
import kotlin.text.Charsets.UTF_8
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.TimeSource.Monotonic.markNow

typealias Node = PriorityBlockingQueue<Long>

val COMPARATOR = compareBy<Long> { it and 0xFFFFFFFF }.thenBy { it shr 32 }
@OptIn(ExperimentalTime::class)
fun CoroutineScope.SMR(
    n: Int,
    repair: Int, repairs: Array<InetSocketAddress>,
    pipes: Array<Int>, nodes: Array<InetSocketAddress>,
    port: Int, address: InetAddress,
    commit: (String) -> (Unit)
) {
    val log = AtomicLongArray(65536) //Filled with NONE
    val messages = ConcurrentHashMap<Long, String>()
    val committed = AtomicInteger(-1)
    val highest = AtomicInteger(-1)
    val using = ConcurrentSkipListSet<Int>()
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val provider = SocketProvider(65536, group)
    val instances = Array(pipes.size) { Node(10, COMPARATOR) }

    suspend fun repair(start: Int, end: Int) {
        warn("Repair: $start - $end")
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
                            log[i % log.length()] = id
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
    instances.forEachIndexed { i, it -> it.apply {
        val mark = AtomicReference(markNow())
        val count = AtomicInteger(0)
        launch(CoroutineName("Node-${port - 1000}")) { try {
            var last = -1L; var slot = i
            Node(pipes[i], address, n, { depth, id ->
                if (depth != slot) warn("DEPTH OFF: $depth != $slot")
//                println("$depth - $id != $last")
//                println("Depth: $depth Id: $id - ${messages[id]}")
                if (id != last)
                    warn("Bad Sync: $id != $last")
//                if (id == 0L) { error("Trying to erase!") }
                if (depth < slot) { error("Trying to reinsert!") } //is this actually an issue?
                if (depth % pipes.size != i) { error("Trying to pipe mix!") }
                if (id != last) {
                    offer(last)
                    if (depth > slot && id == 0L) repair(slot, depth)
                } else {
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
                    if (count.incrementAndGet() == 1000) {
                        val amount = count.getAndSet(0)
                        println("$amount in ${mark.getAndSet(markNow()).elapsedNow()}")
                    }
                }
            }, {
                log("Size: $size")
//                println("Size: $size")
                while ((slot - committed.get()) >= log.length()) {}
                take().also<Long> { last = it }
            }, {
                using.add(slot)
                slot
            }, *nodes)
        } catch (reason: Throwable) { reason.printStackTrace() }
            println("No Longer Running")
            cancel("Please everything die")
        }
    } }

    launch { try {
        while (isActive) {
            delay(1.seconds)
            catchup()
        }
        println("No Longer Catching Up")
    } catch (reason: Throwable) { reason.printStackTrace() } }
    launch { try {
        val socket = InetSocketAddress(address, port)
        while (provider.isOpen && isActive) {
            provider.accept(socket).apply { launch {
                while (isActive && isOpen) {
                    val id = read.long()
                    messages[id] = read.bytes(read.int()).toString(UTF_8)
                    val instance = instances[abs(id % instances.size).toInt()]
                    instance.offer(id)
                }; close()
            } }
        }
        println("No Longer Accepting Messages")
    } catch (reason: Throwable) { reason.printStackTrace() } }
    launch { try {
        val socket = InetSocketAddress(address, repair)
        while (provider.isOpen) {
            provider.accept(socket).apply { launch {
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
        println("No Longer Avaliable For Repair")
    } catch (reason: Throwable) { reason.printStackTrace()} }
}