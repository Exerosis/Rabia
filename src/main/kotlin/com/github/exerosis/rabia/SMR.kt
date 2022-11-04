package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.math.abs
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

typealias Node = PriorityBlockingQueue<Long>

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
//            delay(5.seconds)
            println("Size: $size")
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
                while ((slot - committed.get()) >= log.length()) {}
                take().also<Long> { last = it }
            }, {
                using.add(slot)
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
                            messages[id] = bytes.toString(Charsets.UTF_8)
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
        val buffer = ByteBuffer.allocateDirect(64)
        val channel = UDP(address, port, 65527)
        while (channel.isOpen) {
            channel.receive(buffer.clear())
            val id = buffer.flip().long
            val bytes = ByteArray(buffer.int)
            buffer.get(bytes)
            messages[id] = bytes.toString(Charsets.UTF_8)
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
                    val bytes = message.toByteArray(Charsets.UTF_8)
                    write.short(bytes.size.toShort())
                    write.bytes(bytes)
                }
            } }
        }
    } catch (reason: Throwable) { reason.printStackTrace()} }
}