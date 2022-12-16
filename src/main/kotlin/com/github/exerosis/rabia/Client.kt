package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.base.Connection
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.time.Instant
import kotlin.random.Random

val EPOCH = 1666745204552

fun main() = runBlocking(dispatcher) {
    val addresses = arrayOf(
//        InetSocketAddress(current(), 1000),
//        InetSocketAddress(current(), 1001),
//        InetSocketAddress(current(), 1002),
//        InetSocketAddress(current(), 1003),
//        InetSocketAddress(current(), 1004),

//        InetSocketAddress(other(), 1000),
//        InetSocketAddress(other(), 1001),
//        InetSocketAddress(other(), 1002),

        InetSocketAddress("192.168.1.1", 1000),
        InetSocketAddress("192.168.1.2", 1000),
        InetSocketAddress("192.168.1.3", 1000),
//        InetSocketAddress("192.168.1.4", 1000),
    )
    val addresses2 = arrayOf<InetSocketAddress>(
//        InetSocketAddress(current(), 1000),
//        InetSocketAddress(current(), 1001),
//        InetSocketAddress(current(), 1002),
//        InetSocketAddress(current(), 1003),
//        InetSocketAddress(current(), 1004),

//        InetSocketAddress(current(), 1002),
//        InetSocketAddress(current(), 1003),
//        InetSocketAddress(other(), 1002),
    )
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val provider = SocketProvider(65536, group)
    val connections = addresses.map {
        Channel<suspend Connection.() -> (Unit)>(10).apply {
            val connection = provider.connect(it)
            launch { consumeEach { it(connection) } }
        }
    }
    val seconds = addresses2.map {
        Channel<suspend Connection.() -> (Unit)>(10).apply {
            val connection = provider.connect(it)
            launch { consumeEach { it(connection) } }
        }
    }

    var test = 0
    suspend fun submit(message: String) {
        val bytes = message.toByteArray(Charsets.UTF_8)
        val time = Instant.now().toEpochMilli() - EPOCH
        val random = Random.nextInt().toLong() shl 32
        test += bytes.size
        test += 8
        test += 4
        connections.forEach { it.send {
            write.long((time or random) and MASK_MID)
            write.int(bytes.size); write.bytes(bytes)
        } }
    }
    suspend fun submit2(message: String) {
        val bytes = message.toByteArray(Charsets.UTF_8)
        val time = Instant.now().toEpochMilli() - EPOCH
        val random = Random.nextInt().toLong() shl 32
        test += bytes.size
        test += 8
        test += 4
        seconds.forEach { it.send {
            write.long((time or random) and MASK_MID)
            write.int(bytes.size); write.bytes(bytes)
        } }
    }
    println("Starting!")
    var i = 0
    while (i < 15) {
        println("$i")
        submit("${i++}")
    }
    delay(50_000_000L)
//    submit("hello")
//    submit2("world")
}