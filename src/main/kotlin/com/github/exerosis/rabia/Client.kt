package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.base.Connection
import com.github.exerosis.mynt.base.Write
import com.github.exerosis.mynt.bytes
import com.github.exerosis.mynt.component1
import com.github.exerosis.mynt.component2
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteBuffer.*
import java.nio.channels.AsynchronousChannelGroup
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

val EPOCH = 1666745204552

fun main() = runBlocking(dispatcher) {
    val addresses = arrayOf(
        InetSocketAddress("192.168.10.38", 1000),
        InetSocketAddress("192.168.10.38", 1001),
        InetSocketAddress("192.168.10.38", 1002),
//        InetSocketAddress("192.168.10.38", 1003),
//        InetSocketAddress("192.168.10.38", 1004),

//        InetSocketAddress("192.168.10.54", 1000),
//        InetSocketAddress("192.168.10.54", 1001),
//        InetSocketAddress("192.168.10.54", 1002),
    )
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val provider = SocketProvider(65536, group)
    val connections = addresses.map {
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
    println("Starting!")
    var i = 0
    while (true) {
        println("$i")
        submit("${i++}")
        delay(20.milliseconds)
    }
}