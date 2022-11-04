package com.github.exerosis.rabia

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteBuffer.*
import java.time.Instant
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

val EPOCH = 1666745204552

fun main() = runBlocking {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val current = if (hostname == "DESKTOP-NJ3CTN8") "192.168.10.38" else "192.168.10.54"
    val address = getByName(current)

    val broadcast = InetSocketAddress(BROADCAST, 1000)
    val buffer = allocateDirect(64)
    val channel = UDP(address, 1000, 65000)
    var test = 0
    fun submit(message: String): Long {
        val bytes = message.toByteArray(Charsets.UTF_8)
        val time = Instant.now().toEpochMilli() - EPOCH
        val random = Random.nextInt().toLong() shl 32
        test += bytes.size
        test += 8
        test += 4
        buffer.clear().putLong((time or random) and MASK_MID)
        buffer.putInt(bytes.size).put(bytes)
        channel.send(buffer.flip(), broadcast)
        return time
    }
    println("Starting!")
    var i = 0
    while (true) {
        println("$i")
        submit("${i++}")
        delay(20.milliseconds)
    }
}