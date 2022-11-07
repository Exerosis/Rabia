package com.github.exerosis.rabia

import kotlinx.coroutines.*
import java.net.InetAddress.getByName
import java.net.InetAddress.getLocalHost
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

val executor: ExecutorService = Executors.newCachedThreadPool()
val dispatcher = executor.asCoroutineDispatcher()

const val DEBUG = true
suspend fun log(message: String) {
    val ctx = currentCoroutineContext()[CoroutineName]
    if (DEBUG) {
        println("[${ctx?.name}] $message")
        System.out.flush()
    }
}

fun run() = runBlocking(dispatcher) {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val main = hostname == "DESKTOP-NJ3CTN8"
    val current =  if (main) "192.168.10.38" else "192.168.10.54"
    val other = if (main) "192.168.10.54" else "192.168.10.38"

    val address = getByName(current)
    val nodes = arrayOf(InetSocketAddress(other, 1000))

    val network = NetworkInterface.getByInetAddress(address)
    println("Interface: ${network.displayName}")

    for (i in 0 until (if (main) 2 else 2)) { //(if (main) 2 else 3)
        //create a node that takes messages on 1000
        //and runs weak mvc instances on 2000-2002
        val processed = AtomicInteger(0)
        var index = 0
        SMR(4, nodes, address, 1000 + i, 2000 + (i * 4), 3000) {
            processed.incrementAndGet()
//            if ("$index" != it) error("IDk why this is happening :D")
            println("${index++}: $it")
        }

        launch {
            while (isActive) {
                val count = processed.getAndSet(0)
                println("Operations: $count - ${count / 15.0}/s")
                delay(15.seconds)
            }
        }
    }
}
fun test() = runBlocking(dispatcher) {
    val address = getByName("192.168.10.254")
    println(address)
    val channel = UDP(address, 1000, 1000)
    launch {
        val buffer = ByteBuffer.allocateDirect(12)
        channel.receive(buffer)
        println("Got data!")
    }
    val buffer = ByteBuffer.allocateDirect(12)
    buffer.putInt(10).putLong(15L).flip()
    channel.send(buffer, InetSocketAddress(BROADCAST, 1000))
    println("Sent!")
}
fun test2() = runBlocking(dispatcher) {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val main = hostname == "DESKTOP-NJ3CTN8"
    val current =  if (main) "192.168.10.38" else "192.168.10.254"
    val address = getByName(current)
    println(address)
    val channel = UDP(address, 1000, 1000)
    if (main) {
        val buffer = ByteBuffer.allocateDirect(12)
        channel.receive(buffer)
        println("Got data!")
    } else {
        val buffer = ByteBuffer.allocateDirect(12)
        buffer.putInt(10).putLong(15L).flip()
        channel.send(buffer, InetSocketAddress(BROADCAST, 1000))
        println("Sent!")
    }
}

fun main() = test2()