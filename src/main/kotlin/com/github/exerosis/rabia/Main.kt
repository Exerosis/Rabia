package com.github.exerosis.rabia

import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetAddress.getByName
import java.net.InetAddress.getLocalHost
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

const val DEBUG = false
const val SERVER = false

fun run() = runBlocking(dispatcher) {
    println("Hostname: $hostname")
    println("Current: ${current()}")
    println("Other: ${other()}")
    val address = getByName(current())
    val repairs = arrayOf(
        InetSocketAddress(other(), 2000),
        InetSocketAddress(other(), 2001)
    )
    val nodes = arrayOf(
        InetSocketAddress(current(), 3000),
        InetSocketAddress(current(), 3004),
        InetSocketAddress(other(), 3000),
        InetSocketAddress(other(), 3004)
    )

    val network = NetworkInterface.getByInetAddress(address)
    println("Interface: ${network.displayName}")

    for (i in 0 until 4) {
        //create a node that takes messages on 1000
        //and runs weak mvc instances on 2000-2002
        val processed = AtomicInteger(0)
        var index = 0
        SMR(4,
            repair=2000 + i, repairs,
            pipes=arrayOf(3000 + (i * 4)), nodes,
            port=1000, address
        ) {
            processed.incrementAndGet()
//            if ("$index" != it) error("IDk why this is happening :D")
            println("${index++}: $it")
        }

//        launch {
//            while (isActive) {
//                val count = processed.getAndSet(0)
//                println("Operations: $count - ${count / 15.0}/s")
//                delay(15.seconds)
//            }
//        }
    }
}
fun test() = runBlocking(dispatcher) {
    val address = getByName("10.0.2.15")
    println(address)
    val channel = UDP(address, 1000, 1000)
    launch {
        val buffer = ByteBuffer.allocateDirect(12)
        channel.receive(buffer)
        println("Got data!")
    }
    val buffer = ByteBuffer.allocateDirect(12)
    buffer.putInt(10).putLong(15L).flip()
    channel.send(buffer)
    println("Sent!")
}
fun test2() = runBlocking(dispatcher) {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val main = hostname == "DESKTOP-NJ3CTN8"
    val current =  if (main) "192.168.10.38" else "192.168.10.54"
    val address = getByName(current)
    println(address)
    val channel = UDP(address, 2000, 65000)
    if (main) {
        val buffer = ByteBuffer.allocateDirect(12)
        while (isActive) {
            println("Type something")
            readln()
            channel.receive(buffer.clear())
            println("Got data!")
        }
    } else {
        while (isActive) {
            val buffer = ByteBuffer.allocateDirect(12)
            buffer.putInt(10).putLong(15L).flip()
            channel.send(buffer)
            println("Sent!")
            delay(5.seconds)
        }
    }
}
fun test3() = runBlocking(dispatcher) {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val main = hostname == "DESKTOP-NJ3CTN8"
    val current =  if (main) "192.168.10.38" else "192.168.10.54"
    val address = getByName(current)
    println(address)
    val port = 1000; val size = 6000
    val channel = UDP(address, port, size)
    val test = launch {
        channel.receive(ByteBuffer.allocateDirect(19))
    }
    delay(1.seconds)
    println("Going to cancel!")
    test.cancel()

//
//    println(test)
    println("Done!")
}
fun main() = run()