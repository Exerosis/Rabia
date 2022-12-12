@file:OptIn(ExperimentalTime::class)

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
import kotlin.streams.asSequence
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

const val DEBUG = true
const val WARN = true
const val SERVER = false

fun run() {
    runBlocking(dispatcher) {
        println("Hostname: $hostname")
        println("Current: ${current()}")
        println("Other: ${other()}")
        val address = NetworkInterface.networkInterfaces().asSequence().flatMap {
            it.inetAddresses.asSequence()
        }.find { println(it); "192.168.1" in it.toString() }!!
        println(address)
//    val repairs = arrayOf(
//        InetSocketAddress(other(), 2000),
//        InetSocketAddress(other(), 2001)
//    )
//    val nodes = arrayOf(
//        InetSocketAddress(current(), 3000),
//        InetSocketAddress(current(), 3004),
//        InetSocketAddress(other(), 3000),
//        InetSocketAddress(other(), 3004)
//    )
        val repairs = arrayOf(
            InetSocketAddress("192.168.1.1", 2000),
            InetSocketAddress("192.168.1.2", 2000),
            InetSocketAddress("192.168.1.3", 2000),
//        InetSocketAddress("192.168.1.4", 2000),
        )
        val nodes = arrayOf(
            InetSocketAddress("192.168.1.1", 3000),
            InetSocketAddress("192.168.1.2", 3000),
            InetSocketAddress("192.168.1.3", 3000),
//        InetSocketAddress("192.168.1.4", 3000),
        )

        val network = NetworkInterface.getByInetAddress(address)
        println("Interface: ${network.displayName}")

        for (i in 0 until 1) {
            //create a node that takes messages on 1000
            //and runs weak mvc instances on 2000-2002
            val processed = AtomicInteger(0)
            var index = 0
            SMR(3,
                repair=2000 + i, repairs,
                pipes=arrayOf(3000 + (i * 4)), nodes,
                port=1000 + i, address
            ) {
                processed.incrementAndGet()
//            if ("$index" != it) error("IDk why this is happening :D")
//            println("${index++}: $it")
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
    println("Exited Run")
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

fun test4() = runBlocking(dispatcher) {
    val address = NetworkInterface.networkInterfaces().asSequence().flatMap {
        it.inetAddresses.asSequence()
    }.find { println(it); "192.168.1" in it.toString() }!!
    println(address)
    if ("192.168.1.5" in address.toString()) {
        println("I'm 5")
        val test = TCP(address, 1000, 65000,
            InetSocketAddress("192.168.1.4", 1000)
        )
        println("Connected")
        test.send(ByteBuffer.allocateDirect(8).putLong(10L).flip())
        println("Sent")
    } else {
        println("i'm 4")
        val test = TCP(address, 1000, 65000,
            InetSocketAddress("192.168.1.5", 1000)
        )
        println("Connected")
        val buffer = ByteBuffer.allocateDirect(8)
        test.receive(buffer)
        println("Received: ${buffer.flip().getLong()}")
    }
}
fun main() = run()