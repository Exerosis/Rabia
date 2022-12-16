@file:OptIn(ExperimentalTime::class)

package com.github.exerosis.rabia

import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.util.concurrent.atomic.AtomicInteger
import kotlin.streams.asSequence
import kotlin.time.ExperimentalTime

const val DEBUG = false
const val WARN = false
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

            InetSocketAddress("192.168.1.1", 4000),
            InetSocketAddress("192.168.1.2", 4000),
            InetSocketAddress("192.168.1.3", 4000),
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
                pipes=arrayOf(3000 + (i * 4), 4000 + (i * 4)), nodes,
                port=1000 + i, address
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
    println("Exited Run")
}
fun main() = run()