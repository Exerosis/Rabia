@file:OptIn(ExperimentalTime::class)

package com.github.exerosis.rabia

import kotlinx.coroutines.runBlocking
import java.net.InetAddress.getByName
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.util.concurrent.atomic.AtomicInteger
import kotlin.streams.asSequence
import kotlin.time.ExperimentalTime

const val INFO = true
const val DEBUG = false
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
            getByName("192.168.1.1"),
            getByName("192.168.1.2"),
            getByName("192.168.1.3"),
        )

        val network = NetworkInterface.getByInetAddress(address)
        println("Interface: ${network.displayName}")
        val processed = AtomicInteger(0)
        var index = 0
        SMR(3, address, nodes,
            queue=1000, repair=2000,
            pipes=intArrayOf(3000, 4000)
        ) {
            processed.incrementAndGet()
//            if ("$index" != it) error("IDk why this is happening :D")
            println("${index++}: $it")
        }
    }
    println("Exited Run")
}
fun main() = run()