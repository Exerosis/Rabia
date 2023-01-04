@file:OptIn(ExperimentalTime::class)

package com.github.exerosis.rabia

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetAddress.getByName
import java.net.NetworkInterface
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import kotlin.streams.asSequence
import kotlin.time.ExperimentalTime

const val INFO = false
const val DEBUG = false
const val WARN = true
const val COUNT = 10_000_000 - 1
const val AVERAGE = 10_000

val executor = Executors.newFixedThreadPool(16) as ThreadPoolExecutor
val dispatcher = executor.asCoroutineDispatcher()
val group = AsynchronousChannelGroup.withThreadPool(executor)!!

fun run() {
    runBlocking(dispatcher) {
        val hostname = InetAddress.getLocalHost().hostName.split('.')[0]
        val address = NetworkInterface.networkInterfaces().asSequence().flatMap {
            it.inetAddresses.asSequence()
        }.find { "192.168.1" in it.toString() }!!
        val network = NetworkInterface.getByInetAddress(address)
        println("Hostname: $hostname")
        println("Address: ${address.hostAddress}")
        println("Interface: ${network.displayName}")

        val nodes = arrayOf(
            getByName("192.168.1.1"),
            getByName("192.168.1.2"),
            getByName("192.168.1.3"),
        )

        SMR(3, address, nodes,
            queue=2000, repair=2001,
            pipes=IntArray(300) { 3000 + (it * 10) }
        ) {
//            println("${index++}: $it")
        }
    }
    println("Exited Run")
}
fun main() = run()