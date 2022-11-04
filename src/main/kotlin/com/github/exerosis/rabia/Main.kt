package com.github.exerosis.rabia

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import java.net.InetAddress.getByName
import java.net.InetAddress.getLocalHost
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

val executor: ExecutorService = Executors.newCachedThreadPool()
val dispatcher = executor.asCoroutineDispatcher()

fun main() = runBlocking(dispatcher) {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val current = if (hostname == "DESKTOP-NJ3CTN8") "192.168.10.38" else "192.168.10.54"
    val other = if (hostname == "DESKTOP-NJ3CTN8") "192.168.10.54" else "192.168.10.38"

    val address = getByName(current)
    val nodes = arrayOf(InetSocketAddress(other, 1000))

    val network = NetworkInterface.getByInetAddress(address)
    println("Interface: $network")

    for (i in 0 until 1) {
        //create a node that takes messages on 1000
        //and runs weak mvc instances on 2000-2002
        var index = 0
        SMR(3, nodes, address, 1000, 1000 + i, 2000) {
                println("${index++}: $it")
        }
    }
    println("Done!")
}