package com.github.exerosis.rabia

import kotlinx.coroutines.*
import java.net.InetAddress.getByName
import java.net.InetAddress.getLocalHost
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

val executor: ExecutorService = Executors.newCachedThreadPool()
val dispatcher = executor.asCoroutineDispatcher()

fun main() = runBlocking(dispatcher) {
    val hostname = getLocalHost().hostName.split('.')[0]
    println("Hostname: $hostname")
    val main = hostname == "DESKTOP-NJ3CTN8"
    val current =  if (main) "192.168.10.38" else "192.168.10.54"
    val other = if (main) "192.168.10.54" else "192.168.10.38"

    val address = getByName(current)
    val nodes = arrayOf(InetSocketAddress(other, 1000))

    val network = NetworkInterface.getByInetAddress(address)
    println("Interface: ${network.name}")

    for (i in 0 until (if (main) 2 else 2)) { //(if (main) 2 else 3)
        //create a node that takes messages on 1000
        //and runs weak mvc instances on 2000-2002
        val processed = AtomicInteger(0)
        var index = 0
        SMR(4, nodes, address, 1000 + i, 2000 + (i * 4), 3000) {
            processed.incrementAndGet()
//            if ("$index" != it) error("IDk why this is happening :D")
//            println("${index++}: $it")
        }

        launch {
            while (isActive) {
                println("Operations: ${processed.getAndSet(0) / 30.0}/s")
                delay(30.seconds)
            }
        }
    }
}