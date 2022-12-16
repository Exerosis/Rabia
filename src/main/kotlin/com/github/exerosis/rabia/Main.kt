@file:OptIn(ExperimentalTime::class)

package com.github.exerosis.rabia

import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetAddress.getByName
import java.net.NetworkInterface
import kotlin.streams.asSequence
import kotlin.time.ExperimentalTime

const val INFO = true
const val DEBUG = false
const val WARN = true

fun run() {
    runBlocking(dispatcher) {
        val hostname = InetAddress.getLocalHost().hostName.split('.')[0]
        val address = NetworkInterface.networkInterfaces().asSequence().flatMap {
            it.inetAddresses.asSequence()
        }.find { "192.168.1" in it.toString() }!!
        val network = NetworkInterface.getByInetAddress(address)
        println("Hostname: $hostname")
        println("Address: $address")
        println("Interface: ${network.displayName}")

        val nodes = arrayOf(
            getByName("192.168.1.1"),
            getByName("192.168.1.2"),
            getByName("192.168.1.3"),
        )


        SMR(3, address, nodes,
            queue=1000, repair=2000,
            pipes=intArrayOf(3000, 4000)
        ) {
//            if ("$index" != it) error("IDk why this is happening :D")
//            println("${index++}: $it")
        }
    }
    println("Exited Run")
}
fun main() = run()