package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.bytes
import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetAddress.*
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.StandardProtocolFamily.*
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.ByteBuffer.*
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.DatagramChannel
import java.time.Instant.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.experimental.or
import kotlin.math.abs
import kotlin.random.Random
import kotlin.random.Random.Default.nextInt
import kotlin.text.Charsets.UTF_8
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds


val TESTTEST = ArrayList<Long>()

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