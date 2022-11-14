package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.base.Connection
import jdk.net.ExtendedSocketOptions.TCP_QUICKACK
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface.getByInetAddress
import java.net.StandardProtocolFamily.INET
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousChannelGroup.withThreadPool
import java.nio.channels.DatagramChannel
import kotlin.time.Duration.Companion.milliseconds

const val BROADCAST = "230.0.0.0" //230

interface Multicaster : AutoCloseable {
    suspend fun send(buffer: ByteBuffer)
    suspend fun receive(buffer: ByteBuffer)
    val isOpen: Boolean
}

fun UDP(
    address: InetAddress,
    port: Int, size: Int
): Multicaster {
    val broadcast = InetSocketAddress(BROADCAST, port)
    val channel = DatagramChannel.open(INET).apply {
        val network = getByInetAddress(address)
        setOption(SO_REUSEADDR, true)
        setOption(IP_MULTICAST_LOOP, true)
        setOption(IP_MULTICAST_IF, network)
        setOption(SO_SNDBUF, size)
        setOption(SO_RCVBUF, size)
        bind(InetSocketAddress(address, port))
        join(InetAddress.getByName(BROADCAST), network)
        configureBlocking(false)
    }
    return object : Multicaster, AutoCloseable by channel {
        override val isOpen = channel.isOpen
        override suspend fun send(buffer: ByteBuffer) {
            while (active() && channel.send(buffer, broadcast) == 0) { Thread.onSpinWait()}
        }
        override suspend fun receive(buffer: ByteBuffer)
            { while (active() && channel.receive(buffer) == null) { Thread.onSpinWait() } }
    }
}

suspend fun TCP(
    address: InetAddress,
    port: Int, size: Int,
    vararg nodes: InetAddress
): Multicaster {
    val group = withThreadPool(executor)
    val provider = SocketProvider(size, group) {
        it.setOption(SO_SNDBUF, size)
        it.setOption(SO_RCVBUF, size)
        it.setOption(TCP_NODELAY, true)
        it.setOption(TCP_QUICKACK, true)
    }
    val addresses = nodes.map { InetSocketAddress(it, port) }
    val connections = Array<Connection?>(nodes.size) { null }
    return object : Multicaster {
        val lock = Mutex(false)
        override val isOpen = provider.isOpen
        override fun close() = runBlocking { provider.close() }
        override suspend fun send(buffer: ByteBuffer) = lock.withLock {
            addresses.forEachIndexed { i, it ->
                if (connections[i]?.isOpen != true)
                    connections[i] = withTimeoutOrNull(20.milliseconds) {
                        provider.connect(it)
                    }
                connections[i]?.write?.buffer(buffer)
            }
        }

        override suspend fun receive(buffer: ByteBuffer) {
            TODO("Not yet implemented")
        }
    }
}