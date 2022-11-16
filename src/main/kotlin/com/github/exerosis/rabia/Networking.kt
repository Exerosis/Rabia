package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.base.Connection
import jdk.net.ExtendedSocketOptions.TCP_QUICKACK
import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface.getByInetAddress
import java.net.StandardProtocolFamily.INET
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousChannelGroup.withThreadPool
import java.nio.channels.DatagramChannel

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
    val server = InetSocketAddress(address, port)
    val connections = ArrayList<Connection>()
    val scope = CoroutineScope(dispatcher)
    scope.launch {
        while (provider.isOpen && isActive)
            connections.add(provider.accept(server).apply {
                scope.launch {

                }
            })
    }
    addresses.map {
        scope.async { connections.add(provider.connect(it)) }
    }.forEach { it.await() }
    return object : Multicaster {
        override val isOpen = provider.isOpen
        override fun close() = runBlocking { scope.cancel(); provider.close() }
        override suspend fun send(buffer: ByteBuffer) {
            connections.map {
                scope.async { it.write.buffer(buffer) }
            }.awaitAll()
        }

        override suspend fun receive(buffer: ByteBuffer) {
            connections.forEach {
                it.read.buffer(buffer)
            }
        }
    }
}