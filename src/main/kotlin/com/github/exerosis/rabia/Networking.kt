package com.github.exerosis.rabia

import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface.getByInetAddress
import java.net.SocketAddress
import java.net.StandardProtocolFamily.INET
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.concurrent.ConcurrentLinkedQueue

const val BROADCAST = "230.0.0.0" //230

interface Multicaster : AutoCloseable {
    suspend fun send(buffer: ByteBuffer)
    suspend fun receive(buffer: ByteBuffer): SocketAddress
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
            while (active() && channel.send(buffer, broadcast) == 0) { Thread.onSpinWait() }
        }
        override suspend fun receive(buffer: ByteBuffer) = TODO()
//            { while (active() && channel.receive(buffer) == null) { Thread.onSpinWait() } }
    }
}

suspend fun TCP(
    address: InetAddress,
    port: Int, size: Int,
    vararg addresses: InetSocketAddress
): Multicaster {
    val server = ServerSocketChannel.open()
    server.configureBlocking(false)
    server.bind(InetSocketAddress(address, port))
    val scope = CoroutineScope(dispatcher)
    val outbound = ConcurrentLinkedQueue<SocketChannel>()
    val inbound = ConcurrentLinkedQueue<SocketChannel>()
    scope.launch {
        while (server.isOpen && isActive)
            server.accept()?.apply {
                configureBlocking(false)
                setOption(SO_SNDBUF, size)
                setOption(SO_RCVBUF, size)
                setOption(TCP_NODELAY, true)
//                setOption(TCP_QUICKACK, true)
                inbound.add(this)
            }
    }
    addresses.map {
        scope.async { while (true) try {
            return@async outbound.add(SocketChannel.open(it).apply {
                configureBlocking(false)
                setOption(SO_SNDBUF, size)
                setOption(SO_RCVBUF, size)
                setOption(TCP_NODELAY, true)
//            setOption(TCP_QUICKACK, true)
            })
        } catch (_: Throwable) {}}
    }.forEach { it.await() }
    return object : Multicaster {
        override val isOpen = server.isOpen
        override fun close() = runBlocking { scope.cancel(); server.close() }
        override suspend fun send(buffer: ByteBuffer) {
//            withContext(Dispatchers.IO) {
                outbound.map {
                    val copy = buffer.duplicate()
                    scope.async {
                        try {
                            while (copy.hasRemaining()) {
                                it.write(copy)
                                Thread.onSpinWait()
                            }
                        } catch (reason: Throwable) {
                            reason.printStackTrace()
                        }
                    }
                }.awaitAll()
//            }
        }
        override suspend fun receive(buffer: ByteBuffer): SocketAddress {
            while (true) {
                inbound.forEach {
                    if (it.read(buffer) != 0) {
                        while (buffer.hasRemaining()) {
                            it.read(buffer)
                            Thread.onSpinWait()
                        }
                        return it.remoteAddress
                    }
                }
                Thread.onSpinWait()
            }
        }
    }
}