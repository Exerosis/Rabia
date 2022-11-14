package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import jdk.net.ExtendedSocketOptions.TCP_QUICKACK
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
    }
    return object : Multicaster, AutoCloseable by channel {
        override val isOpen = channel.isOpen
        override suspend fun send(buffer: ByteBuffer)
            { channel.send(buffer, broadcast) }
        override suspend fun receive(buffer: ByteBuffer)
            { channel.receive(buffer) }
    }
}

suspend fun TCP(address: InetAddress, port: Int, size: Int, vararg nodes: InetAddress) {
    val group = withThreadPool(executor)
    val provider = SocketProvider(size, group) {
        it.setOption(SO_SNDBUF, size)
        it.setOption(SO_RCVBUF, size)
        it.setOption(TCP_NODELAY, true)
        it.setOption(TCP_QUICKACK, true)
    }
}