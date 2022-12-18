package com.github.exerosis.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.base.Connection
import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface.getByInetAddress
import java.net.StandardProtocolFamily.INET
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.channels.*
import java.nio.channels.CompletionHandler
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

const val BROADCAST = "230.0.0.0" //230

interface Multicaster : AutoCloseable {
    suspend fun send(buffer: ByteBuffer)
    suspend fun receive(buffer: ByteBuffer): InetSocketAddress
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

suspend fun TCPN(
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
        override suspend fun receive(buffer: ByteBuffer): InetSocketAddress {
            //TODO Switch to round robin
            while (true) {
                inbound.shuffled().forEach {
                    if (it.read(buffer) != 0) {
                        while (buffer.hasRemaining()) {
                            it.read(buffer)
                            Thread.onSpinWait()
                        }
                        return it.remoteAddress as InetSocketAddress
                    }
                }
                Thread.onSpinWait()
            }
        }
    }
}

suspend fun TCPB(
    address: InetAddress,
    port: Int, size: Int,
    vararg addresses: InetSocketAddress
): Multicaster {
    val server = ServerSocketChannel.open()
    server.bind(InetSocketAddress(address, port))
    val scope = CoroutineScope(dispatcher)
    val outbound = CopyOnWriteArrayList<SocketChannel>()
    val inbound = CopyOnWriteArrayList<SocketChannel>()
    scope.launch {
        while (server.isOpen && isActive)
            server.accept()?.apply {
                setOption(SO_SNDBUF, size)
                setOption(SO_RCVBUF, size)
                setOption(TCP_NODELAY, true)
                inbound.add(this)
            }
    }
    addresses.map {
        scope.async { while (true) try {
            return@async outbound.add(SocketChannel.open(it).apply {
                setOption(SO_SNDBUF, size)
                setOption(SO_RCVBUF, size)
                setOption(TCP_NODELAY, true)
            })
        } catch (_: Throwable) {}}
    }.forEach { it.await() }
    return object : Multicaster {
        override val isOpen = server.isOpen
        override fun close() = runBlocking { scope.cancel(); server.close() }
        override suspend fun send(buffer: ByteBuffer) {
            outbound.map {
                val copy = buffer.duplicate()
                scope.async { it.write(copy) }
            }.awaitAll()
        }
        var i = 0
        override suspend fun receive(buffer: ByteBuffer): InetSocketAddress {
            val socket = inbound[i++ % inbound.size]
            socket.read(buffer)
            return socket.remoteAddress as InetSocketAddress
        }
    }
}



suspend fun TCPI(
    address: InetAddress,
    port: Int, size: Int,
    vararg addresses: InetSocketAddress
): Multicaster {
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val server = AsynchronousServerSocketChannel.open(group)
    server.bind(InetSocketAddress(address, port))
    val scope = CoroutineScope(dispatcher)

    val remaining = AtomicInteger()
    val outboundContinuation = AtomicReference<Continuation<Unit>>()
    val inboundContinuation = AtomicReference<Continuation<InetSocketAddress>>()
    class Outbound(
        val socket: AsynchronousSocketChannel
    ) : CompletionHandler<Int, ByteBuffer> {
        override fun completed(result: Int, buffer: ByteBuffer) {
            if (buffer.hasRemaining())
                return socket.write(buffer, buffer, this)
            if (remaining.decrementAndGet() == 0)
                outboundContinuation.getAndSet(null).resume(Unit)
        }
        override fun failed(reason: Throwable, buffer: ByteBuffer) =
            outboundContinuation.getAndSet(null).resumeWithException(reason)
    }
    class Inbound(
        val socket: AsynchronousSocketChannel
    ) : CompletionHandler<Int, ByteBuffer> {
        val remote = socket.remoteAddress as InetSocketAddress
        override fun completed(result: Int, buffer: ByteBuffer) {
            if (buffer.hasRemaining())
                return socket.write(buffer, buffer, this)
            inboundContinuation.getAndSet(null).resume(remote)
        }
        override fun failed(reason: Throwable, buffer: ByteBuffer) =
            inboundContinuation.getAndSet(null).resumeWithException(reason)
    }

    val outbound = CopyOnWriteArrayList<Outbound>()
    val inbound = CopyOnWriteArrayList<Inbound>()
    server.accept(Unit, object : CompletionHandler<AsynchronousSocketChannel, Unit> {
        override fun completed(result: AsynchronousSocketChannel, attachment: Unit) {
            inbound.add(Inbound(result))
            server.accept(Unit, this)
        }
        override fun failed(reason: Throwable, attachment: Unit) = throw reason
    })


    addresses.map {
        scope.async {
            while (true) try {
            return@async outbound.add(Outbound(AsynchronousSocketChannel.open(group).apply {
                connect(it).get()
                setOption(SO_SNDBUF, size)
                setOption(SO_RCVBUF, size)
                setOption(TCP_NODELAY, true)
            }))
        } catch (_: Throwable) {}}
    }.forEach { it.await() }
    return object : Multicaster {
        override val isOpen = server.isOpen
        override fun close() = runBlocking { scope.cancel(); server.close() }

        override suspend fun send(buffer: ByteBuffer) =
            suspendCoroutineUninterceptedOrReturn { next ->
                remaining.set(outbound.size)
                outboundContinuation.set(next)
                outbound.forEach {
                    val copy = buffer.duplicate()
                    it.socket.write(copy, copy, it)
                }
                COROUTINE_SUSPENDED
            }

        var i = 0
        override suspend fun receive(buffer: ByteBuffer)
            = suspendCoroutineUninterceptedOrReturn { next ->
                val handler = inbound[i++ % inbound.size]
                inboundContinuation.set(next)
                handler.socket.read(buffer, buffer, handler)
                COROUTINE_SUSPENDED
            }
    }
}

suspend fun TCP(
    address: InetAddress,
    port: Int, size: Int,
    vararg addresses: InetSocketAddress
): Multicaster {
    val group = AsynchronousChannelGroup.withThreadPool(executor)
    val provider = SocketProvider(0, group) {
        it.setOption(SO_SNDBUF, size)
        it.setOption(SO_RCVBUF, size)
        it.setOption(TCP_NODELAY, true)
    }
    val scope = CoroutineScope(dispatcher)
    val inbound = CopyOnWriteArrayList<Connection>()
    val outbound = addresses.map {
        while (true) try {
            provider.connect(it)
        } catch (_: Throwable) {}
        provider.connect(it)
    }
    scope.launch {
        while (provider.isOpen && isActive)
            inbound.add(provider.accept(InetSocketAddress(address, port)))
    }
    return object : Multicaster {
        override val isOpen = provider.isOpen
        override fun close() = runBlocking { scope.cancel(); provider.close() }

        override suspend fun send(buffer: ByteBuffer) {
            outbound.map { scope.async {
                it.write.buffer(buffer.duplicate())
            } }.awaitAll()
        }

        var i = 0
        override suspend fun receive(buffer: ByteBuffer): InetSocketAddress {
            val connection = inbound[i++ % inbound.size]
            connection.read.buffer(buffer)
            return connection.address
        }
    }
}