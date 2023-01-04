package com.github.exerosis.rabia

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface.getByInetAddress
import java.net.StandardProtocolFamily.INET
import java.net.StandardSocketOptions.*
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import java.nio.channels.DatagramChannel
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

suspend fun TCP(
    address: InetAddress,
    port: Int, size: Int,
    vararg addresses: InetSocketAddress
): Multicaster {
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

//    val waiters = LinkedList<Continuation<Unit>>()
//    val things = addresses.map { scope.async {
//        suspendCoroutineUninterceptedOrReturn {
//            waiters += it
//            COROUTINE_SUSPENDED
//        }
//    } }
    val inbound = CopyOnWriteArrayList<Inbound>()
    server.accept(Unit, object : CompletionHandler<AsynchronousSocketChannel, Unit> {
        override fun completed(result: AsynchronousSocketChannel, attachment: Unit) {
            result.setOption(SO_SNDBUF, size)
            result.setOption(SO_RCVBUF, size)
            result.setOption(TCP_NODELAY, false)
            inbound.add(Inbound(result))
//            waiters.poll()?.resume(Unit)
            server.accept(Unit, this)
        }
        override fun failed(reason: Throwable, attachment: Unit) = throw reason
    })

    val outbound = addresses.map {
        val client = suspendCoroutineUninterceptedOrReturn { continuation ->
            val client = AsynchronousSocketChannel.open(group)
            client.connect(it, client, object : CompletionHandler<Void, AsynchronousSocketChannel> {
                override fun completed(result: Void?, client: AsynchronousSocketChannel) =
                    continuation.resume(client)

                override fun failed(reason: Throwable, ignored: AsynchronousSocketChannel)  {
                    val client = AsynchronousSocketChannel.open(group)
                    client.connect(it, client, this)
                }
            }); COROUTINE_SUSPENDED
        }
        client.setOption(SO_SNDBUF, size)
        client.setOption(SO_RCVBUF, size)
        client.setOption(TCP_NODELAY, false)
        Outbound(client)
    }
//    things.awaitAll()
    return object : Multicaster {
        override val isOpen = server.isOpen
        override fun close() = runBlocking { scope.cancel(); server.close() }

        override suspend fun send(buffer: ByteBuffer) =
            //if you use normal suspend it's much slower (profile?)
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
