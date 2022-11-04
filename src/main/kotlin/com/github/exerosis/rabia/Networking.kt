package com.github.exerosis.rabia

import java.net.*
import java.net.NetworkInterface.*
import java.net.StandardProtocolFamily.*
import java.net.StandardSocketOptions.*
import java.nio.channels.DatagramChannel

const val BROADCAST = "230.0.0.0" //230

fun UDP(
    address: InetAddress,
    port: Int, size: Int
) = DatagramChannel.open(INET).apply {
    val network = getByInetAddress(address)
    setOption(SO_REUSEADDR, true)
    setOption(IP_MULTICAST_LOOP, true)
    setOption(IP_MULTICAST_IF, network)
    setOption(SO_SNDBUF, size)
    setOption(SO_RCVBUF, size)
    bind(InetSocketAddress(address, port))
    join(InetAddress.getByName(BROADCAST), network)
}