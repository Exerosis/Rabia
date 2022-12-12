package com.github.exerosis.rabia

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.currentCoroutineContext
import java.net.InetAddress
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

val executor: ExecutorService = Executors.newCachedThreadPool()
val dispatcher = executor.asCoroutineDispatcher()

suspend fun log(message: String) {
    try {
        val ctx = currentCoroutineContext()[CoroutineName]
        if (DEBUG) {
            println("[${ctx?.name}] $message")
            System.out.flush()
        }
    } catch (reason: Throwable) {
        reason.printStackTrace()
    }
}
suspend fun warn(message: String) {
    try {
        val ctx = currentCoroutineContext()[CoroutineName]
        if (WARN) {
            println("[${ctx?.name}] $message")
            System.out.flush()
        }
    } catch (reason: Throwable) {
        reason.printStackTrace()
    }
}

val hostname = InetAddress.getLocalHost().hostName.split('.')[0]
fun current() = when (hostname) {
    "DESKTOP-NJ3CTN8" -> "192.168.10.38"
    "exerosis-server" -> "192.168.10.254"
    else -> "192.168.10.54"
}
fun other() = if (SERVER) when (hostname) {
    "exerosis-server" -> "192.168.10.38"
    else -> "192.168.10.254"
} else when (hostname) {
    "DESKTOP-NJ3CTN8" -> "192.168.10.54"
    else -> "192.168.10.38"
}