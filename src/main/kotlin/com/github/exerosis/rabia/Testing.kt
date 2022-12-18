package com.github.exerosis.rabia

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.currentCoroutineContext
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

val executor: ExecutorService = Executors.newFixedThreadPool(20)
val dispatcher = executor.asCoroutineDispatcher()

suspend fun info(message: String) {
    val ctx = currentCoroutineContext()[CoroutineName]
    if (INFO) {
        println("[${ctx?.name}] $message")
//        System.out.flush()
    }
}
suspend fun debug(message: String) {
    val ctx = currentCoroutineContext()[CoroutineName]
    if (DEBUG) {
        println("[${ctx?.name}] $message")
//        System.out.flush()
    }
}
suspend fun warn(message: String) {
    val ctx = currentCoroutineContext()[CoroutineName]
    if (WARN) {
        println("[${ctx?.name}] $message")
//        System.out.flush()
    }
}