package com.github.exerosis.rabia

import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteBuffer.*
import kotlin.experimental.or
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

const val OP_PROPOSE = 1L
const val OP_STATE = 2L
const val OP_VOTE = 3L
const val OP_LOST = 4L

const val STATE_ZERO = (OP_STATE shl 6).toByte()
const val STATE_ONE = (OP_STATE shl 6 or 32).toByte()

const val VOTE_ZERO = (OP_VOTE shl 6).toByte()
const val VOTE_ONE = (OP_VOTE shl 6 or 32).toByte()
const val VOTE_LOST = (OP_LOST shl 6).toByte()

const val MASK_MID = (0b11L shl 62).inv()

suspend fun Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Int, Long) -> (Unit),
    messages: suspend () -> (Long),
    slot: suspend () -> (Int),
) = withContext(dispatcher) {
    val f = (n / 2) - 1
    val channel = UDP(address, port,  65527)
    val broadcast = InetSocketAddress(BROADCAST, port)
    val buffer = allocateDirect(12)
    val majority = (n / 2) + 1
    val proposals = LongArray(majority)
    var random = Random(0)
    println("N: $n F: $f Majority: $majority")
    fun phase(p: Byte, state: Byte, common: Long): Long {
        buffer.clear().put(state or p)
        channel.send(buffer.flip(), broadcast)
        var zero = 0; var one = 0; var lost = 0;
        while ((zero + one) < majority) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                STATE_ONE or p -> ++one
                STATE_ZERO or p -> ++zero
            }
        }
        buffer.clear().put(when {
            zero >= majority -> VOTE_ZERO
            one >= majority -> VOTE_ONE
            else -> VOTE_LOST
        } or p)
        channel.send(buffer.flip(), broadcast)
        zero = 0; one = 0
        //TODO can we reduce the amount we wait for here.
        while ((zero + one + lost) < (n - f)) {
            channel.receive(buffer.clear())
            when (buffer.get(0)) {
                VOTE_ONE or p -> ++one
                VOTE_ZERO or p -> ++zero
                VOTE_LOST or p -> ++lost
            }
        }
        return if (zero >= f + 1) -1
        else if (one >= f + 1) common and MASK_MID
        else phase((p + 1).toByte(), when {
            zero > 0 -> STATE_ZERO
            one > 0 -> STATE_ONE
            else -> if (random.nextBoolean())
                STATE_ZERO else STATE_ONE
        }, common)
    }
    outer@ while (channel.isOpen) {
        withTimeout(10.milliseconds) {
            val proposed = OP_PROPOSE shl 62 or messages()
            println("Proposed: $proposed")
            var current = slot()
            buffer.clear().putLong(proposed).putInt(current)
            channel.send(buffer.flip(), broadcast)
            var index = 0
            //create this lazily
            random = Random(current)
            while (index < majority) {
                channel.receive(buffer.clear())
                proposals[index] = buffer.getLong(0)
                println("Countered: ${proposals[index]}")
                if (proposals[index] shr 62 == OP_PROPOSE) {
                    val depth = buffer.getInt(8)
                    if (current < depth) current = depth
                    var count = 1
                    for (i in 0 until index) {
                        if (proposals[i] == proposals[index]) {
                            if (++count >= majority) {
                                val state = if (proposals[i] == proposed) STATE_ONE else STATE_ZERO
                                println("Ok I'm going to vote!")
                                commit(current, phase(0, state, proposals[i])); return@withTimeout
                                //continue@outer
                            }
                        }
                    }
                    ++index;
                }
            }
            commit(current, phase(0, STATE_ZERO, -1))
        }
//        delay(1.milliseconds)
    }
}