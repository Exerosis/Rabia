package com.github.exerosis.rabia

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withTimeout
import java.net.InetAddress
import java.net.InetSocketAddress
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
) {
    val f = (n / 2) - 1
    val proposes = UDP(address, port,  65527)
    val states = UDP(address, port + 1,  65527)
    val votes = UDP(address, port + 2,  65527)
    val toProposes = InetSocketAddress(BROADCAST, port)
    val toStates = InetSocketAddress(BROADCAST, port + 1)
    val toVotes = InetSocketAddress(BROADCAST, port + 2)
    val buffer = allocateDirect(12)
    val majority = (n / 2) + 1
    val proposals = LongArray(majority)
    var random = Random(0)
    log("N: $n F: $f Majority: $majority")
    suspend fun phase(p: Byte, state: Byte, common: Long, slot: Int): Long {
        log("Phase: $p - $slot")
        buffer.clear().put(state or p).putInt(slot)
        states.send(buffer.flip(), toStates)
        log("Sent State: ${state or p}")
        var zero = 0; var one = 0; var lost = 0
        while ((zero + one) < majority) {
            states.receive(buffer.clear())
            val op = buffer.get(0)
            log("Got State: $op")
            val depth = buffer.getInt(1)
            if (depth > slot) error("State Too High: $depth")
            if (depth == slot) when (op) {
                STATE_ONE or p -> ++one
                STATE_ZERO or p -> ++zero
            }
        }
        val vote = when {
            zero >= majority -> VOTE_ZERO
            one >= majority -> VOTE_ONE
            else -> VOTE_LOST
        }  or p
        buffer.clear().put(vote).putInt(slot)
        votes.send(buffer.flip(), toVotes)
        log("Sent Vote: $vote")
        zero = 0; one = 0
        //TODO can we reduce the amount we wait for here.
        while ((zero + one + lost) < majority) {
            votes.receive(buffer.clear())
            val op = buffer.get(0)
            log("Got Vote: $op")
            val depth = buffer.getInt(1)
            if (depth > slot) error("Vote Too High: $depth")
            if (depth == slot) when (op) {
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
        }, common, slot)
    }
    outer@ while (proposes.isOpen) {
        withTimeout(10.milliseconds) {
            val proposed = OP_PROPOSE shl 62 or messages()
            log("Proposed: $proposed")
            var current = slot()
            buffer.clear().putLong(proposed).putInt(current)
            proposes.send(buffer.flip(), toProposes)
            var index = 0
            //create this lazily
            random = Random(current)
            while (index < majority) {
                proposes.receive(buffer.clear())
                proposals[index] = buffer.getLong(0)
                log("Countered: ${proposals[index]}")
                if (proposals[index] shr 62 == OP_PROPOSE) {
                    val depth = buffer.getInt(8)
                    if (depth < current) {
                        log("Skipped a proposal!")
                        continue
                    }
                    if (current < depth) {
                        println("Might have accepted an earlier proposal that should have been dropped")
                        current = depth
                    }
                    var count = 1
                    for (i in 0 until index) {
                        if (proposals[i] == proposals[index]) {
                            if (++count >= majority) {
                                val state = if (proposals[i] == proposed) STATE_ONE else STATE_ZERO
                                commit(current, phase(0, state, proposals[i], current)); return@withTimeout
                                //continue@outer
                            }
                        }
                    }
                    ++index;
                }
            }
            commit(current, phase(0, STATE_ZERO, -1, current))
        }
//        delay(1.milliseconds)
    }
}