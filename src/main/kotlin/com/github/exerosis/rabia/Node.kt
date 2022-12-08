package com.github.exerosis.rabia

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer.allocateDirect
import java.util.*
import kotlin.experimental.or
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

const val OP_STATE = 2L
const val OP_VOTE = 3L
const val OP_LOST = 4L

const val STATE_ZERO = (OP_STATE shl 6).toByte()
const val STATE_ONE = (OP_STATE shl 6 or 32).toByte()

const val VOTE_ZERO = (OP_VOTE shl 6).toByte()
const val VOTE_ONE = (OP_VOTE shl 6 or 32).toByte()
const val VOTE_LOST = (OP_LOST shl 6).toByte()

const val MASK_MID = (0b11L shl 62).inv()

suspend fun active() = currentCoroutineContext().isActive

suspend fun Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Int, Long) -> (Unit),
    messages: suspend () -> (Long),
    slot: suspend () -> (Int),
    vararg nodes: InetSocketAddress
) {
    val f = (n / 2) - 1
    val proposes = TCP(address, port, 65527, *nodes)
    val states = TCP(address, port + 1, 65527, *nodes.map {
        InetSocketAddress(it.address, it.port + 1)
    }.toTypedArray())
    val votes = TCP(address, port + 2, 65527, *nodes.map {
        InetSocketAddress(it.address, it.port + 2)
    }.toTypedArray())
    val buffer = allocateDirect(12)
    val majority = (n / 2) + 1
    val proposals = LongArray(majority)
    var random = Random(0)
    log("N: $n F: $f Majority: $majority")
    val savedProposals = HashMap<Int, LinkedList<Long>>()
    val savedVotes = HashMap<Int, LinkedList<Byte>>()
    val savedStates = HashMap<Int, LinkedList<Byte>>()

    suspend fun phase(p: Byte, state: Byte, common: Long, slot: Int): Long {
        if (p > 0) warn("Phase: $p")
        buffer.clear().put(state or p).putInt(slot)
        states.send(buffer.flip())
        log("Sent State: ${state or p} - $slot")
        var zero = 0; var one = 0; var lost = 0
        while (zero + one < majority) {
            val op = if (savedStates[slot]?.isNotEmpty() == true) {
//                warn("Used Saved State")
                savedStates[slot]!!.pollLast()
            } else {
                states.receive(buffer.clear().limit(5))
                val op = buffer.get(0)
                val depth = buffer.getInt(1)
                if (depth < slot) continue
                if (depth > slot) {
                    savedStates.getOrPut(depth) { LinkedList() }.offerFirst(op)
//                    warn("Saved State")
                    continue
                }; op
            }
            log("Got State (${zero + one}/$majority): $op - $slot")
            when (op) {
                STATE_ONE or p -> ++one
                STATE_ZERO or p -> ++zero
            }
        }
        val vote = when {
            zero >= majority -> VOTE_ZERO
            one >= majority -> VOTE_ONE
            else -> VOTE_LOST
        } or p
        buffer.clear().put(vote).putInt(slot)
        votes.send(buffer.flip())
        log("Sent Vote: $vote - $slot")
        zero = 0; one = 0
        //TODO can we reduce the amount we wait for here.
        while (zero + one + lost < majority) {
            val op = if (savedVotes[slot]?.isNotEmpty() == true) {
//                warn("Used Saved State")
                savedVotes[slot]!!.pollLast()
            } else {
                votes.receive(buffer.clear().limit(5))
                val op = buffer.get(0)
                val depth = buffer.getInt(1)
                if (depth < slot) continue
                if (depth > slot) {
                    savedVotes.getOrPut(depth) { LinkedList() }.offerFirst(op)
//                    warn("Saved State")
                    continue
                }; op
            }
            when (op) {
                VOTE_ONE or p -> ++one
                VOTE_ZERO or p -> ++zero
                VOTE_LOST or p -> ++lost
            }
        }
        log("End Phase: $p - $slot")
        val next = (p + 1).toByte()
        if (next > 120) error("State to high!")
        return if (zero >= f + 1) -1
        else if (one >= f + 1) common and MASK_MID
        else phase(
            next, when {
                zero > 0 -> STATE_ZERO
                one > 0 -> STATE_ONE
                else -> {
                    warn("Rolling: $p")
                    if (random.nextBoolean())
                        STATE_ZERO else STATE_ONE
                }
            }, common, slot
        )
    }


    outer@ while (proposes.isOpen) {
        val proposed = messages()
        val current = slot()
        try {
            withTimeout(5.seconds) {
                log("Proposed: $proposed - $current")
                buffer.clear().putLong(proposed).putInt(current)
                proposes.send(buffer.flip())
                var index = 0
                //create this lazily
                random = Random(current)
                while (index < majority) {
                    val proposal = if (savedProposals[current]?.isNotEmpty() == true) {
//                        warn("Used Saved")
                        savedProposals[current]!!.pollLast()
                    } else {
                        proposes.receive(buffer.clear())
                        val proposal = buffer.getLong(0)
                        val depth = buffer.getInt(8)
                        if (depth < current) continue
                        if (current < depth) {
//                            warn("Added Saved")
                            savedProposals.getOrPut(depth) { LinkedList() }.offerFirst(proposal)
                            continue
                        }
                        proposal
                    }
                    var count = 1
                    for (i in 0 until index)
                        if (proposals[i] == proposal && ++count >= majority) {
                            log("Countered ($count/$majority): $proposal - $current")
                            return@withTimeout commit(current, phase(0, STATE_ONE, proposals[i], current))
                        }
                    log("Countered ($count/$majority): $proposal - $current")
                    proposals[index++] = proposal
                }
                commit(current, phase(0, STATE_ZERO, -1, current))
            }
        } catch (reason: Throwable) {
            if (reason is TimeoutCancellationException)
                warn("Timed Out")
            else throw reason
            commit(current, 0)
        }
    }
}