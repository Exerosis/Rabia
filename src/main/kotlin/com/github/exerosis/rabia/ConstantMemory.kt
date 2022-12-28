package com.github.exerosis.rabia

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer.allocateDirect
import kotlin.random.Random

class State(logs: Int, majority: Int) {
    val indices = IntArray(logs)
    val proposals = LongArray(logs * majority)
    val statesZero = ByteArray(logs * 256)
    val statesOne = ByteArray(logs * 256)
    val votesZero = ByteArray(logs * 256)
    val votesOne = ByteArray(logs * 256)
    val votesLost = ByteArray(logs * 256)
}

operator fun LongArray.get(index: Int, phase: Int) =
    this[index shl 8 or phase]
operator fun LongArray.set(index: Int, phase: Int, value: Long)
    { this[index shl 8 or phase] = value }

operator fun ByteArray.get(index: Int, phase: Int) =
    this[index shl 8 or phase]
operator fun ByteArray.set(index: Int, phase: Int, value: Byte)
    { this[index shl 8 or phase] = value }

suspend fun State.Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Long) -> (Unit),
    messages: suspend () -> (Long),
    slot: suspend () -> (Int),
    vararg nodes: InetAddress
) {
    val f = n / 2
    val majority = (n / 2) + 1
    info("N: $n F: $f Majority: $majority")
    val proposes = TCP(address, port, 65527 * 5, *nodes.map {
        InetSocketAddress(it, port)
    }.toTypedArray())
    val states = TCP(address, port + 1, 65527 * 5, *nodes.map {
        InetSocketAddress(it, port + 1)
    }.toTypedArray())
    val votes = TCP(address, port + 2, 65527 * 5, *nodes.map {
        InetSocketAddress(it, port + 2)
    }.toTypedArray())
    val buffer = allocateDirect(12)

    outer@ while (proposes.isOpen) {
        val proposed = messages()
        val current = slot()

        buffer.clear().putInt(current).putLong(proposed)
        proposes.send(buffer.flip())
        info("Sent Proposal: $proposed - $current")

        while (indices[current] < majority) {
            val index = indices[current]
            val from = proposes.receive(buffer.clear()).address
            val depth = buffer.getInt(0)
            if (depth < current) continue
            val proposal = buffer.getLong(4)
            info("Got Proposal: $proposal - $current $from")
            proposals[depth, index] = proposal
            ++indices[depth]
        }
        val proposal = proposals[current, 0]
        val all = (1 until majority).all { proposals[current, it] == proposal }
        indices[current] = 0
        var phase = 0
        var state = if (all) STATE_ONE else STATE_ZERO
        while (phase < 64) { //if we want to pack op and phase into the same place.
            val height = current shl 8 or phase
            buffer.clear().putInt(height).put(state)
            states.send(buffer.flip())
            info("Sent State: $state - $slot")
            while (statesZero[height] + statesOne[height] < majority) {
                val from = states.receive(buffer.clear().limit(5)).address
                val depth = buffer.getInt(0)
                if (depth shr 8 < current) continue
                val op = buffer.get(4)
                info("Got State (${statesZero[height] + statesOne[height] + 1}/$majority): $op - $slot $from")
                when (op) {
                    STATE_ONE -> ++statesOne[depth]
                    STATE_ZERO -> ++statesZero[depth]
                    else -> error("Invalid state!")
                }
            }
            val vote = when {
                statesOne[height] >= majority -> VOTE_ONE
                statesZero[height] >= majority -> VOTE_ZERO
                else -> VOTE_LOST
            }
            statesZero[height] = 0
            statesOne[height] = 0

            buffer.clear().putInt(height).put(vote)
            votes.send(buffer.flip())
            info("Sent Vote: $vote - $slot")
            //TODO can we reduce the amount we wait for here.
            while (votesZero[height] + votesOne[height] + votesLost[height] < majority) {
                val from = votes.receive(buffer.clear().limit(5)).address
                val depth = buffer.getInt(0)
                if (depth shr 8 < current) continue
                val op = buffer.get(4)
                info("Got Vote (${votesZero[height] + votesOne[height] + votesLost[height] + 1}/$majority): $op - $slot $from")
                when (op) {
                    VOTE_ZERO -> ++votesZero[depth]
                    VOTE_ONE -> ++votesOne[depth]
                    VOTE_LOST -> ++votesLost[depth]
                    else -> error("Invalid vote!")
                }
            }

            val zero = votesZero[height]
            val one = votesOne[height]
            votesZero[height] = 0
            votesOne[height] = 0
            votesLost[height] = 0

            if (one >= f + 1) {
                if (!all) error("This should be -1")
                commit(proposal)
            } else if (zero >= f + 1) commit(-1) else {
                ++phase
                state = when {
                    one > 0 -> STATE_ONE
                    zero > 0 -> STATE_ZERO
                    else -> {
                        error("Rolling: $phase")
                        if (Random(height).nextBoolean())
                            STATE_ZERO else STATE_ONE
                    }
                }
            }
        }
        error("For now lets not do this!")
    }
}