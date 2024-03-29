package com.github.exerosis.rabia

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer.allocateDirect
import kotlin.random.Random

const val STATE_ZERO = 0
const val STATE_ONE = 1

const val VOTE_ZERO = 0
const val VOTE_ONE = 1
const val VOTE_LOST = 2

class State(val logs: Int, val n: Int) {
    val f = n / 2
    val majority = (n / 2) + 1

    val indices = IntArray(logs)
    val proposals = Array(majority) { LongArray(logs) }
    val statesZero = ByteArray(logs * 256)
    val statesOne = ByteArray(logs * 256)
    val votesZero = ByteArray(logs * 256)
    val votesOne = ByteArray(logs * 256)
    val votesLost = ByteArray(logs * 256)
}

//new = a current = b
//if new is less than current but not by a huge amount then it's old
//if new is greater than current by a huge amount then it's old
inline fun isOld(a: Int, b: Int, half: Int) =
    a < b && (b - a) < half || a > b && (a - b) > half

suspend fun State.Node(
    i: Int,
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Long) -> (Unit),
    messages: suspend () -> (Long),
    slot: suspend () -> (Int),
    vararg nodes: InetAddress
) {
    info("I: $i N: $n F: $f Majority: $majority")
    val proposes = TCP(address, port, 65527 * 5, *nodes.map {
        InetSocketAddress(it, port)
    }.toTypedArray())
    val states = TCP(address, port + 1, 65527 * 5, *nodes.map {
        InetSocketAddress(it, port + 1)
    }.toTypedArray())
    val votes = TCP(address, port + 2, 65527 * 5, *nodes.map {
        InetSocketAddress(it, port + 2)
    }.toTypedArray())
    val buffer = allocateDirect(10)
    val half = logs / 2

    outer@ while (proposes.isOpen) {
        val proposed = messages()
        val realSlot = slot() //32 bit slot
        val current = realSlot % logs //the slot wrapped
        buffer.clear().putShort(current.toShort()).putLong(proposed)
        proposes.send(buffer.flip())
        info("Sent Proposal: $proposed - $current")

        while (indices[current] < majority) {
            val from = proposes.receive(buffer.clear()).address
            val depth = buffer.getShort(0).toInt() and 0xFFFF
            if (isOld(depth, current, half)) continue
            val proposal = buffer.getLong(2)

            info("Got Proposal: $proposal - $current $from")
            if (indices[depth] < majority)
                proposals[indices[depth]++][depth] = proposal
        }
        val proposal = proposals[0][current]
//        if (proposal == 65535L) error("Done here for now")
        val all = (1 until majority).all {
            proposals[it][current] == proposal
        }
        if (!all) {
            warn("Current: $current")
            (0 until majority).forEach {
                warn("${proposals[it][current]}")
            }
            error("Very strange")
        }
        indices[current] = 0
        var phase = 0
        var state = if (all) STATE_ONE else STATE_ZERO
        while (true) {
            val height = current shl 8 or phase
            buffer.clear().putShort(current.toShort()).put((phase shl 2 or state).toByte())
            states.send(buffer.flip())
            info("Sent State: $state - $current")
            while (statesZero[height] + statesOne[height] < majority) {
                val from = states.receive(buffer.clear().limit(3)).address
                val depth = buffer.getShort(0).toInt() and 0xFFFF
                if (isOld(depth, current, half)) continue
                val op = buffer.get(2).toInt()
                val round = op shr 2
                if (isOld(round, phase, 32)) continue
                info("Got State (${statesZero[height] + statesOne[height] + 1}/$majority): $op - $current $from")
                when (op and 3) {
                    STATE_ONE -> ++statesOne[depth shl 8 or round]
                    STATE_ZERO -> ++statesZero[depth shl 8 or round]
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

            buffer.clear().putShort(current.toShort()).put((phase shl 2 or vote).toByte())
            votes.send(buffer.flip())
            info("Sent Vote: $vote - $current")
            //TODO can we reduce the amount we wait for here.
            while (votesZero[height] + votesOne[height] + votesLost[height] < majority) {
                val from = votes.receive(buffer.clear().limit(3)).address
                val depth = buffer.getShort(0).toInt() and 0xFFFF
                if (isOld(depth, current, half)) continue
                val op = buffer.get(2).toInt()
                val round = op shr 2
                if (isOld(round, phase, 64)) continue
                info("Got Vote (${votesZero[height] + votesOne[height] + votesLost[height] + 1}/$majority): $op - $current $from")
                when (op) {
                    VOTE_ZERO -> ++votesZero[depth shl 8 or round]
                    VOTE_ONE -> ++votesOne[depth shl 8 or round]
                    VOTE_LOST -> ++votesLost[depth shl 8 or round]
                }
            }

            val zero = votesZero[height]
            val one = votesOne[height]
            votesZero[height] = 0
            votesOne[height] = 0
            votesLost[height] = 0

            if (one >= f + 1) commit(if (all) proposal else 0)
            else if (zero >= f + 1) commit(-1) else {
                ++phase
                state = when {
                    one > 0 -> STATE_ONE
                    zero > 0 -> STATE_ZERO
                    else -> {
                        println("Rolling: $phase")
                        if (Random(height).nextBoolean())
                            STATE_ZERO else STATE_ONE
                    }
                }; continue
            }; continue@outer
        }
    }
}