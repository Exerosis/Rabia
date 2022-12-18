package com.github.exerosis.rabia

import kotlinx.coroutines.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer.allocateDirect
import java.util.*
import java.util.concurrent.Executors
import kotlin.experimental.or
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

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

val test = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

@OptIn(ExperimentalTime::class)
suspend fun Node(
    port: Int, address: InetAddress, n: Int,
    commit: suspend (Int, Long) -> (Unit),
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
    println("Ready")
    val proposals = LongArray(majority)
    var random = Random(0)

    val savedProposals = HashMap<Int, LinkedList<Long>>()
    val savedVotes = HashMap<Int, LinkedList<Byte>>()
    val savedStates = HashMap<Int, LinkedList<Byte>>()

//    val profileProposals = Profiler(5000, "ProposalsProfiler")
//    val profileState = Profiler(5000, "StateProfiler")
//    val profileVote = Profiler(5000, "VoteProfiler")
    val profileRound = Profiler(100_000, "RoundProfiler")

    fun HashMap<Int, LinkedList<Long>>.poll(depth: Int): Long? {
        val list = this[depth]
        val it = list?.pollLast()
        if (list?.isEmpty() == true) remove(depth)
        return it
    }
    fun HashMap<Int, LinkedList<Byte>>.poll(depth: Int): Byte? {
        val list = this[depth]
        val it = list?.pollLast()
        if (list?.isEmpty() == true) remove(depth)
        return it
    }
    val loopback = InetSocketAddress(InetAddress.getLoopbackAddress(), 0).address

    suspend fun phase(p: Byte, state: Byte, common: Long, slot: Int): Long {
//        profileProposals.end()
//        profileState.start()
        if (p > 0) warn("Phase: $p")
        buffer.clear().put(state or p).putInt(slot)
        states.send(buffer.flip())
        info("Sent State: ${state or p} - $slot")
        var zero = 0; var one = 0; var lost = 0
        while (zero + one < majority) {
            var op = savedStates.poll(slot)
            //TODO don't show port in message
            var from: InetAddress = loopback
            if (op == null) {
                from = states.receive(buffer.clear().limit(5)).address
                op = buffer.get(0)
                val depth = buffer.getInt(1)
                if (depth < slot) continue
                if (depth > slot) {
                    savedStates.getOrPut(depth) { LinkedList() }.offerFirst(op)
//                    warn("Saved State")
                    continue
                }
            }
            info("Got State (${zero + one + 1}/$majority): $op - $slot $from")//candidate
            when (op) {
                STATE_ONE or p -> ++one
                STATE_ZERO or p -> ++zero
                else -> error("LOST MESSAGE")
            }
        }
//        profileState.end()
//        profileVote.start()
        val vote = when {
            zero >= majority -> VOTE_ZERO
            one >= majority -> VOTE_ONE
            else -> VOTE_LOST
        } or p
        buffer.clear().put(vote).putInt(slot)
        votes.send(buffer.flip())
        info("Sent Vote: $vote - $slot")
        zero = 0; one = 0
        //TODO can we reduce the amount we wait for here.
        while (zero + one + lost < majority) {
            var from: InetAddress = loopback
            var op = savedVotes.poll(slot)
            if (op == null) {
                from = votes.receive(buffer.clear().limit(5)).address
                op = buffer.get(0)
                val depth = buffer.getInt(1)
                if (depth < slot) continue
                if (depth > slot) {
                    savedVotes.getOrPut(depth) { LinkedList() }.offerFirst(op)
//                    warn("Saved State")
                    continue
                }
            }
            info("Got Vote (${zero + one + lost + 1}/$majority): $op - $slot $from")//candidate
            when (op) {
                VOTE_ONE or p -> ++one
                VOTE_ZERO or p -> ++zero
                VOTE_LOST or p -> ++lost
                else -> error("LOST MESSAGE")
            }
        }
//        profileVote.end()
        debug("End Phase: $p - $slot")
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

    //TODO Figure out why it sometimes visits the network many many times in a row (but doesn't lose sync in the process)
    outer@ while (proposes.isOpen) {
        val proposed = messages()
        val current = slot()
//        profileProposals.start()
        try {
            profileRound.start()
            withTimeout(5.seconds) {
                debug("Proposals: ${savedProposals.size} Votes: ${savedVotes.size} States: ${savedStates.size}")
                info("Sent Proposal: $proposed - $current")
                buffer.clear().putLong(proposed).putInt(current)
                proposes.send(buffer.flip())
                var index = 0

                //Removed all messages on receiving
//                val test = System.nanoTime() + 100_000
//                while (System.nanoTime() < test) {}
                //create this lazily
//                random = Random(current)
                while (index < majority) {
                    var from: InetAddress = loopback
                    var proposal = savedProposals.poll(current)
                    if (proposal == null) {
                        from = proposes.receive(buffer.clear()).address
                        debug("N: $from") //candidate
                        proposal = buffer.getLong(0)
                        val depth = buffer.getInt(8)
                        if (depth < current) {
                            debug("OLD: $depth vs $current")
                            continue
                        }
                        if (current < depth) {
                            debug("NEW: $depth vs $current")
//                            warn("Added Saved")
                            savedProposals.getOrPut(depth) { LinkedList() }.offerFirst(proposal)
                            continue
                        }
                        debug("CORRECT: $depth")
                    }
                    var count = 1
                    for (i in 0 until index)
                        if (proposals[i] == proposal && ++count >= majority) {
                            info("Got Proposal: $proposal - $current $from")//candidate
                            return@withTimeout commit(current, phase(0, STATE_ONE, proposals[i], current))
                        }
                    info("Got Proposal: $proposal - $current $from")//candidate
                    proposals[index++] = proposal
                }
                commit(current, phase(0, STATE_ZERO, -1, current))
            }
            profileRound.end()
        } catch (reason: Throwable) {
            if (reason is TimeoutCancellationException) {
                warn("Timed Out")
            } else throw reason
            break@outer
            commit(current, 0)
        }
    }
}