package com.github.exerosis.rabia

class Theory(logs: Int, majority: Int) {
    val proposals = LongArray(logs * majority)
    val statesZero = ByteArray(logs * 256)
    val statesOne = ByteArray(logs * 256)
    val votesZero = ByteArray(logs * 256)
    val votesOne = ByteArray(logs * 256)
    val votesLost = ByteArray(logs * 256)
}
