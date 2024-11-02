package io.r.raft.machine

import io.r.raft.protocol.Index

data class PeerState(
    val nextIndex: Index,
    val matchIndex: Index,
    val lastContactTime: Long = Long.MIN_VALUE
)