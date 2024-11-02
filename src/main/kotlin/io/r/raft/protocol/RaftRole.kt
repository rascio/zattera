package io.r.raft.protocol

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}