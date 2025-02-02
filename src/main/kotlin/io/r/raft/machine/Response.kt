package io.r.raft.machine

import io.r.raft.protocol.RaftRpc
import kotlinx.serialization.Serializable

@Serializable
sealed interface Response {
    @Serializable
    data class Success(val value: ByteArray): Response {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Success

            return value.contentEquals(other.value)
        }

        override fun hashCode(): Int {
            return value.contentHashCode()
        }

        override fun toString(): String {
            return "Success(value=${value.contentToString()})"
        }

    }

    @Serializable
    data class NotALeader(val node: RaftRpc.ClusterNode): Response

    @Serializable
    data object LeaderUnknown: Response

}