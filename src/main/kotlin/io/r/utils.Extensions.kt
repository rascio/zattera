package io.r


fun LongArray.toHex() =
    joinToString("") { "%02x".format(it) }