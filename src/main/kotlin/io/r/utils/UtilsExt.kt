package io.r.utils

fun ByteArray.encodeBase64(): String = java.util.Base64.getEncoder().encodeToString(this)