package dev.goquick.sqlitenow.core.sqlite

import org.khronos.webgl.ArrayBuffer
import org.khronos.webgl.Uint8Array

internal fun ByteArray.toUint8Array(): Uint8Array {
    val array = Uint8Array(size)
    val dyn = array.asDynamic()
    for (i in indices) {
        val value = this[i].toInt() and 0xFF
        dyn[i] = value
    }
    return array
}

internal fun Uint8Array.toByteArray(): ByteArray {
    val result = ByteArray(length)
    val dyn = this.asDynamic()
    for (i in 0 until length) {
        val value = (dyn[i] as Number).toInt()
        result[i] = (value and 0xFF).toByte()
    }
    return result
}

internal fun ArrayBuffer.toByteArray(): ByteArray = Uint8Array(buffer = this).toByteArray()
