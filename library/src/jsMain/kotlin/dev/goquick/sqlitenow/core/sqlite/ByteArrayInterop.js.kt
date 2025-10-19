/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
