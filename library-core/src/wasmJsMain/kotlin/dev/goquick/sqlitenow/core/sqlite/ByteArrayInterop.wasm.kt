/*
 * Copyright 2026 Toly Pochkin
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

import kotlin.JsFun
import kotlin.js.JsAny
import kotlin.js.JsArray
import kotlin.js.toJsNumber

internal fun jsArrayOfSize(size: Int): JsArray<JsAny?> {
    val array = JsArray<JsAny?>()
    for (index in 0 until size) {
        jsArraySetNull(array, index)
    }
    return array
}

internal fun ByteArray.toJsArray(): JsArray<JsAny?> {
    val array = JsArray<JsAny?>()
    for (index in indices) {
        jsArraySetValue(array, index, (this[index].toInt() and 0xFF).toJsNumber())
    }
    return array
}

internal fun JsArray<JsAny?>.asByteArray(): ByteArray {
    val result = ByteArray(length)
    for (index in 0 until length) {
        result[index] = (toNumber(this[index]).toInt() and 0xFF).toByte()
    }
    return result
}

@JsFun("(value) => Array.isArray(value)")
internal external fun isJsArray(value: JsAny?): Boolean

@JsFun("(value) => value == null")
internal external fun isNull(value: JsAny?): Boolean

@JsFun("(value) => Number(value)")
internal external fun toNumber(value: JsAny?): Double

@JsFun("(value) => String(value)")
internal external fun toStringValue(value: JsAny?): String

@JsFun("(value) => value")
internal external fun asJsArray(value: JsAny?): JsArray<JsAny?>

@JsFun("(array, index) => { array[index] = null; }")
internal external fun jsArraySetNull(array: JsArray<JsAny?>, index: Int)

@JsFun("(array, index, value) => { array[index] = value; }")
internal external fun jsArraySetValue(array: JsArray<JsAny?>, index: Int, value: JsAny?)

@JsFun("(value) => typeof value")
internal external fun jsTypeOf(value: JsAny?): String
