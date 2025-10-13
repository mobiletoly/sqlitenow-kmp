/*
 * Copyright 2025 Anatoliy Pochkin
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
package com.pluralfusion.daytempo.domain.model

internal const val BYTE_FALSE = 0.toByte()
internal const val BYTE_TRUE = 1.toByte()

internal fun Boolean.toByte() = if (this) BYTE_TRUE else BYTE_FALSE
internal fun Byte.toBoolean() = this != BYTE_FALSE

interface HasStringValue {
    val value: String
}
