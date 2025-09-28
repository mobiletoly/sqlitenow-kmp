package com.pluralfusion.daytempo.domain.model

internal const val BYTE_FALSE = 0.toByte()
internal const val BYTE_TRUE = 1.toByte()

internal fun Boolean.toByte() = if (this) BYTE_TRUE else BYTE_FALSE
internal fun Byte.toBoolean() = this != BYTE_FALSE

interface HasStringValue {
    val value: String
}
