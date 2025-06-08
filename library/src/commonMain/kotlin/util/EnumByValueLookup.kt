package dev.goquick.sqlitenow.core.util

import kotlin.jvm.JvmName

open class EnumByValueLookup<T, V>(private val valueMap: Map<T, V>) {
    fun from(value: T): V =
        valueMap[value] ?: throw NoSuchElementException("$this: Cannot deserialize value '$value' into enum value")

    @JvmName("fromOptional")
    fun from(value: T?): V? = if (value == null) null else from(value)
}
