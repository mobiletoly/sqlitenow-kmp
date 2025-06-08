package dev.goquick.sqlitenow.samplekmp.model

import dev.goquick.sqlitenow.core.util.EnumByValueLookup

enum class AddressType(val value: String) {
    HOME("HOME"),
    WORK("WORK");

    companion object : EnumByValueLookup<String, AddressType>(entries.associateBy { it.value })
}
