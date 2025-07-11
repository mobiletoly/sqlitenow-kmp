package dev.goquick.sqlitenow.samplekmp.db

import dev.goquick.sqlitenow.core.util.EnumByValueLookup

enum class AddressType(val value: String) {
    HOME("home"),
    WORK("work");

    companion object : EnumByValueLookup<String, AddressType>(entries.associateBy { it.value })
}