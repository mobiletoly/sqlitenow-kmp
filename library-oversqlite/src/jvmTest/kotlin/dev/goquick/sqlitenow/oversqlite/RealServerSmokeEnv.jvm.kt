package dev.goquick.sqlitenow.oversqlite

internal actual fun realServerEnv(name: String): String? = System.getenv(name)
