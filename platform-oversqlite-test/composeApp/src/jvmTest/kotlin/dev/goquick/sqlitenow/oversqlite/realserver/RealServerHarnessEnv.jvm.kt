package dev.goquick.sqlitenow.oversqlite.realserver

internal actual fun realServerEnv(name: String): String? = System.getenv(name)
