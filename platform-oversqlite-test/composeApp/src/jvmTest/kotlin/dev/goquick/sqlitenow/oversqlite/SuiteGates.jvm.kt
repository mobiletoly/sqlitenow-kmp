package dev.goquick.sqlitenow.oversqlite

internal actual fun suiteEnv(name: String): String? = System.getenv(name)
