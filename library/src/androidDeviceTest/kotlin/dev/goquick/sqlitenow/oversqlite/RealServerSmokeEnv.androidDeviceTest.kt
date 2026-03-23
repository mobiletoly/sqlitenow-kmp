package dev.goquick.sqlitenow.oversqlite

internal actual fun realServerSmokeEnv(name: String): String? = System.getenv(name)
