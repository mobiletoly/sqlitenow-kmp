package dev.goquick.sqlitenow.oversqlite

internal expect fun suiteEnv(name: String): String?

internal fun suiteFlagEnabled(name: String): Boolean =
    when (suiteEnv(name)?.trim()?.lowercase()) {
        "1", "true", "yes", "on" -> true
        else -> false
    }

internal fun platformSuiteEnabled(): Boolean =
    suiteFlagEnabled("OVERSQLITE_PLATFORM_TESTS")

internal fun realServerSuiteEnabled(): Boolean =
    suiteFlagEnabled("OVERSQLITE_REALSERVER_TESTS")

internal fun realServerHeavyModeEnabled(): Boolean =
    suiteFlagEnabled("OVERSQLITE_REALSERVER_HEAVY")
