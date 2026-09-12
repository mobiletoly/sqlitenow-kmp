package dev.goquick.sqlitenow.oversqlite

/** Only the unpublished generated W1 client implements this telemetry. */
internal expect fun w1WorkerModuleUrl(scenario: String): String
internal expect fun w1Configure(configuration: String)
internal expect suspend fun w1AwaitHeld()
internal expect fun w1ReleaseHeld()
internal expect fun w1Mark(phase: String)
internal expect fun w1Snapshot(): String
