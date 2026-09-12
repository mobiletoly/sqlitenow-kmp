package dev.goquick.sqlitenow.core.worker

/** Test-only raw fixture runner; supplies no production backend capability. */
internal expect suspend fun runBoundWriteFaultFixture(scenario: String): String
