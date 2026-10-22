package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test

// Fast semantic regressions; the 64-row and 2,000-row qualification owners remain.
internal class ConflictReservationRegressionTest : ConflictReservationTestSupport() {
    @Test fun automaticCancellationReopens() = conflictReservation(16,1,4096,true,true)
    @Test fun explicitCancellationReopens() = conflictReservation(16,1,4096,false,true)
    @Test fun automaticAtOne() = conflictReservation(16,1,4096,true)
    @Test fun automaticAtEight() = conflictReservation(16,8,4096,true)
    @Test fun automaticAtSeventeen() = conflictReservation(16,17,4096,true)
    @Test fun automaticAtDefault() = conflictReservation(16,1_000_000,4096,true)
    @Test fun explicitAtOne() = conflictReservation(16,1,4096,false)
    @Test fun explicitAtEight() = conflictReservation(16,8,4096,false)
    @Test fun explicitAtSeventeen() = conflictReservation(16,17,4096,false)
    @Test fun explicitAtDefault() = conflictReservation(16,1_000_000,4096,false)
}
