package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test

internal class ConflictReservationContinuationTest : ConflictReservationTestSupport() {
    @Test fun automaticCancellationReopens() = conflictReservation(64,1,4096,true,true)
    @Test fun explicitCancellationReopens() = conflictReservation(64,1,4096,false,true)
    @Test fun automaticAtOne() = conflictReservation(64,1,4096,true)
    @Test fun automaticAtEight() = conflictReservation(64,8,4096,true)
    @Test fun automaticAtSeventeen() = conflictReservation(64,17,4096,true)
    @Test fun automaticAtDefault() = conflictReservation(64,1_000_000,4096,true)
    @Test fun explicitAtOne() = conflictReservation(64,1,4096,false)
    @Test fun explicitAtEight() = conflictReservation(64,8,4096,false)
    @Test fun explicitAtSeventeen() = conflictReservation(64,17,4096,false)
    @Test fun explicitAtDefault() = conflictReservation(64,1_000_000,4096,false)
}
