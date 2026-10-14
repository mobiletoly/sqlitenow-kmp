package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test

internal class ConflictReservationContinuationJvmTest : ConflictReservationTestSupport() {
    @Test fun automaticTwoThousandAtOne() = conflictReservation(2000,1,131072,true)
    @Test fun explicitTwoThousandAtOne() = conflictReservation(2000,1,131072,false)
}
