package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.*

internal class ReservationFormatTest : SurvivorTestSupport() {
    @Test fun rejectAndResume() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,http,seed ->
            val config=OversqliteConfig(generationId=1,databaseId="fixture",serviceId="main",syncTables=nodeTables)
            val unit=applyUnit("orphan",0,"")
            val ledger="""[{"token":"$unit","unit":"$unit","context":"$unit","entries":4,"bytes":4196}]"""
            db.execSQL("UPDATE _sync_operation_state SET workspace_context='$ledger',reserved_entries=4,reserved_bytes=4196")
            seed.closeFromCoordinatedOwner()
            val ordinary=newClient(db,http,syncTables=nodeTables)
            ordinary.openAndConnect("user-1").getOrThrow()
            assertEquals(4L,scalarLong(db,"SELECT reserved_entries FROM _sync_operation_state"))
            ordinary.closeFromCoordinatedOwner()
            try {db.transaction(TransactionMode.IMMEDIATE) {
                OversqliteWorkspace(db,config).reclaimAfterLifetimeAcquisition()
                error("rollback witness")
            }} catch (_:IllegalStateException) { }
            assertEquals(ledger,scalarText(db,"SELECT workspace_context FROM _sync_operation_state"))
            db.execSQL("UPDATE _sync_operation_state SET reserved_entries=5")
            val beforeReject=baselineQualificationVector(db,listOf("nodes"))
            assertFails {db.transaction(TransactionMode.IMMEDIATE) {OversqliteWorkspace(db,config).reclaimAfterLifetimeAcquisition()}}
            assertEquals(beforeReject,baselineQualificationVector(db,listOf("nodes")))
            assertEquals(ledger,scalarText(db,"SELECT workspace_context FROM _sync_operation_state"))
        }
    }
}
