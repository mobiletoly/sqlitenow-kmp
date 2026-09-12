package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNull

internal class ContractStoreTest : CrossTargetSyncTestSupport() {
    private val identity = LocalContractIdentity("app", "service", 1, 0, "physical shape")
    private val generation = testGeneration(
        listOf(RegisteredTableSpec("business", "person", listOf("id"))),
        schema = "business",
    )

    @Test
    fun contractIdentityUsesActualSQLiteUserVersion() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE person(id TEXT PRIMARY KEY NOT NULL, full_name TEXT NOT NULL)")
            db.execSQL("PRAGMA user_version = 7")
            val config = OversqliteConfig(
                generationId = 1,
                databaseId = "app",
                serviceId = "service",
                syncTables = listOf(SyncTable("person", syncKeyColumnName = "id")),
            )
            val physical = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db)

            assertEquals(7L, physical.contractIdentity(config, db).localVersion)
        } finally {
            db.close()
        }
    }

    @Test
    fun unconfirmedOpenAndAdmissionSurviveReopenWithoutChangingAttachment() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val store = OversqliteContractStore(db)
            db.transaction(TransactionMode.IMMEDIATE) { assertNull(store.open(identity)) }
            db.transaction(TransactionMode.IMMEDIATE) { assertNull(OversqliteContractStore(db).open(identity)) }
            db.transaction(TransactionMode.IMMEDIATE) { store.install(identity, generation) }
            db.transaction(TransactionMode.IMMEDIATE) { assertEquals(generation, OversqliteContractStore(db).open(identity)) }
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _sync_attachment_state WHERE binding_state='anonymous' AND attached_user_id=''") )
            assertFails { db.transaction(TransactionMode.IMMEDIATE) { store.install(identity, generation) } }
        } finally { db.close() }
    }

    @Test
    fun interruptedAdmissionRollsBackBothRecords() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val store = OversqliteContractStore(db)
            db.transaction(TransactionMode.IMMEDIATE) { store.open(identity) }
            assertFails {
                db.transaction(TransactionMode.IMMEDIATE) {
                    store.install(identity, generation)
                    error("interrupted before commit")
                }
            }
            assertNull(store.read(identity))
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_admitted_contracts"))
        } finally { db.close() }
    }

    @Test
    fun rejectsMissingOrChangedIdentityAndCorruptAdmittedRecord() = runOwnedWebDatabaseTest {
        val scenarios = mapOf(
            "missing identity" to "DELETE FROM _sync_contract_identity",
            "missing descriptor" to "DELETE FROM _sync_admitted_contracts",
            "changed digest" to "UPDATE _sync_admitted_contracts SET descriptor_hash='" + "0".repeat(64) + "'",
            "changed fingerprint" to "UPDATE _sync_contract_identity SET fingerprint='" + "0".repeat(64) + "'",
            "changed service" to "UPDATE _sync_contract_identity SET service_id='other'",
            "changed generation" to "UPDATE _sync_contract_identity SET generation_id=2",
            "changed shape" to "UPDATE _sync_contract_identity SET physical_shape='different'",
            "oversized descriptor" to "UPDATE _sync_admitted_contracts SET descriptor=printf('%.*c',65537,'x')",
        )
        for ((name, sql) in scenarios) {
            val db = newDb()
            try {
                val store = OversqliteContractStore(db)
                db.transaction(TransactionMode.IMMEDIATE) { store.open(identity); store.install(identity, generation) }
                db.execSQL("CREATE TABLE business_value(value TEXT)")
                db.execSQL("INSERT INTO business_value VALUES('retained')")
                db.execSQL(sql)
                assertFails(name) { db.transaction(TransactionMode.IMMEDIATE) { store.open(identity) } }
                assertEquals(1L, db.scalarLong("SELECT count(*) FROM business_value WHERE value='retained'"), name)
            } finally { db.close() }
        }
    }

    @Test
    fun missingDescriptorIsLegalOnlyForUnconfirmedState() = runOwnedWebDatabaseTest {
        val mutations = mapOf(
            "attached" to "UPDATE _sync_attachment_state SET binding_state='attached',attached_user_id='user'",
            "prepared outbox" to "UPDATE _sync_outbox_bundle SET state='prepared'",
            "unexpected record" to "INSERT INTO _sync_admitted_contracts VALUES(2,'','','')",
            "missing identity row" to "DELETE FROM _sync_contract_identity",
        )
        for ((name, sql) in mutations) {
            val db = newDb()
            try {
                val store = OversqliteContractStore(db)
                db.transaction(TransactionMode.IMMEDIATE) { store.open(identity) }
                db.execSQL(sql)
                assertFails(name) { db.transaction(TransactionMode.IMMEDIATE) { store.open(identity) } }
            } finally { db.close() }
        }
    }
}
