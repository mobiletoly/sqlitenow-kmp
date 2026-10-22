package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.*

internal class ProvenanceContextCleanupTest : SurvivorTestSupport() {
    @Test fun allReferencedSlicesAdvanceWithBoundedCandidateAndProbeWork() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                val store = OversqliteProvenanceContextStore(db)
                val state = provenanceTestState(client)
                for (n in 1..40) db.transaction(TransactionMode.IMMEDIATE) {
                    val id = store.target(state, source(n))
                    db.execSQL("INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known) VALUES('main','nodes','{\"id\":\"retained-$n\"}',1,'deleted',NULL,json_object('context_id',$id,'origin','pull','bundle_sequence',1),1)")
                }
                val first = store.cleanup(8)
                val second = store.cleanup(8)
                assertEquals(0, first.deleted); assertEquals(0, second.deleted)
                assertTrue(second.cursor > first.cursor)
                for (slice in listOf(first, second)) {
                    assertEquals(8, slice.examined); assertTrue(slice.referenceProbes <= slice.examined * 4)
                }
                assertEquals(41L, scalarLong(db, "SELECT count(*) FROM _sync_provenance_contexts"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun persistedCursorResumesAndReleasedContextsStabilizeAfterReopen() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("context-cleanup-restart")
        try {
            withSurvivorFixture(dbPath = path) { _, db, http, client ->
                val store = OversqliteProvenanceContextStore(db)
                for (n in 1..20) store.target(provenanceTestState(client), source(n))
                val first = store.cleanup(4)
                assertTrue(first.examined in 1..4)
                val cursor = scalarLong(db, "SELECT cleanup_cursor FROM _sync_provenance_state")
                assertEquals(first.cursor, cursor)
                client.closeFromCoordinatedOwner(); db.close()
                val reopened = newFileBackedDb(path)
                val next = newClient(reopened, http, syncTables = nodeTables)
                try {
                    assertEquals(cursor, scalarLong(reopened, "SELECT cleanup_cursor FROM _sync_provenance_state"))
                    next.openAndConnect("user-1").getOrThrow()
                    val restarted = OversqliteProvenanceContextStore(reopened)
                    repeat(8) { val slice = restarted.cleanup(4); assertTrue(slice.examined <= 4); assertTrue(slice.referenceProbes <= 16) }
                    assertEquals(1L, scalarLong(reopened, "SELECT count(*) FROM _sync_provenance_contexts"))
                    assertEquals(4L, scalarLong(reopened, "SELECT count(*) FROM nodes"))
                } finally { next.closeFromCoordinatedOwner(); reopened.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun cancellationAfterDeleteRollsBackCursorAndContextMutation() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                val store = OversqliteProvenanceContextStore(db)
                for (n in 1..8) store.target(provenanceTestState(client), source(n))
                val count = scalarLong(db, "SELECT count(*) FROM _sync_provenance_contexts")
                val cursor = scalarLong(db, "SELECT cleanup_cursor FROM _sync_provenance_state")
                val original = db.ref
                var owner: Job? = null; var deleted = false
                val wrapper = object : SQLiteConnection by original {
                    override fun prepare(sql: String): SQLiteStatement {
                        val statement = original.prepare(sql)
                        if (!sql.startsWith("DELETE FROM _sync_provenance_contexts")) return statement
                        return object : SQLiteStatement by statement {
                            override fun step(): Boolean {
                                val result = statement.step()
                                if (!deleted) { deleted = true; checkNotNull(owner).cancel(CancellationException("after native deletion")) }
                                return result
                            }
                        }
                    }
                }
                val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
                db.withExclusiveAccess { field.set(db, wrapper) }
                try {
                    supervisorScope {
                        val attempt = async { owner = currentCoroutineContext()[Job]; store.cleanup(8) }
                        assertFailsWith<CancellationException> { attempt.await() }
                    }
                } finally { db.withExclusiveAccess { field.set(db, original) } }
                assertTrue(deleted)
                assertEquals(count, scalarLong(db, "SELECT count(*) FROM _sync_provenance_contexts"))
                assertEquals(cursor, scalarLong(db, "SELECT cleanup_cursor FROM _sync_provenance_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun memoChargesExactLimitFallsBackAtPlusOneAndReleasesItsLifetime() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                db.execSQL("UPDATE nodes SET name='memo-local' WHERE id='P'")
                val state = provenanceTestState(client)
                val config = provenanceTestConfig(client)
                val payload = scalarText(db, "SELECT base_payload FROM _sync_dirty_rows")
                val observation = scalarText(db, "SELECT base_provenance FROM _sync_dirty_rows")
                val local = OversqliteLocalStore(db, Json) { state.validated }
                val base = OversqliteWorkspace(db, config).usage("memo-proof")
                val limited = config.copy(maxSyncWorkspaceEntries = base.entries + 2, maxSyncWorkspaceBytes = base.bytes + 128)
                db.transaction(TransactionMode.IMMEDIATE) {
                    val alternate = OversqliteProvenanceContextStore(db).target(state, source(99))
                    val other = JsonObject(Json.parseToJsonElement(observation).jsonObject + ("context_id" to JsonPrimitive(alternate))).toString()
                    // Corrupt/ineligible captured identities fail through the historical-base
                    // boundary; current settings must never fabricate replacement evidence.
                    val columns = listOf("database_id", "service_id", "account_binding", "account_id", "generation_id", "generation_fingerprint", "admission", "local_version", "physical_shape", "origin_source_id")
                    for ((column, replacement) in listOf("account_id" to "'another-account'", "generation_id" to "2", "generation_fingerprint" to "'${"b".repeat(64)}'", "physical_shape" to "physical_shape||' '")) {
                        db.execSQL("INSERT INTO _sync_provenance_contexts(${columns.joinToString()}) SELECT ${columns.joinToString { if (it == column) replacement else it }} FROM _sync_provenance_contexts WHERE context_id=(SELECT active_context_id FROM _sync_provenance_state)")
                        val incompatible = scalarLong(db, "SELECT last_insert_rowid()")
                        val bad = JsonObject(Json.parseToJsonElement(observation).jsonObject + ("context_id" to JsonPrimitive(incompatible))).toString()
                        assertFailsWith<ResolutionBaseUnavailableException> { local.validateHistoricalBase("nodes", 1, HistoricalBase("present", payload, bad)) }
                    }
                    for (bad in listOf("{}", "{\"context_id\":9223372036854775807,\"origin\":\"settlement\",\"bundle_sequence\":1}",
                        observation.dropLast(1) + ",\"extra\":0}")) {
                        assertFailsWith<ResolutionBaseUnavailableException> { local.validateHistoricalBase("nodes", 1, HistoricalBase("present", payload, bad)) }
                    }
                    assertEquals(payload, scalarText(db, "SELECT base_payload FROM _sync_dirty_rows"))
                    val scope = ProvenanceReadScope(db, limited)
                    val sql = ReplaySqlObservation.install(db)
                    try {
                        withContext(scope) {
                            repeat(32) { local.validateHistoricalBase("nodes", 1, HistoricalBase("present", payload, observation)) }
                            assertEquals(WorkspaceCharge(2, 128), scope.charge)
                            assertEquals(WorkspaceCharge(base.entries + 2, base.bytes + 128), OversqliteWorkspace(db, limited).usage("memo-proof"))
                            local.validateHistoricalBase("nodes", 1, HistoricalBase("present", payload, other))
                            assertEquals(WorkspaceCharge(2, 128), scope.charge, "plus-one growth falls back without an uncharged retained pair")
                        }
                        val eligibility = sql.counts.filterKeys { it.startsWith("SELECT c.database_id=i.database_id") }.values.single()
                        assertEquals(2L, eligibility.steps, "one full validation per distinct context; repeated rows use the bounded memo")
                    } finally { scope.clear(); sql.restore() }
                    assertEquals(WorkspaceCharge(), scope.charge)
                }
                assertEquals(base, OversqliteWorkspace(db, config).usage("memo-proof"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun bindingPromotesDistinctRootsWithoutMaterializingContextOrOwnerSets() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                db.execSQL("UPDATE nodes SET name='binding-local' WHERE id='P'")
                val payload = scalarText(db, "SELECT base_payload FROM _sync_dirty_rows")
                val columns = listOf("database_id", "service_id", "account_binding", "account_id", "generation_id", "generation_fingerprint", "admission", "local_version", "physical_shape", "origin_source_id")
                for (n in 1..24) {
                    val fields = columns.joinToString { when (it) {
                        "account_binding" -> "'anonymous'"
                        "account_id", "generation_fingerprint" -> "''"
                        "admission" -> "'unadmitted'"
                        "origin_source_id" -> "'${source(n)}'"
                        else -> it
                    } }
                    db.execSQL("INSERT INTO _sync_provenance_contexts(${columns.joinToString()}) SELECT $fields FROM _sync_provenance_contexts WHERE context_id=(SELECT active_context_id FROM _sync_provenance_state)")
                    val id = scalarLong(db, "SELECT last_insert_rowid()")
                    repeat(2) { row ->
                        db.execSQL("INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known) VALUES('main','nodes','{\"id\":\"binding-$n-$row\"}',1,'deleted',NULL,json_object('context_id',$id,'origin','pull','bundle_sequence',1),1)")
                    }
                    if (n == 1) db.execSQL("UPDATE _sync_dirty_rows SET base_provenance=json_set(base_provenance,'$.context_id',$id)")
                }
                val config = provenanceTestConfig(client)
                val before = OversqliteWorkspace(db, config).usage("binding-proof")
                val scope = ProvenanceReadScope(db, config.copy(maxSyncWorkspaceEntries = before.entries, maxSyncWorkspaceBytes = before.bytes))
                val original = db.ref
                val statements = linkedSetOf<String>(); var textReads = 0; var candidateSteps = 0; var promotionSteps = 0
                val wrapper = object : SQLiteConnection by original {
                    override fun prepare(sql: String): SQLiteStatement {
                        statements += sql
                        val statement = original.prepare(sql)
                        return object : SQLiteStatement by statement {
                            override fun getText(index: Int): String { textReads++; return statement.getText(index) }
                            override fun step(): Boolean {
                                if (sql.startsWith("SELECT c.context_id FROM _sync_provenance_contexts c CROSS JOIN")) candidateSteps++
                                if (sql.startsWith("INSERT OR IGNORE INTO _sync_provenance_contexts(") && sql.contains(" VALUES(")) promotionSteps++
                                return statement.step()
                            }
                        }
                    }
                }
                val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
                db.withExclusiveAccess { field.set(db, wrapper) }
                try { withContext(scope) { OversqliteProvenanceContextStore(db).refreshActiveAndBind() } }
                finally { scope.clear(); db.withExclusiveAccess { field.set(db, original) } }
                assertEquals(25, candidateSteps, "one scalar selection per distinct input context plus exhaustion, not per owner row")
                assertEquals(24, promotionSteps)
                assertEquals(0, textReads, "complete contexts remain in primary SQLite storage")
                for (sql in statements) db.prepare("EXPLAIN $sql").use { statement ->
                    val plan = buildList {
                        while (statement.step()) add(listOf(statement.getLong(0).toString(), statement.getText(1),
                            statement.getLong(2).toString(), statement.getLong(3).toString(), statement.getLong(4).toString(),
                            if (statement.isNull(5)) "" else statement.getText(5)))
                    }
                    for ((index, row) in plan.withIndex()) {
                        if (row[1] == "OpenEphemeral") {
                            // Some SQLite versions compile a fixed discriminator CHECK IN list
                            // as literal schema scratch. It contains no retained/input values.
                            val literalRows = (0 until 5).map { n -> plan.getOrNull(index + 1 + n * 3) }
                            val fixedDomain = literalRows.map { it?.get(5) }.toSet() == setOf("never_published", "snapshot", "pull", "settlement", "explicit_conflict")
                            val programEnd = (index + 1 until plan.size).firstOrNull { plan[it][0] == "0" } ?: plan.size
                            val writes = plan.subList(index + 1, programEnd).filter { it[1] == "IdxInsert" && it[2] == row[2] }
                            assertTrue(fixedDomain && writes.size == 5 && (0 until 5).all { n ->
                                plan.getOrNull(index + 1 + n * 3)?.get(1) == "String8" &&
                                    plan.getOrNull(index + 2 + n * 3)?.let { it[1] == "MakeRecord" && it[3] == "1" } == true &&
                                    plan.getOrNull(index + 3 + n * 3)?.let { it[1] == "IdxInsert" && it[2] == row[2] } == true
                            }, "only the exact fixed discriminator CHECK literals may use schema scratch: SQL=$sql plan=${plan.drop(index).take(22)}")
                        } else assertFalse(row[1] in setOf("SorterOpen", "RowSetAdd"),
                            "binding must not materialize a derived context/ID/owner relation: SQL=$sql opcode=$row")
                    }
                }
                for (n in 1..24) assertEquals(2L, scalarLong(db, """SELECT count(*) FROM _sync_row_state r
                    JOIN _sync_provenance_contexts c ON c.context_id=r.server_context_id CROSS JOIN _sync_contract_identity i
                    WHERE r.key_json LIKE '%binding-$n-%' AND c.account_binding='attached' AND c.account_id='user-1'
                    AND c.admission='admitted' AND c.generation_fingerprint=i.fingerprint AND c.physical_shape=i.physical_shape
                    AND c.origin_source_id='${source(n)}' AND r.row_version=1 AND r.server_kind='deleted'"""))
                assertEquals(payload, scalarText(db, "SELECT base_payload FROM _sync_dirty_rows"))
                assertEquals(source(1), scalarText(db, "SELECT c.origin_source_id FROM _sync_dirty_rows d JOIN _sync_provenance_contexts c ON c.context_id=d.base_context_id"))
                assertEquals(25L, scalarLong(db, "SELECT count(*) FROM _sync_provenance_contexts"))
                assertEquals(before, OversqliteWorkspace(db, config).usage("binding-proof"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private fun source(n: Int) = "00000000-0000-4000-8000-${n.toString().padStart(12, '0')}"
}
