package dev.goquick.sqlitenow.oversqlite

import java.security.MessageDigest
import kotlin.io.path.readBytes
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlinx.serialization.json.*

internal class SharedFinalStateUniqueFixtureTest : SurvivorTestSupport() {
    @Test
    fun everyDeclaredUniqueCaseExecutesAgainstSQLite() = runOwnedWebDatabaseTest {
        val path = oversqliteContractFixture("local-apply/unique-final-state.json")
        val fixture = Json.parseToJsonElement(path.readText()).jsonObject
        val hash =
            MessageDigest.getInstance("SHA-256")
                .digest(
                    path.parent.parent.parent
                        .resolve("specs/oversqlite-fk-safe-sync-and-automatic-resolution.md")
                        .readBytes()
                )
                .joinToString("") { "%02x".format(it) }
        assertEquals(
            fixture
                .getValue("authority")
                .jsonObject
                .getValue("kotlinSpecSHA256")
                .jsonPrimitive
                .content,
            hash,
        )
        for (value in fixture.getValue("cases").jsonArray) {
            val case = value.jsonObject
            withSurvivorFixture { server, db, _, client ->
                db.execSQL(case.getValue("indexSQL").jsonPrimitive.content)
                case["additionalIndexes"]?.jsonArray?.forEach {
                    db.execSQL(it.jsonPrimitive.content)
                }
                case["applicationTriggerSQL"]?.jsonPrimitive?.content?.let { db.execSQL(it) }
                val effects = case.getValue("effects").jsonArray.map { it.jsonObject }
                server.addRemoteBundleForTest(
                    effects.map {
                        BundleRow(
                            "main",
                            "nodes",
                            mapOf("id" to it.getValue("id").jsonPrimitive.content),
                            "UPDATE",
                            2,
                            it,
                        )
                    }
                )
                val result = client.pullToStable()
                val reason = case["expectBlockedReason"]?.jsonPrimitive?.content
                val expected =
                    if (reason == null) {
                        result.getOrThrow()
                        effects
                    } else {
                        val error =
                            assertIs<FinalStateApplyBlockedException>(
                                result.exceptionOrNull(),
                                case.getValue("id").jsonPrimitive.content,
                            )
                        assertEquals(reason, error.reason)
                        assertNotNull(error.table)
                        fixture.getValue("initial").jsonArray.map { it.jsonObject }
                    }
                assertEquals(
                    if (reason == null) 2L else 1L,
                    scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
                )
                for (row in expected) {
                    val id = row.getValue("id").jsonPrimitive.content
                    assertEquals(
                        row.getValue("name").jsonPrimitive.content,
                        scalarText(db, "SELECT name FROM nodes WHERE id='$id'"),
                    )
                    val parent = row["parent_id"]
                    if (parent != JsonNull)
                        assertEquals(
                            parent!!.jsonPrimitive.content,
                            scalarText(db, "SELECT parent_id FROM nodes WHERE id='$id'"),
                        )
                    else
                        assertEquals(
                            1L,
                            scalarLong(db, "SELECT parent_id IS NULL FROM nodes WHERE id='$id'"),
                        )
                }
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
            }
        }
    }

    @Test
    fun committedReplayPreservesDirtyClosure() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            db.execSQL("UPDATE nodes SET name='changed child' WHERE id='C'")
            db.execSQL(
                "CREATE TRIGGER application_delete AFTER DELETE ON nodes BEGIN SELECT 1; END"
            )
            server.beforePushResponse = {
                db.execSQL("UPDATE nodes SET name='local grandchild' WHERE id='D'")
            }
            server.normalizePushEffects = {
                listOf(
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "C"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement(
                            """{"id":"C","name":"changed child","parent_id":"P"}"""
                        ),
                    ),
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "P"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"P","name":"other","parent_id":null}"""),
                    ),
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "Q"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"Q","name":"parent","parent_id":null}"""),
                    ),
                )
            }
            assertEquals(
                "application_delete",
                assertIs<FinalStateApplyBlockedException>(client.pushPending().exceptionOrNull())
                    .trigger,
            )
            val hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertEquals(
                1L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            db.execSQL("DROP TRIGGER application_delete")
            client.pushPending().getOrThrow()
            assertEquals("local grandchild", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
            assertEquals(
                1L,
                scalarLong(
                    db,
                    "SELECT count(*) FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id')='D'",
                ),
            )
            assertEquals(
                2L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
            assertEquals(hash, Json.parseToJsonElement(server.pushBodies.last()).jsonObject.getValue("canonical_request_hash").jsonPrimitive.content)
        }
    }

    @Test fun indexChangeInvalidatesPreparedGraph() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,http,_ ->
            db.execSQL("CREATE UNIQUE INDEX unique_parent ON nodes(parent_id)")
            val config = OversqliteConfig(1, nodeTables, "fixture", "main")
            val generation = OversqliteContractStore(db).readGeneration(1)
            val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
            val state = RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
                "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            val local = OversqliteLocalStore(db, Json) { validated }
            val owner = OversqliteApplySurvivors(db, local, config,
                OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}, Json)
            suspend fun prepare() {
                db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                    val cache = StatementCache(db)
                    try {
                        owner.prepare(state,"index-change",2,workspaceStatementCache=cache,replayStatementCache=cache) { i ->
                            val id=if(i==0)"P" else "Q"
                            val name=if(i==0)"other" else "free"
                            ApplySurvivor("nodes",id,"""{"id":"$id","name":"$name","parent_id":null}""",true)
                        }
                    } finally { cache.close() }
                }
            }
            prepare()
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'"))
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            prepare()
            assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'"))
        }
    }
}
