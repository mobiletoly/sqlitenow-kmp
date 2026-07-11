package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AuthorityStatus
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.NumericColumnKind
import dev.goquick.sqlitenow.oversqlite.PushOutcome
import dev.goquick.sqlitenow.oversqlite.RemoteSyncOutcome
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.oversqlite.platformsupport.assertConnectedOutcome
import io.ktor.client.HttpClient
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

internal class RealServerGeneratedRichSchemaJvmTest : RealServerHarnessSupport() {
    @Test
    fun generatedBusinessRichDatabase_pushPullAndRebuild_workAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("generated-rich-user")
        val seedDb = newGeneratedDb()
        val pullDb = newGeneratedDb()
        val hydrateDb = newGeneratedDb()
        var seedHttp: HttpClient? = null
        var pullHttp: HttpClient? = null
        var hydrateHttp: HttpClient? = null
        try {
            seedDb.open()
            pullDb.open()
            hydrateDb.open()

            val seedSource = bootstrapSourceId(config.baseUrl) { http -> newGeneratedClient(seedDb, http) }
            val pullSource = bootstrapSourceId(config.baseUrl) { http -> newGeneratedClient(pullDb, http) }
            val hydrateSource = bootstrapSourceId(config.baseUrl) { http -> newGeneratedClient(hydrateDb, http) }
            seedHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, seedSource),
            )
            pullHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, pullSource),
            )
            hydrateHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, hydrateSource),
            )

            val seed = newGeneratedClient(seedDb, seedHttp)
            val pull = newGeneratedClient(pullDb, pullHttp)
            val hydrate = newGeneratedClient(hydrateDb, hydrateHttp)

            seed.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = seed.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            pull.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = pull.attach(userId).getOrThrow(),
            )
            hydrate.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = hydrate.attach(userId).getOrThrow(),
            )

            val fixture = insertGeneratedRichRows(seedDb.connection())
            assertEquals(PushOutcome.COMMITTED, seed.pushPending().getOrThrow().outcome)
            assertEquals(RemoteSyncOutcome.APPLIED_INCREMENTAL, pull.pullToStable().getOrThrow().outcome)
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, hydrate.rebuild().getOrThrow().outcome)

            assertGeneratedRichRows(pullDb.connection(), fixture)
            assertGeneratedRichRows(hydrateDb.connection(), fixture)
            assertFalse(pull.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertFalse(hydrate.syncStatus().getOrThrow().pending.hasPendingSyncData)
        } finally {
            seedHttp?.close()
            pullHttp?.close()
            hydrateHttp?.close()
            seedDb.close()
            pullDb.close()
            hydrateDb.close()
        }
    }

    private fun newGeneratedDb(): RealServerGeneratedDatabase =
        RealServerGeneratedDatabase(
            dbName = ":memory:",
            migration = VersionBasedDatabaseMigrations(),
            debug = true,
        )

    private fun newGeneratedClient(
        db: RealServerGeneratedDatabase,
        http: HttpClient,
    ): OversqliteClient =
        db.newOversqliteClient(
            schema = "business",
            httpClient = http,
            uploadLimit = 8,
            downloadLimit = 8,
			syncTables = RealServerGeneratedDatabase.syncTables.map { table ->
				if (table.tableName != "typed_rows") table else table.copy(
					numericColumns = mapOf(
						"count_value" to NumericColumnKind.EXACT_INT64,
						"enabled_flag" to NumericColumnKind.EXACT_INT64,
						"rating" to NumericColumnKind.APPROXIMATE,
					),
				)
			},
        )

    private suspend fun insertGeneratedRichRows(db: SafeSQLiteConnection): GeneratedRichFixture {
        val fixture = GeneratedRichFixture(
            userId = randomRealServerUuid(),
            postId = randomRealServerUuid(),
            categoryRootId = randomRealServerUuid(),
            categoryChildId = randomRealServerUuid(),
            fileIdHex = "00112233445566778899aabbccddeeff",
            reviewIdHex = "10112233445566778899aabbccddeeff",
            typedRowId = randomRealServerUuid(),
        )
        db.execSQL(
            """
            INSERT INTO users(id, name, email)
            VALUES('${fixture.userId}', 'Generated Rich Ada', 'generated-rich@example.com')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO posts(id, title, content, author_id)
            VALUES('${fixture.postId}', 'Generated Rich Post', 'from generated rich schema', '${fixture.userId}')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO categories(id, name, parent_id)
            VALUES('${fixture.categoryRootId}', 'Generated Root', NULL)
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO categories(id, name, parent_id)
            VALUES('${fixture.categoryChildId}', 'Generated Child', '${fixture.categoryRootId}')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO files(id, name, data)
            VALUES(x'${fixture.fileIdHex}', 'Generated File', x'cafebabe')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO file_reviews(id, review, file_id)
            VALUES(x'${fixture.reviewIdHex}', 'Generated Review', x'${fixture.fileIdHex}')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
            VALUES('${fixture.typedRowId}', 'Generated Typed', NULL, 42, 1, 1.25, x'deadbeef', '2026-03-24T18:42:11Z')
            """.trimIndent(),
        )
        return fixture
    }

    private suspend fun assertGeneratedRichRows(
        db: SafeSQLiteConnection,
        fixture: GeneratedRichFixture,
    ) {
        assertEquals("Generated Rich Ada", scalarText(db, "SELECT name FROM users WHERE id = '${fixture.userId}'"))
        assertEquals("Generated Rich Post", scalarText(db, "SELECT title FROM posts WHERE id = '${fixture.postId}'"))
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM categories child
                JOIN categories root ON root.id = child.parent_id
                WHERE child.id = '${fixture.categoryChildId}'
                  AND root.id = '${fixture.categoryRootId}'
                """.trimIndent(),
            ),
        )
        assertEquals("Generated File", scalarText(db, "SELECT name FROM files WHERE lower(hex(id)) = '${fixture.fileIdHex}'"))
        assertEquals("cafebabe", scalarText(db, "SELECT lower(hex(data)) FROM files WHERE lower(hex(id)) = '${fixture.fileIdHex}'"))
        assertEquals(
            fixture.fileIdHex,
            scalarText(db, "SELECT lower(hex(file_id)) FROM file_reviews WHERE lower(hex(id)) = '${fixture.reviewIdHex}'"),
        )
        assertEquals("Generated Typed", scalarText(db, "SELECT name FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
        assertEquals(42L, scalarLong(db, "SELECT count_value FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
        assertEquals("deadbeef", scalarText(db, "SELECT lower(hex(data)) FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
    }

    private data class GeneratedRichFixture(
        val userId: String,
        val postId: String,
        val categoryRootId: String,
        val categoryChildId: String,
        val fileIdHex: String,
        val reviewIdHex: String,
        val typedRowId: String,
    )
}
