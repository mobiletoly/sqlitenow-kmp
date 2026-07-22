package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import java.nio.file.Files
import kotlin.io.path.deleteIfExists
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

internal class SharedInvalidationBehaviorFixtureTest : CrossTargetSyncTestSupport() {
    private val fixtureFile = oversqliteContractFixture("invalidation/behavior/basic.json")

    @Test
    fun kmpSharedInvalidationBehaviorFixturesExecuteAgainstRuntime() = runBlocking {
        val spec = json.decodeFromString(InvalidationBehaviorSpec.serializer(), fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            runCase(case)
        }
    }

    private suspend fun runCase(case: InvalidationBehaviorCase) {
        when (case.action) {
            "incrementalPull" -> incrementalPull(case)
            "snapshotHydrate" -> snapshotHydrate(case)
            "committedReplay" -> committedReplay(case)
            "conflictRewrite" -> conflictRewrite(case)
            "syncUnion" -> syncUnion(case)
            "successfulDetach" -> successfulDetach(case)
            "blockedDetach" -> blockedDetach(case)
            "noopDetach" -> noopDetach(case)
            else -> error("${case.name}: unknown invalidation action ${case.action}")
        }
    }

    private suspend fun incrementalPull(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { followerDb ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            followerDb.open()
            val follower = newDatabaseClient(followerDb, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)
                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild().getOrThrow()
                follower.rebuild().getOrThrow()

                assertInvalidation(case, followerDb.flowFor(case.watchTables)) {
                    insertUser(leaderDb, "u1", "Ada")
                    leader.pushPending().getOrThrow()
                    follower.pullToStable().getOrThrow()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    private suspend fun snapshotHydrate(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { followerDb ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            followerDb.open()
            val follower = newDatabaseClient(followerDb, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)
                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild().getOrThrow()
                follower.rebuild().getOrThrow()

                assertInvalidation(case, followerDb.flowFor(case.watchTables)) {
                    insertUser(leaderDb, "u1", "Ada")
                    leader.pushPending().getOrThrow()
                    follower.rebuild().getOrThrow()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    private suspend fun committedReplay(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val http = server.newHttpClient()
        withReactiveDatabase { database ->
            database.open()
            val client = newDatabaseClient(database, http)
            try {
                client.openAndConnect("user-1").getOrThrow()
                client.rebuild().getOrThrow()

                assertInvalidation(case, database.flowFor(case.watchTables)) {
                    database.connection().execSQL("INSERT INTO users(id, name) VALUES('u1', 'Ada')")
                    client.pushPending().getOrThrow()
                }
            } finally {
                client.close()
                http.close()
            }
        }
    }

    private suspend fun conflictRewrite(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)
                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild().getOrThrow()
                follower.rebuild().getOrThrow()
                insertUser(leaderDb, "u1", "Original")
                leader.pushPending().getOrThrow()
                follower.pullToStable().getOrThrow()

                assertInvalidation(case, database.flowFor(case.watchTables)) {
                    database.connection().execSQL("UPDATE users SET name = 'Local Edit' WHERE id = 'u1'")
                    server.conflictOverride = { row, _, _ ->
                        if (row.table == "users" && row.key["id"] == "u1") {
                            PushConflictDetails(
                                schema = row.schema,
                                table = row.table,
                                key = row.key,
                                op = row.op,
                                baseRowVersion = row.baseRowVersion,
                                serverRowVersion = 99,
                                serverRowDeleted = false,
                                serverRow = buildJsonObject {
                                    put("id", JsonPrimitive("u1"))
                                    put("name", JsonPrimitive("Server Wins"))
                                },
                            )
                        } else {
                            null
                        }
                    }
                    follower.pushPending().getOrThrow()
                }
            } finally {
                server.conflictOverride = null
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    private suspend fun syncUnion(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)
                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild().getOrThrow()
                follower.rebuild().getOrThrow()
                database.connection().execSQL("INSERT INTO users(id, name) VALUES('local-user', 'Local User')")
                database.connection().execSQL("INSERT INTO posts(id, user_id, title) VALUES('p1', 'local-user', 'Local Post')")
                insertUser(leaderDb, "remote-user", "Remote User")
                leader.pushPending().getOrThrow()

                assertInvalidation(case, database.flowFor(case.watchTables)) {
                    follower.sync().getOrThrow()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    private suspend fun successfulDetach(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)
                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild().getOrThrow()
                follower.rebuild().getOrThrow()
                insertUser(leaderDb, "u1", "Ada")
                leader.pushPending().getOrThrow()
                follower.pullToStable().getOrThrow()

                assertInvalidation(case, database.flowFor(case.watchTables)) {
                    assertEquals(DetachOutcome.DETACHED, follower.detach().getOrThrow(), case.name)
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    private suspend fun blockedDetach(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val http = server.newHttpClient()
        withReactiveDatabase { database ->
            database.open()
            val client = newDatabaseClient(database, http)
            try {
                client.openAndConnect("user-1").getOrThrow()
                client.rebuild().getOrThrow()

                assertInvalidation(case, database.flowFor(case.watchTables)) {
                    database.connection().execSQL("INSERT INTO users(id, name) VALUES('u1', 'Unsynced')")
                    assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, client.detach().getOrThrow(), case.name)
                }
            } finally {
                client.close()
                http.close()
            }
        }
    }

    private suspend fun noopDetach(case: InvalidationBehaviorCase) {
        val server = MockSyncServer()
        val http = server.newHttpClient()
        withReactiveDatabase { database ->
            database.open()
            val client = newDatabaseClient(database, http)
            try {
                client.open().getOrThrow()
                database.connection().execSQL(
                    """
                    UPDATE _sync_operation_state
                    SET kind = 'remote_replace',
                        target_user_id = 'user-1',
                        staged_snapshot_id = '',
                        snapshot_bundle_seq = 0,
                        snapshot_row_count = 0
                    WHERE singleton_key = 1
                    """.trimIndent(),
                )

                assertInvalidation(case, database.flowFor(case.watchTables)) {
                    assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow(), case.name)
                }
            } finally {
                client.close()
                http.close()
            }
        }
    }

    private suspend fun assertInvalidation(
        case: InvalidationBehaviorCase,
        flow: Flow<Any>,
        action: suspend () -> Unit,
    ) {
        val (emissions, collector) = collectFlow(flow)
        try {
            receiveNext(emissions)
            action()
            if (case.expectedEmission) {
                assertNotNull(
                    withTimeoutOrNull(5_000) { emissions.receive() },
                    "${case.name}: expected invalidation emission",
                )
            } else {
                assertNull(
                    withTimeoutOrNull(300) { emissions.receive() },
                    "${case.name}: expected no invalidation emission",
                )
            }
        } finally {
            collector.cancelAndJoin()
        }
    }

    private fun newDatabaseClient(
        database: ReactiveSyncDatabase,
        http: io.ktor.client.HttpClient,
        resolver: Resolver = ServerWinsResolver,
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient(
            db = database.connection(),
            config = OversqliteConfig(
                schema = "main",
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
            ),
            http = http,
            resolver = resolver,
        )
    }

    private suspend fun withReactiveDatabase(
        block: suspend (ReactiveSyncDatabase) -> Unit,
    ) {
        val dbPath = Files.createTempFile("sqlitenow-oversqlite-shared-invalidation", ".db")
        val database = ReactiveSyncDatabase(dbPath.toString())
        try {
            block(database)
        } finally {
            if (database.isOpen()) {
                database.close()
            }
            dbPath.deleteIfExists()
        }
    }

    private fun <T> collectFlow(flow: Flow<T>): Pair<Channel<T>, Job> {
        val emissions = Channel<T>(Channel.UNLIMITED)
        val collector = CoroutineScope(Dispatchers.Default).launch {
            flow.collect { value ->
                emissions.send(value)
            }
        }
        return emissions to collector
    }

    private suspend fun <T> receiveNext(channel: Channel<T>): T {
        return withTimeout(5_000) { channel.receive() }
    }

    private class ReactiveSyncDatabase(
        dbName: String,
    ) : SqliteNowDatabase(dbName, ReactiveSyncMigration()) {
        fun flowFor(watchTables: Set<String>): Flow<Any> {
            return if ("posts" in watchTables) usersAndPostsFlow() else userNamesFlow()
        }

        private fun userNamesFlow(): Flow<Any> = createReactiveQueryFlow(setOf("users")) {
            val conn = connection()
            conn.withExclusiveAccess {
                conn.prepare("SELECT name FROM users ORDER BY id").use { statement ->
                    buildList {
                        while (statement.step()) {
                            add(statement.getText(0))
                        }
                    }
                }
            }
        }

        private fun usersAndPostsFlow(): Flow<Any> = createReactiveQueryFlow(setOf("users", "posts")) {
            val conn = connection()
            conn.withExclusiveAccess {
                UsersAndPosts(
                    users = conn.prepare("SELECT name FROM users ORDER BY id").use { statement ->
                        buildList {
                            while (statement.step()) {
                                add(statement.getText(0))
                            }
                        }
                    },
                    posts = conn.prepare("SELECT title FROM posts ORDER BY id").use { statement ->
                        buildList {
                            while (statement.step()) {
                                add(statement.getText(0))
                            }
                        }
                    },
                )
            }
        }
    }

    private data class UsersAndPosts(
        val users: List<String>,
        val posts: List<String>,
    )

    private class ReactiveSyncMigration : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                conn.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
                conn.execSQL(
                    """
                    CREATE TABLE posts (
                        id TEXT PRIMARY KEY NOT NULL,
                        user_id TEXT NOT NULL REFERENCES users(id),
                        title TEXT NOT NULL
                    )
                    """.trimIndent(),
                )
                return 0
            }
            return currentVersion
        }
    }

    @Serializable
    private data class InvalidationBehaviorSpec(
        val formatVersion: Int,
        val cases: List<InvalidationBehaviorCase>,
    )

    @Serializable
    private data class InvalidationBehaviorCase(
        val name: String,
        val description: String,
        val action: String,
        val watchTables: Set<String>,
        val expectedEmission: Boolean,
    )
}
