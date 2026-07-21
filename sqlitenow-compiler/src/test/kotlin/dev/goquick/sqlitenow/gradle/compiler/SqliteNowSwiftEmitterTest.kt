package dev.goquick.sqlitenow.gradle.compiler

import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftExportConfig
import java.io.File
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqliteNowSwiftEmitterTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun compilerGeneratesSwiftOverlayAndKotlinBridgeWithoutSwiftBackend() {
        val sqlDir = createTaskFixtureSql(tempDir)
        val outputDir = tempDir.resolve("generated-kotlin")
        val swiftOutputDir = tempDir.resolve("generated-swift/AppDatabaseSQLiteNow")

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "AppDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.app.db",
                outputDirectory = outputDir,
                swiftExport = SqliteNowSwiftExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "AppDatabaseSQLiteNow",
                    frameworkModuleName = "AppDatabaseKmp",
                    bridgePackageName = "dev.test.app.swiftbridge",
                ),
            )
        )

        assertFalse(
            SqliteNowCompilerBackend.entries.any { it.name == "SWIFT" },
            "Swift support must stay out of the public backend enum for this spike"
        )
        assertTrue(result.generatedFiles.any { it.name == "AppDatabaseBridge.kt" })
        assertTrue(result.generatedFiles.any { it.name == "AppDatabase.swift" })

        val bridge = outputDir.resolve("dev/test/app/swiftbridge/AppDatabaseBridge.kt").readText()
        assertTrue(bridge.contains("public class AppTaskSelectAllQuery"))
        assertTrue(bridge.contains("public suspend fun list(): List<AppTaskRow>"))
        assertTrue(bridge.contains("public suspend fun oneOrNull(): AppTaskRow?"))
        assertTrue(bridge.contains("public fun observe("))
        assertTrue(bridge.contains("public class AppDatabaseSwiftOverlayErrorPayload("))
        assertTrue(bridge.contains("public class AppDatabaseSwiftOverlayException("))
        assertTrue(bridge.contains("private suspend inline fun <T> mapSwiftOverlayErrors("))
        assertTrue(bridge.contains("onError: (AppDatabaseSwiftOverlayErrorPayload) -> Unit"))
        assertTrue(bridge.contains("import kotlinx.coroutines.CancellationException"))
        assertTrue(bridge.contains("if (error is CancellationException) throw error"))
        assertTrue(bridge.contains("mapSwiftOverlayErrors(preferredCategory = \"migration\") {"))
        val misuseCategoryIndex = bridge.indexOf(
            "if (containsCause { it is IllegalStateException || it is IllegalArgumentException || " +
                "it is NoSuchElementException }) return \"misuse\""
        )
        val sqliteCategoryIndex = bridge.indexOf(
            "if (containsCause { it is SqliteException }) return preferredCategory ?: \"sqlite\""
        )
        val preferredCategoryIndex = bridge.indexOf("if (preferredCategory != null) return preferredCategory")
        assertTrue(misuseCategoryIndex >= 0)
        assertTrue(sqliteCategoryIndex > misuseCategoryIndex)
        assertTrue(preferredCategoryIndex > sqliteCategoryIndex)
        assertTrue(bridge.contains("public val titles: Collection<String>"))
        assertTrue(bridge.contains("mapSwiftOverlayErrors {"))
        assertTrue(bridge.contains("database.task.deleteAll()"))
        assertFalse(bridge.contains("database.task.deleteAll(Unit)"))
        assertTrue(bridge.contains("public class AppDatabaseMutationBatch"))
        assertTrue(bridge.contains("public suspend fun transaction(batch: AppDatabaseMutationBatch)"))
        assertFalse(bridge.contains("BridgeError"))
        assertFalse(bridge.contains("dev.goquick.sqlitenow.oversqlite"))
        assertFalse(bridge.contains("newOversqliteClient"))

        val swift = swiftOutputDir.resolve("AppDatabase.swift").readText()
        assertTrue(swift.contains("public enum SQLiteNowError: Error"))
        assertTrue(swift.contains("public final class SQLiteNowSelectQuery<Row: Sendable>"))
        assertTrue(swift.contains("public func stream() -> AsyncThrowingStream<[Row], Error>"))
        assertTrue(swift.contains("if let overlayError = (error as NSError).userInfo[\"K\" + \"otlinException\"] as? AppDatabaseSwiftOverlayException"))
        assertTrue(swift.contains("fileprivate static func from(_ payload: AppDatabaseSwiftOverlayErrorPayload, underlying: Error?) -> SQLiteNowError"))
        assertTrue(swift.contains("case \"sqlite\": return .sqlite(message: payload.message, underlying: underlying)"))
        assertFalse(swift.contains("lowercased.contains"))
        assertFalse(swift.contains("case sync(message: String, underlying: Error?)"))
        assertTrue(swift.contains("public func selectAll() -> SQLiteNowSelectQuery<TaskRow>"))
        assertTrue(swift.contains("public func selectById(_ params: TaskSelectByIdParams) -> SQLiteNowSelectQuery<TaskRow>"))
        assertTrue(swift.contains("public func selectByTitles(_ params: TaskSelectByTitlesParams) -> SQLiteNowSelectQuery<TaskRow>"))
        assertTrue(swift.contains("public func updateDone(_ params: TaskUpdateDoneParams) async throws"))
        assertTrue(swift.contains("public func deleteAll() async throws"))
        assertTrue(swift.contains("public final class AppDatabaseTransaction"))
        assertTrue(swift.contains("public let task: TaskTransactionQueries"))
        assertTrue(swift.contains("private func sqliteNowData(from value: KotlinByteArray) -> Data"))
        assertTrue(swift.contains("private func sqliteNowByteArray(from data: Data) -> KotlinByteArray"))
        assertFalse(swift.contains("SQLiteNowSyncClient"))
        assertFalse(swift.contains("makeSyncClient"))
    }

    @Test
    fun swiftEmitterExposesDataAndAdapterClosureShape() {
        val sqlDir = createAdapterFixtureSql(tempDir)
        val outputDir = tempDir.resolve("generated-adapter-kotlin")
        val swiftOutputDir = tempDir.resolve("generated-adapter-swift/AppDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "AdapterDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.adapter.db",
                outputDirectory = outputDir,
                swiftExport = SqliteNowSwiftExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "AdapterDatabaseSQLiteNow",
                    frameworkModuleName = "AdapterDatabaseKmp",
                    bridgePackageName = "dev.test.adapter.swiftbridge",
                ),
            )
        )

        val bridge = outputDir.resolve("dev/test/adapter/swiftbridge/AdapterDatabaseBridge.kt").readText()
        assertTrue(bridge.contains("public class AppStatusToSqlValueAdapterResult private constructor("))
        assertTrue(bridge.contains("statusToSqlValue: (dev.test.adapter.db.TaskStatus) -> AppStatusToSqlValueAdapterResult"))
        assertTrue(bridge.contains("sqlValueToStatus: (kotlin.String) -> AppSqlValueToStatusAdapterResult"))
        assertTrue(bridge.contains("docAdapters = DocAdapters("))
        assertTrue(bridge.contains("statusToSqlValue = { input -> statusToSqlValue(input).getOrThrow() }"))
        assertTrue(bridge.contains("sqlValueToStatus = { input -> sqlValueToStatus(input).getOrThrow() }"))
        assertTrue(bridge.contains("public val status: dev.test.adapter.db.TaskStatus"))
        assertTrue(bridge.contains("public val tags: List<String>"))
        assertTrue(bridge.contains("public val statuses: Collection<dev.test.adapter.db.TaskStatus>"))
        assertTrue(bridge.contains("category = \"adapter\""))
        assertFalse(bridge.contains("public val status: String"))
        assertFalse(bridge.contains("public val tags: String"))
        assertFalse(bridge.contains("public val statuses: Collection<TaskStatus>"))

        val swift = swiftOutputDir.resolve("AdapterDatabase.swift").readText()
        assertTrue(swift.contains("public struct DocRow: @unchecked Sendable {"))
        assertFalse(swift.contains("public struct DocRow: Equatable"))
        assertFalse(swift.contains("public struct DocRow: Equatable, Sendable"))
        assertTrue(swift.contains("public let payload: Data"))
        assertTrue(swift.contains("public let optionalPayload: Data?"))
        assertTrue(swift.contains("public let optionalCount: Int64?"))
        assertTrue(swift.contains("public let optionalScore: Double?"))
        assertTrue(swift.contains("public let optionalFlag: Bool?"))
        assertTrue(swift.contains("public let status: TaskStatus"))
        assertTrue(swift.contains("public let tags: [String]"))
        assertTrue(swift.contains("public let statuses: [TaskStatus]"))
        assertTrue(swift.contains("payload = sqliteNowData(from: row.payload)"))
        assertTrue(swift.contains("optionalPayload = row.optionalPayload.map { sqliteNowData(from: \$0) }"))
        assertTrue(swift.contains("optionalCount = sqliteNowInt64(from: row.optionalCount)"))
        assertTrue(swift.contains("optionalScore = sqliteNowDouble(from: row.optionalScore)"))
        assertTrue(swift.contains("optionalFlag = sqliteNowBool(from: row.optionalFlag)"))
        assertTrue(swift.contains("payload: sqliteNowByteArray(from: payload)"))
        assertTrue(swift.contains("optionalPayload: optionalPayload.map { sqliteNowByteArray(from: \$0) }"))
        assertTrue(swift.contains("optionalCount: sqliteNowKotlinLong(from: optionalCount)"))
        assertTrue(swift.contains("optionalScore: sqliteNowKotlinDouble(from: optionalScore)"))
        assertTrue(swift.contains("optionalFlag: sqliteNowKotlinBoolean(from: optionalFlag)"))
        assertTrue(swift.contains("public let statusToSqlValue: (TaskStatus) throws -> String"))
        assertTrue(swift.contains("public let sqlValueToStatus: (String) throws -> TaskStatus"))
        assertTrue(swift.contains("public func selectByStatuses(_ params: DocSelectByStatusesParams) -> SQLiteNowSelectQuery<DocRow>"))
        assertTrue(swift.contains("return AppStatusToSqlValueAdapterResult.companion.success(value: try adapters.statusToSqlValue(value))"))
        assertTrue(swift.contains("return AppStatusToSqlValueAdapterResult.companion.failure(message: String(describing: error))"))
        assertTrue(swift.contains("return AppSqlValueToStatusAdapterResult.companion.success(value: try adapters.sqlValueToStatus(value))"))
        assertTrue(swift.contains("return AppSqlValueToStatusAdapterResult.companion.failure(message: String(describing: error))"))
        assertTrue(swift.contains("throw SQLiteNowError.adapter"))
    }

    @Test
    fun swiftEmitterGeneratesOversqliteBridgeAndSwiftSyncOverlay() {
        val sqlDir = createSwiftOversqliteFixtureSql(tempDir)
        val outputDir = tempDir.resolve("generated-sync-kotlin")
        val swiftOutputDir = tempDir.resolve("generated-sync-swift/SyncAppDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "SyncAppDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.sync.db",
                outputDirectory = outputDir,
                oversqlite = true,
                swiftExport = SqliteNowSwiftExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "SyncAppDatabaseSQLiteNow",
                    frameworkModuleName = "SyncAppDatabaseKmp",
                    bridgePackageName = "dev.test.sync.swiftbridge",
                ),
            )
        )

        val bridge = outputDir.resolve("dev/test/sync/swiftbridge/SyncAppDatabaseBridge.kt").readText()
        assertTrue(bridge.contains("import dev.goquick.sqlitenow.oversqlite.AttachResult"))
        assertTrue(bridge.contains("import dev.goquick.sqlitenow.oversqlite.DetachOutcome"))
        assertTrue(bridge.contains("import dev.goquick.sqlitenow.oversqlite.OversqliteCategorizedException"))
        assertTrue(bridge.contains("import dev.goquick.sqlitenow.oversqlite.SourceInfo"))
        assertTrue(bridge.contains("import dev.goquick.sqlitenow.oversqlite.SyncThenDetachResult"))
        assertTrue(bridge.contains("import dev.goquick.sqlitenow.oversqlite.runAutomaticDownloads"))
        assertTrue(bridge.contains("firstCauseMatching { it is OversqliteCategorizedException }"))
        assertTrue(bridge.contains("return oversqliteCause.category.payloadCategory"))
        assertFalse(bridge.contains("PushConflictRetryExhaustedException"))
        assertFalse(bridge.contains("UploadHttpException"))
        assertFalse(bridge.contains("ConnectLifecycleUnsupportedException"))
        assertTrue(bridge.contains("public class SyncAppDatabaseSyncBridge internal constructor("))
        assertTrue(bridge.contains("private val httpClient = SyncAppDatabaseSyncHttpClient(baseUrl, auth)"))
        assertTrue(bridge.contains("mapSwiftOverlayErrors { client.open().getOrThrow() }"))
        assertTrue(bridge.contains("mapSwiftOverlayErrors { client.attach(userId).getOrThrow().toSyncAppDatabaseAttachResultBridge() }"))
        assertTrue(bridge.contains("client.sourceInfo().getOrThrow().toSyncAppDatabaseSourceInfoBridge()"))
        assertTrue(bridge.contains("client.detach().getOrThrow().toSyncAppDatabaseDetachOutcomeBridge()"))
        assertTrue(bridge.contains("client.syncThenDetach().getOrThrow().toSyncAppDatabaseSyncThenDetachResultBridge()"))
        assertTrue(bridge.contains("private fun SourceInfo.toSyncAppDatabaseSourceInfoBridge(): SyncAppDatabaseSourceInfoBridge"))
        assertTrue(bridge.contains("private fun DetachOutcome.toSyncAppDatabaseDetachOutcomeBridge(): SyncAppDatabaseDetachOutcomeBridge"))
        assertTrue(bridge.contains("private fun SyncThenDetachResult.toSyncAppDatabaseSyncThenDetachResultBridge(): SyncAppDatabaseSyncThenDetachResultBridge"))
        assertTrue(bridge.contains("client.progress.collect { progress ->"))
        assertTrue(bridge.contains("onComplete: () -> Unit,"))
        assertTrue(bridge.contains("onError: (SyncAppDatabaseSwiftOverlayErrorPayload) -> Unit"))
        assertTrue(bridge.contains("if (error is CancellationException) {"))
        assertTrue(bridge.contains("onComplete()"))
        assertTrue(bridge.contains("client.runAutomaticDownloads(config.toOversqliteAutomaticDownloadConfig(database))"))
        assertTrue(bridge.contains("database.newOversqliteClient("))

        val swift = swiftOutputDir.resolve("SyncAppDatabase.swift").readText()
        assertTrue(swift.contains("public struct SQLiteNowSyncAuth: Sendable"))
        assertTrue(swift.contains("refreshedAccessTokenProvider"))
        assertTrue(swift.contains("refreshedAccessToken: (@Sendable () -> String?)? = nil"))
        assertFalse(swift.contains("refreshTokenProvider"))
        assertFalse(swift.contains("refreshToken: (@Sendable () -> String?)? = nil"))
        assertTrue(swift.contains("public final class SQLiteNowSyncClient"))
        assertTrue(swift.contains("case sync(message: String, underlying: Error?)"))
        assertTrue(swift.contains("case \"network\", \"auth\", \"conflict\": return .sync(message: payload.message, underlying: underlying)"))
        assertTrue(swift.contains("public struct SQLiteNowSourceInfo"))
        assertTrue(swift.contains("public enum SQLiteNowSourceRecoveryReason"))
        assertTrue(swift.contains("public enum SQLiteNowDetachOutcome"))
        assertTrue(swift.contains("public struct SQLiteNowSyncThenDetachResult"))
        assertTrue(swift.contains("public func makeSyncClient("))
        assertTrue(swift.contains("baseURL: URL,"))
        assertTrue(swift.contains("auth: SQLiteNowSyncAuth,"))
        assertTrue(swift.contains("public func attach(userId: String) async throws -> SQLiteNowAttachResult"))
        assertTrue(swift.contains("public func sourceInfo() async throws -> SQLiteNowSourceInfo"))
        assertTrue(swift.contains("public func syncStatus() async throws -> SQLiteNowSyncStatus"))
        assertTrue(swift.contains("public func detach() async throws -> SQLiteNowDetachOutcome"))
        assertTrue(swift.contains("public func pushPending() async throws -> SQLiteNowPushReport"))
        assertTrue(swift.contains("public func pullToStable() async throws -> SQLiteNowRemoteSyncReport"))
        assertTrue(swift.contains("public func sync() async throws -> SQLiteNowSyncReport"))
        assertTrue(swift.contains("public func syncThenDetach() async throws -> SQLiteNowSyncThenDetachResult"))
        assertTrue(swift.contains("public func rebuild() async throws -> SQLiteNowRemoteSyncReport"))
        assertTrue(swift.contains("public func progress() -> AsyncThrowingStream<SQLiteNowSyncProgress, Error>"))
        assertTrue(swift.contains("onComplete: {"))
        assertTrue(swift.contains("continuation.finish()"))
        assertTrue(swift.contains("public func startAutomaticDownloads("))
        assertTrue(swift.contains("SQLiteNowBundleChangeWatchMode"))
        assertTrue(swift.contains("return SQLiteNowSyncClient(bridge: syncBridge)"))
        listOf(
            "sync config type" to "public struct SQLiteNowSyncConfig: Equatable, Sendable",
            "transient retry type" to "public struct SQLiteNowTransientRetryPolicy: Equatable, Sendable",
            "capacity retry type" to "public struct SQLiteNowSnapshotCapacityRetryPolicy: Equatable, Sendable",
            "config factory argument" to "config: SQLiteNowSyncConfig = .init(),",
            "resolver factory argument" to "resolver: SQLiteNowSyncResolver? = nil",
            "throwing factory" to ") throws -> SQLiteNowSyncClient",
            "protocol error declaration" to "case `protocol`(message: String, underlying: Error?)",
            "protocol payload mapping" to "case \"protocol\": return .protocol(message: payload.message, underlying: underlying)",
            "pause uploads" to "public func pauseUploads() async throws",
            "resume uploads" to "public func resumeUploads() async throws",
            "pause downloads" to "public func pauseDownloads() async throws",
            "resume downloads" to "public func resumeDownloads() async throws",
            "resolver bridge" to "public class SyncAppDatabaseSyncResolverResultBridge(",
            "snapshot forwarding" to "snapshotChunkRows = snapshotChunkRows",
            "transient forwarding" to "transientRetryPolicy = OversqliteTransientRetryPolicy(",
            "capacity forwarding" to "snapshotCapacityRetryPolicy = OversqliteSnapshotCapacityRetryPolicy(",
        ).forEach { (scenario, expected) ->
            val output = if (scenario.endsWith("forwarding") || scenario == "resolver bridge") bridge else swift
            assertTrue(output.contains(expected), "Missing legacy Swift sync scenario: $scenario")
        }
        assertFalse(
            swift.contains("auth: SQLiteNowSyncAuth,\n        schema: String = \"main\","),
            "Old flat sync factory arguments must be removed.",
        )
    }

    @Test
    fun swiftEmitterExposesExecuteReturningAndDynamicResultModels() {
        val sqlDir = createShowcaseFixtureSql(tempDir)
        val outputDir = tempDir.resolve("generated-showcase-kotlin")
        val swiftOutputDir = tempDir.resolve("generated-showcase-swift/ShowcaseDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "ShowcaseDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.showcase.db",
                outputDirectory = outputDir,
                swiftExport = SqliteNowSwiftExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "ShowcaseDatabaseSQLiteNow",
                    frameworkModuleName = "ShowcaseDatabaseKmp",
                    bridgePackageName = "dev.test.showcase.swiftbridge",
                ),
            )
        )

        val bridge = outputDir.resolve("dev/test/showcase/swiftbridge/ShowcaseDatabaseBridge.kt").readText()
        assertTrue(bridge.contains("public class AppPersonAddReturningQuery internal constructor("))
        assertTrue(bridge.contains("public suspend fun list(): List<AppPersonAddReturningResult>"))
        assertTrue(bridge.contains("public suspend fun oneOrNull(): AppPersonAddReturningResult?"))
        assertTrue(bridge.contains("public fun addReturningPerson(params: AppPersonAddReturningParamsBridge): AppPersonAddReturningQuery ="))
        assertTrue(bridge.contains("listBlock = { database.person.addReturning.list(params.toGeneratedParams()) }"))
        assertTrue(bridge.contains("public val addresses: List<AppAddressRow>"))
        assertTrue(bridge.contains("addresses = addresses.map { it.toAppAddressRow() }"))
        assertTrue(bridge.contains("private fun PersonAddReturningResult.toAppPersonAddReturningResult(): AppPersonAddReturningResult ="))

        val swift = swiftOutputDir.resolve("ShowcaseDatabase.swift").readText()
        assertTrue(swift.contains("public final class SQLiteNowExecuteReturningQuery<Row: Sendable>"))
        assertTrue(swift.contains("public func addReturning(_ params: PersonAddReturningParams) -> SQLiteNowExecuteReturningQuery<PersonAddReturningResult>"))
        assertTrue(swift.contains("return rows.map(PersonAddReturningResult.init)"))
        assertTrue(swift.contains("public let addresses: [AddressRow]"))
        assertTrue(swift.contains("addresses = row.addresses.map(AddressRow.init)"))
        assertFalse(swift.contains("RETURNING execute statements are not part of the current Swift spike fixture"))
    }
}

private fun createTaskFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/AppDatabase")
    sqlDir.resolve("schema/task.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                CREATE TABLE task
                (
                    id         INTEGER PRIMARY KEY NOT NULL,
                    title      TEXT                NOT NULL,
                    -- @@{ field=is_done, propertyType=Boolean }
                    is_done    INTEGER             NOT NULL DEFAULT 0,
                    created_at TEXT                NOT NULL
                );
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/task/selectAll.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{ queryResult=TaskRow }
                SELECT id,
                       title,
                       is_done,
                       created_at
                FROM task
                ORDER BY created_at, id;
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/task/selectById.sql").writeText(
        """
            -- @@{ queryResult=TaskRow }
            SELECT id,
                   title,
                   is_done,
                   created_at
            FROM task
            WHERE id = :id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/task/selectByTitles.sql").writeText(
        """
            -- @@{ queryResult=TaskRow }
            SELECT id,
                   title,
                   is_done,
                   created_at
            FROM task
            WHERE title IN :titles
            ORDER BY created_at, id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/task/insert.sql").writeText(
        """
            INSERT INTO task (id,
                              title,
                              is_done,
                              created_at)
            VALUES (:id,
                    :title,
                    :isDone,
                    :createdAt);
        """.trimIndent()
    )
    sqlDir.resolve("queries/task/updateDone.sql").writeText(
        """
            UPDATE task
            SET is_done = :isDone
            WHERE id = :id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/task/delete.sql").writeText(
        """
            DELETE
            FROM task
            WHERE id = :id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/task/deleteAll.sql").writeText(
        """
            DELETE
            FROM task;
        """.trimIndent()
    )
    return sqlDir
}

private fun createSwiftOversqliteFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/SyncAppDatabase")
    sqlDir.resolve("schema/note.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{enableSync=true, syncKeyColumnName=id}
                CREATE TABLE note (
                  id TEXT PRIMARY KEY NOT NULL,
                  title TEXT NOT NULL,
                  body TEXT
                );
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/note/selectAll.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{ queryResult=NoteRow }
                SELECT id,
                       title,
                       body
                FROM note
                ORDER BY id;
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/note/upsert.sql").writeText(
        """
            INSERT INTO note (id,
                              title,
                              body)
            VALUES (:id,
                    :title,
                    :body)
            ON CONFLICT(id) DO UPDATE SET title = :title,
                                          body = :body;
        """.trimIndent()
    )
    sqlDir.resolve("queries/note/deleteById.sql").writeText(
        """
            DELETE FROM note
            WHERE id = :id;
        """.trimIndent()
    )
    return sqlDir
}

private fun createAdapterFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/AdapterDatabase")
    sqlDir.resolve("schema/doc.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                CREATE TABLE doc
                (
                    id               INTEGER PRIMARY KEY NOT NULL,
                    payload          BLOB                NOT NULL,
                    optional_payload BLOB,
                    optional_count   INTEGER,
                    optional_score   REAL,
                    -- @@{ field=optional_flag, propertyType=Boolean }
                    optional_flag    INTEGER,
                    -- @@{ field=status, adapter=custom, propertyType=dev.test.adapter.db.TaskStatus }
                    status           TEXT                NOT NULL,
                    -- @@{ field=tags, adapter=custom, propertyType=List<String> }
                    tags             TEXT                NOT NULL
                );
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/doc/selectAll.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{ queryResult=DocRow }
                SELECT id,
	                       payload,
	                       optional_payload,
	                       optional_count,
	                       optional_score,
	                       optional_flag,
	                       status,
	                       tags
                FROM doc
                ORDER BY id;
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/doc/selectByStatuses.sql").writeText(
        """
            -- @@{ queryResult=DocRow }
            SELECT id,
	                   payload,
	                   optional_payload,
	                   optional_count,
	                   optional_score,
	                   optional_flag,
	                   status,
	                   tags
            FROM doc
            WHERE status IN :statuses
            ORDER BY id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/doc/insert.sql").writeText(
        """
            INSERT INTO doc (id,
	                             payload,
	                             optional_payload,
	                             optional_count,
	                             optional_score,
	                             optional_flag,
	                             status,
	                             tags)
	            VALUES (:id,
	                    :payload,
	                    :optionalPayload,
	                    :optionalCount,
	                    :optionalScore,
	                    :optionalFlag,
	                    :status,
	                    :tags);
        """.trimIndent()
    )
    return sqlDir
}

private fun createShowcaseFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/ShowcaseDatabase")
    sqlDir.resolve("schema/person.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                CREATE TABLE person
                (
                    id         INTEGER PRIMARY KEY NOT NULL,
                    -- @@{ field=first_name, propertyName=myFirstName }
                    first_name TEXT                NOT NULL,
                    -- @@{ field=last_name, propertyName=myLastName }
                    last_name  TEXT                NOT NULL,
                    email      TEXT                NOT NULL UNIQUE,
                    phone      TEXT
                );

                CREATE TABLE address
                (
                    id        INTEGER PRIMARY KEY NOT NULL,
                    person_id INTEGER             NOT NULL,
                    street    TEXT                NOT NULL,
                    city      TEXT                NOT NULL,
                    FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
                );
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/address/selectAll.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{ queryResult=AddressRow }
                SELECT *
                FROM address
                ORDER BY id;
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/person/addReturning.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                INSERT INTO person(email,
                                   first_name,
                                   last_name,
                                   phone)
                VALUES (:email,
                        :firstName,
                        :lastName,
                        :phone)
                ON CONFLICT(email) DO UPDATE SET first_name = :firstName,
                                                 last_name  = :lastName,
                                                 phone      = :phone
                RETURNING *;
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/person/selectWithAddresses.sql").writeText(
        """
            /* @@{
                queryResult=PersonWithAddressesRow,
                collectionKey=person__id } */
            SELECT p.id AS person__id,
                   p.first_name AS person__first_name,
                   p.last_name AS person__last_name,
                   p.email AS person__email,
                   p.phone AS person__phone,
                   a.id AS address__id,
                   a.person_id AS address__person_id,
                   a.street AS address__street,
                   a.city AS address__city

            /* @@{ dynamicField=addresses,
                   mappingType=collection,
                   propertyType=List<AddressRow>,
                   sourceTable=a,
                   collectionKey=address__id,
                   aliasPrefix=address__,
                   notNull=true } */

            FROM person p
                     LEFT JOIN address a ON p.id = a.person_id
            ORDER BY p.id, a.id
            LIMIT :limit OFFSET :offset;
        """.trimIndent()
    )
    return sqlDir
}
