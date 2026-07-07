package dev.goquick.sqlitenow.gradle.compiler

import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftProductExportConfig
import dev.goquick.sqlitenow.gradle.swift.SwiftProductRuntimeMode
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqliteNowSwiftProductEmitterTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun compilerRefusesToCleanNonEmptyUnownedSwiftSourceOutputDirectory() {
        val sqlDir = createHybridCoreProofSql(tempDir)
        val swiftOutputDir = tempDir.resolve("AppSources")
        val userSource = swiftOutputDir.resolve("UserCode.swift")
        userSource.write(
            """
            import Foundation

            struct UserCode {}
            """.trimIndent()
        )

        val error = assertFailsWith<IllegalArgumentException> {
            compileSqliteNowDatabase(
                SqliteNowCompilerInput(
                    databaseName = "HybridCoreProofDatabase",
                    sqlDirectory = sqlDir,
                    packageName = "dev.test.hybrid.db",
                    outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                    swiftProductExport = SqliteNowSwiftProductExportConfig(
                        swiftOutputDirectory = swiftOutputDir,
                        swiftModuleName = "HybridCoreProofSQLiteNow",
                        runtimeModuleName = "SQLiteNowCoreRuntime",
                    ),
                )
            )
        }

        assertTrue(error.message.orEmpty().contains("Refusing to clean non-empty Swift product output"))
        assertTrue(userSource.isFile, "User Swift source must not be deleted.")
    }

    @Test
    fun compilerGeneratesCoreProductSwiftWithoutPublicSwiftBackendOrKotlinBridgeLeakage() {
        val sqlDir = createHybridCoreProofSql(tempDir)
        val outputDir = tempDir.resolve("generated-kotlin-unused")
        val swiftOutputDir = tempDir.resolve("generated-swift/HybridCoreProofSQLiteNow")

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "HybridCoreProofDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.hybrid.db",
                outputDirectory = outputDir,
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "HybridCoreProofSQLiteNow",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ),
            )
        )

        assertFalse(
            SqliteNowCompilerBackend.entries.any { it.name == "SWIFT" },
            "Product Swift export must not add a public Swift compiler backend enum in Phase 9C."
        )
        val generatedFileNames = result.generatedFiles.map { it.name }.sorted()
        assertEquals(
            listOf("HybridCoreProofDatabase.swift", "SQLiteNowSupport.swift", "TaskQueries.swift"),
            generatedFileNames,
        )
        assertFalse(outputDir.exists() && outputDir.walkTopDown().any { it.isFile && it.extension == "kt" })

        val swift = swiftOutputDir.readGeneratedSwiftFiles()
        assertTrue(swift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertTrue(swift.contains("public final class HybridCoreProofDatabase: @unchecked Sendable"))
        assertTrue(swift.contains("SQLiteNowCoreRuntimeMigrationPlan("))
        assertTrue(swift.contains("SQLiteNowCoreRuntimeDatabase("))
        assertTrue(swift.contains("latestVersion: 2"))
        assertTrue(swift.contains("SQLiteNowCoreRuntimeMigrationStep("))
        assertTrue(swift.contains("public struct TaskRow: Equatable, Sendable"))
        assertTrue(swift.contains("public struct TaskInsertReturningParams: Equatable, Sendable"))
        assertTrue(swift.contains("public func selectAll() -> SQLiteNowSelectQuery<TaskRow>"))
        assertTrue(swift.contains("public func selectById(_ params: TaskSelectByIdParams) -> SQLiteNowSelectQuery<TaskRow>"))
        assertTrue(swift.contains("public func insertReturning(_ params: TaskInsertReturningParams) -> SQLiteNowExecuteReturningQuery<TaskRow>"))
        assertTrue(swift.contains("public func updateDone(_ params: TaskUpdateDoneParams) async throws"))
        assertTrue(swift.contains("public func deleteByIds(_ params: TaskDeleteByIdsParams) async throws"))
        assertTrue(swift.contains("public final class HybridCoreProofDatabaseTransaction"))
        assertTrue(swift.contains("public let task: TaskTransactionQueries"))
        assertTrue(swift.contains("sqliteNowBindJsonArray(params.ids)"))
        assertTrue(swift.contains("runtime.observeTables(tableNames: Self.selectAllAffectedTables"))
        assertTrue(swift.contains("internal func sqliteNowRequireOne<Row>"))
        assertTrue(swift.contains("private static func selectAllLoadRows("))
        assertTrue(swift.contains("private static func selectByIdLoadRows("))
        assertTrue(swift.contains("private static func insertReturningLoadRows("))
        assertTrue(swift.contains("try await Self.selectAllLoadRows(runtime: runtime, adapters: adapters)"))
        assertTrue(swift.contains("try await Self.selectByIdLoadRows(params, runtime: runtime, adapters: adapters)"))
        assertTrue(swift.contains("try await Self.insertReturningLoadRows(params, runtime: runtime, adapters: adapters)"))
        assertTrue(swift.contains("return try sqliteNowRequireOne(rows, resultName: \"TaskRow\")"))
        assertTrue(swift.contains("if let runtimeError = error as? SQLiteNowCoreRuntimeException"))
        assertTrue(swift.contains("userInfo[\"K\" + \"otlinException\"] as? SQLiteNowCoreRuntimeException"))
        assertFalse(swift.contains("lowercased.contains"))
        assertFalse(swift.contains("self.selectAll().list()"))
        assertFalse(swift.contains("self.selectById(params).list()"))
        assertFalse(swift.contains("self.insertReturning(params).list()"))

        listOf(
            "KotlinByteArray",
            "KotlinLong",
            "KotlinDouble",
            "KotlinBoolean",
            "Coroutine",
            "StateFlow",
            "Flow<",
            "Ktor",
            "Kt.",
            "Bridge.kt",
        ).forEach { forbidden ->
            assertFalse(swift.contains(forbidden), "Product Swift source leaked bridge token '$forbidden'.")
        }
    }

    @Test
    fun compilerRejectsBlobCollectionParametersForSwiftProductSource() {
        val sqlDir = createHybridCoreProofSql(tempDir)
        sqlDir.resolve("queries/task/selectByPayloads.sql").write(
            """
            -- @@{ queryResult=TaskRow }
            SELECT id, title, is_done, created_at, payload, priority
            FROM task
            WHERE payload IN :payloads
            ORDER BY id;
            """.trimIndent()
        )

        val error = assertFailsWith<IllegalArgumentException> {
            compileSqliteNowDatabase(
                SqliteNowCompilerInput(
                    databaseName = "HybridCoreProofDatabase",
                    sqlDirectory = sqlDir,
                    packageName = "dev.test.hybrid.db",
                    outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                    swiftProductExport = SqliteNowSwiftProductExportConfig(
                        swiftOutputDirectory = tempDir.resolve("generated-swift/HybridCoreProofSQLiteNow"),
                        swiftModuleName = "HybridCoreProofSQLiteNow",
                        runtimeModuleName = "SQLiteNowCoreRuntime",
                    ),
                )
            )
        }

        assertTrue(
            error.message.orEmpty().contains(
                "Swift product collection parameter 'payloads' maps to Data/BLOB, which is not supported"
            ),
            "Blob collection parameters should fail before generated Swift silently stringifies bytes.",
        )
    }

    @Test
    fun compilerRejectsAdapterBackedBlobCollectionParametersForSwiftProductSource() {
        val sqlDir = createBlobAdapterProductSql(tempDir)
        val error = assertFailsWith<IllegalArgumentException> {
            compileSqliteNowDatabase(
                SqliteNowCompilerInput(
                    databaseName = "BlobAdapterDatabase",
                    sqlDirectory = sqlDir,
                    packageName = "dev.test.blobadapter.db",
                    outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                    swiftProductExport = SqliteNowSwiftProductExportConfig(
                        swiftOutputDirectory = tempDir.resolve("generated-swift/BlobAdapterDatabaseSQLiteNow"),
                        swiftModuleName = "BlobAdapterDatabaseSQLiteNow",
                        runtimeModuleName = "SQLiteNowCoreRuntime",
                    ),
                )
            )
        }

        assertTrue(
            error.message.orEmpty().contains(
                "Swift product collection parameter 'payloadTokens' maps to Data/BLOB, which is not supported"
            ),
            "Adapter-backed Blob collection parameters should fail before generated Swift stringifies bytes.",
        )
    }

    @Test
    fun compilerGeneratesSyncProductSwiftOverCombinedRuntimeWithoutPublicSwiftBackend() {
        val sqlDir = createHybridSyncProofSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/HybridSyncProofSQLiteNow")

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "HybridSyncProofDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.hybridsync.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "HybridSyncProofSQLiteNow",
                    runtimeMode = SwiftProductRuntimeMode.SYNC,
                ),
            )
        )

        assertFalse(
            SqliteNowCompilerBackend.entries.any { it.name == "SWIFT" },
            "Product Swift sync export must not add a public Swift compiler backend enum in Phase 9D."
        )
        val generatedFileNames = result.generatedFiles.map { it.name }.sorted()
        assertEquals(
            listOf(
                "DocsQueries.swift",
                "HybridSyncProofDatabase.swift",
                "SQLiteNowSupport.swift",
                "SQLiteNowSyncSupport.swift",
            ),
            generatedFileNames,
        )
        assertFalse(
            tempDir.resolve("generated-kotlin-unused")
                .walkTopDown()
                .any { it.isFile && it.extension == "kt" }
        )

        val swift = swiftOutputDir.readGeneratedSwiftFiles()
        val supportSwift = swiftOutputDir.resolve("SQLiteNowSupport.swift").readText()
        val syncSupportSwift = swiftOutputDir.resolve("SQLiteNowSyncSupport.swift").readText()
        assertTrue(swift.contains("@preconcurrency import SQLiteNowSyncRuntime"))
        assertFalse(swift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertTrue(swift.contains("public final class HybridSyncProofDatabase: @unchecked Sendable"))
        assertTrue(swift.contains("public func makeSyncClient("))
        assertTrue(swift.contains(") throws -> SQLiteNowSyncClient"))
        assertTrue(swift.contains("Call open() before makeSyncClient(...)."))
        assertTrue(swift.contains("SQLiteNowSyncRuntimeTableSpec(tableName: \"docs\", syncKeyColumnName: \"doc_id\")"))
        assertFalse(supportSwift.contains("public struct SQLiteNowSyncAuth: Sendable"))
        assertTrue(syncSupportSwift.contains("public struct SQLiteNowSyncAuth: Sendable"))
        assertTrue(syncSupportSwift.contains("refreshedAccessTokenProvider"))
        assertTrue(syncSupportSwift.contains("refreshedAccessToken: (@Sendable () -> String?)? = nil"))
        assertFalse(syncSupportSwift.contains("refreshTokenProvider"))
        assertFalse(syncSupportSwift.contains("refreshToken: (@Sendable () -> String?)? = nil"))
        assertTrue(syncSupportSwift.contains("public final class SQLiteNowSyncClient"))
        assertTrue(supportSwift.contains("catch let error as CancellationError"))
        assertTrue(syncSupportSwift.contains("public func progress() -> AsyncThrowingStream<SQLiteNowSyncProgress, Error>"))
        assertTrue(syncSupportSwift.contains("public func startAutomaticDownloads("))
        assertTrue(swift.contains("SQLiteNowSyncRuntimeClient("))
        assertTrue(syncSupportSwift.contains("runtime.close()\n            throw SQLiteNowError.misuse"))
        assertTrue(supportSwift.contains("if let runtimeError = error as? SQLiteNowCoreRuntimeException"))
        assertTrue(supportSwift.contains("userInfo[\"K\" + \"otlinException\"] as? SQLiteNowCoreRuntimeException"))
        assertTrue(supportSwift.contains("if let runtimeError = error as? SQLiteNowSyncRuntimeException"))
        assertTrue(supportSwift.contains("userInfo[\"K\" + \"otlinException\"] as? SQLiteNowSyncRuntimeException"))
        assertTrue(supportSwift.contains("internal static func from(_ payload: SQLiteNowSyncRuntimeErrorPayload)"))
        assertFalse(supportSwift.contains("lowercased.contains"))
        assertTrue(syncSupportSwift.contains("func resolveForRuntimeSmoke(_ conflict: SQLiteNowSyncConflict)"))
        assertFalse(swift.contains("resolveForProof"))
        assertTrue(swift.contains("public struct DocRow: Equatable, Sendable"))
        assertTrue(swift.contains("public struct DocsInsertOrReplaceParams: Equatable, Sendable"))
        assertTrue(swift.contains("public func selectAll() -> SQLiteNowSelectQuery<DocRow>"))
        assertTrue(swift.contains("public func insertOrReplace(_ params: DocsInsertOrReplaceParams) async throws"))

        listOf(
            "KotlinByteArray",
            "KotlinLong",
            "KotlinDouble",
            "KotlinBoolean",
            "Coroutine",
            "StateFlow",
            "Flow<",
            "Ktor",
            "Kt.",
            "Bridge.kt",
        ).forEach { forbidden ->
            assertFalse(swift.contains(forbidden), "Product Swift sync source leaked bridge token '$forbidden'.")
        }
    }

    @Test
    fun productSwiftSyncExportRejectsMissingEnableSyncTables() {
        val sqlDir = createHybridCoreProofSql(tempDir)
        val error = assertFailsWith<IllegalArgumentException> {
            compileSqliteNowDatabase(
                SqliteNowCompilerInput(
                    databaseName = "HybridCoreProofDatabase",
                    sqlDirectory = sqlDir,
                    packageName = "dev.test.hybrid.db",
                    outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                    swiftProductExport = SqliteNowSwiftProductExportConfig(
                        swiftOutputDirectory = tempDir.resolve("generated-swift/HybridCoreProofSQLiteNow"),
                        swiftModuleName = "HybridCoreProofSQLiteNow",
                        runtimeModuleName = "SQLiteNowSyncRuntime",
                        runtimeMode = SwiftProductRuntimeMode.SYNC,
                    ),
                )
            )
        }

        assertTrue(error.message.orEmpty().contains("runtime=sync"))
        assertTrue(error.message.orEmpty().contains("at least one table annotated with enableSync=true"))
    }

    @Test
    fun productSwiftCoreExportRejectsSyncEnabledTablesWithoutOversqlite() {
        val sqlDir = createHybridSyncProofSql(tempDir)
        val error = assertFailsWith<IllegalArgumentException> {
            compileSqliteNowDatabase(
                SqliteNowCompilerInput(
                    databaseName = "HybridSyncProofDatabase",
                    sqlDirectory = sqlDir,
                    packageName = "dev.test.hybridsync.db",
                    outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                    swiftProductExport = SqliteNowSwiftProductExportConfig(
                        swiftOutputDirectory = tempDir.resolve("generated-swift/HybridSyncProofSQLiteNow"),
                        swiftModuleName = "HybridSyncProofSQLiteNow",
                    ),
                )
            )
        }

        assertTrue(error.message.orEmpty().contains("core source export"))
        assertTrue(error.message.orEmpty().contains("sync-enabled tables"))
    }

    @Test
    fun productSwiftSyncExportUsesResolvedSyncTables() {
        val sqlDir = createMultiSyncTableSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/MultiSyncDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "MultiSyncDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.multisync.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "MultiSyncDatabaseSQLiteNow",
                    runtimeModuleName = "SQLiteNowSyncRuntime",
                    runtimeMode = SwiftProductRuntimeMode.SYNC,
                ),
            )
        )

        val swift = swiftOutputDir.resolve("MultiSyncDatabase.swift").readText()
        assertTrue(swift.contains("SQLiteNowSyncRuntimeTableSpec(tableName: \"docs\", syncKeyColumnName: \"doc_id\")"))
        assertTrue(swift.contains("SQLiteNowSyncRuntimeTableSpec(tableName: \"comments\", syncKeyColumnName: \"comment_id\")"))
    }

    @Test
    fun productSwiftExportEmitsSqlAsRawSwiftStrings() {
        val sqlDir = createHybridCoreProofSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/HybridCoreProofSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "HybridCoreProofDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.hybrid.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "HybridCoreProofSQLiteNow",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ),
            )
        )

        val querySwift = swiftOutputDir.resolve("TaskQueries.swift").readText()
        val databaseSwift = swiftOutputDir.resolve("HybridCoreProofDatabase.swift").readText()
        assertTrue(querySwift.contains("internal static let selectEscapedTitleSql ="))
        assertTrue(querySwift.contains("#\"\"\""))
        assertFalse(querySwift.contains("internal static let selectEscapedTitleSql = \"\"\""))
        assertTrue(querySwift.contains("WHERE title LIKE '\\_%' ESCAPE '\\'"))
        assertTrue(querySwift.contains("OR title = '\\(literal)'"))
        assertTrue(databaseSwift.contains("SET title = '\\(migration literal)'"))
    }

    @Test
    fun productSwiftExportClearsStaleSwiftFilesBeforeRegeneration() {
        val sqlDir = createHybridCoreProofSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/HybridCoreProofSQLiteNow")
        swiftOutputDir.resolve("StaleApi.swift").write("public struct StaleApi {}")
        swiftOutputDir.resolve("Nested/StaleNestedApi.swift").write("public struct StaleNestedApi {}")

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "HybridCoreProofDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.hybrid.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "HybridCoreProofSQLiteNow",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ),
            )
        )

        val generatedFileNames = result.generatedFiles.map { it.name }.sorted()
        assertEquals(
            listOf("HybridCoreProofDatabase.swift", "SQLiteNowSupport.swift", "TaskQueries.swift"),
            generatedFileNames,
        )
        assertFalse(swiftOutputDir.resolve("StaleApi.swift").exists())
        assertFalse(swiftOutputDir.resolve("Nested/StaleNestedApi.swift").exists())
        assertFalse(swiftOutputDir.resolve("Nested").exists())
        assertEquals(
            generatedFileNames,
            swiftOutputDir
                .walkTopDown()
                .filter { it.isFile && it.extension == "swift" }
                .map { it.name }
                .sorted()
                .toList()
        )
    }

    @Test
    fun productSwiftExportUsesTablePropertyNameOverridesForReturningResults() {
        val sqlDir = createReturningOverrideSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/ReturningOverrideDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "ReturningOverrideDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.returning.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "ReturningOverrideDatabaseSQLiteNow",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ),
            )
        )

        val swift = swiftOutputDir.readGeneratedSwiftFiles()
        assertTrue(swift.contains("public struct PersonRow: Equatable, Sendable"))
        assertTrue(swift.contains("public let givenName: String"))
        assertTrue(swift.indexOf("public let givenName: String") < swift.indexOf("public let id: Int64"))
        assertTrue(swift.contains("PersonRow("))
        assertTrue(swift.contains("givenName: try sqliteNowReadString(row.cellAt(index: 0), column: \"first_name\")"))
        assertTrue(swift.contains("id: try sqliteNowReadInt64(row.cellAt(index: 1), column: \"id\")"))
        assertFalse(swift.contains("public let firstName: String"))
    }

    @Test
    fun productSwiftExportKeepsAdapterBackedModelsSendableAndAdaptsCollectionValues() {
        val sqlDir = createAdapterProductSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/AdapterDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "AdapterDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.adapter.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "AdapterDatabaseSQLiteNow",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ),
            )
        )

        val swift = swiftOutputDir.readGeneratedSwiftFiles()
        assertTrue(swift.contains("public let statusToSqlValue: @Sendable (TaskStatus) throws -> String"))
        assertTrue(swift.contains("public let sqlValueToStatus: @Sendable (String) throws -> TaskStatus"))
        assertTrue(swift.contains("public struct DocRow: @unchecked Sendable"))
        assertTrue(swift.contains("public struct DocSelectByStatusesParams: @unchecked Sendable"))
        assertTrue(swift.contains("public let statuses: [TaskStatus]"))
        assertTrue(swift.contains("sqliteNowBindJsonArray(try params.statuses.map { try adapters.statusToSqlValue($0) })"))
        assertTrue(swift.contains("sqliteNowBind(try adapters.statusToSqlValue(params.status))"))
        assertTrue(swift.contains("try adapters.sqlValueToStatus(try sqliteNowReadString("))
    }

    @Test
    fun productSwiftExportSupportsSampleDynamicFieldMappings() {
        val sqlDir = createDynamicMappingProductSql(tempDir)
        val swiftOutputDir = tempDir.resolve("generated-swift/DynamicMappingDatabaseSQLiteNow")

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "DynamicMappingDatabase",
                sqlDirectory = sqlDir,
                packageName = "dev.test.dynamic.db",
                outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                swiftProductExport = SqliteNowSwiftProductExportConfig(
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "DynamicMappingDatabaseSQLiteNow",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ),
            )
        )

        val swift = swiftOutputDir.readGeneratedSwiftFiles()
        assertTrue(swift.contains("public struct PersonAddressRow: Equatable, Sendable"))
        assertTrue(swift.contains("public let isPrimary: Bool"))
        assertTrue(swift.contains("public let addresses: [PersonAddressRow]"))
        assertTrue(swift.contains("public let comments: [CommentRow]"))
        assertTrue(swift.contains("public let address: PersonAddressRow?"))
        assertTrue(swift.contains("public let comment: CommentRow?"))
        assertFalse(swift.contains("public let addressId:"))
        assertFalse(swift.contains("public let commentId:"))
        assertTrue(swift.contains("var groupedRows = [String: [SQLiteNowCoreRuntimeRow]]()"))
        assertTrue(swift.contains("private static func selectAllWithAddressesMapAddresses"))
        assertTrue(swift.contains("private static func selectAllWithAddressesMapComments"))
        assertTrue(swift.contains("private static func selectLimitedWithAddressesMapAddress"))
        assertTrue(swift.contains("private static func selectLimitedWithAddressesMapComment"))
        assertTrue(swift.contains("sqliteNowCellKey(row.cellAt(index:"))
        assertTrue(swift.contains("PersonAddressRow("))
        assertTrue(swift.contains("isPrimary: try sqliteNowReadBool("))
    }

    @Test
    fun productSwiftExportRejectsNestedDynamicResultTargets() {
        val sqlDir = createNestedDynamicMappingProductSql(tempDir)
        val error = assertFailsWith<IllegalArgumentException> {
            compileSqliteNowDatabase(
                SqliteNowCompilerInput(
                    databaseName = "NestedDynamicMappingDatabase",
                    sqlDirectory = sqlDir,
                    packageName = "dev.test.dynamicnested.db",
                    outputDirectory = tempDir.resolve("generated-kotlin-unused"),
                    swiftProductExport = SqliteNowSwiftProductExportConfig(
                        swiftOutputDirectory = tempDir.resolve("generated-swift/NestedDynamicMappingDatabaseSQLiteNow"),
                        swiftModuleName = "NestedDynamicMappingDatabaseSQLiteNow",
                        runtimeModuleName = "SQLiteNowCoreRuntime",
                    ),
                )
            )
        }

        val message = error.message.orEmpty()
        assertTrue(message.contains("PersonWithAddressRow.address"))
        assertTrue(message.contains("PersonAddressWithCommentRow"))
        assertTrue(message.contains("nested dynamic result targets are not supported"))
    }

    private fun createReturningOverrideSql(root: File): File {
        val sqlDir = root.resolve("sql/ReturningOverrideDatabase")
        sqlDir.resolve("schema/person.sql").write(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY NOT NULL,
                -- @@{ field=first_name, propertyName=givenName }
                first_name TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/person/insertReturning.sql").write(
            """
            -- @@{ queryResult=PersonRow }
            INSERT INTO person (id, first_name)
            VALUES (:id, :givenName)
            RETURNING first_name, id;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createDynamicMappingProductSql(root: File): File {
        val sqlDir = root.resolve("sql/DynamicMappingDatabase")
        sqlDir.resolve("schema/person.sql").write(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY NOT NULL,
                first_name TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("schema/person_address.sql").write(
            """
            CREATE TABLE person_address (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,
                street TEXT NOT NULL,
                -- @@{ field=is_primary, propertyType=Boolean }
                is_primary INTEGER NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("schema/comment.sql").write(
            """
            CREATE TABLE comment (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,
                body TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/personAddress/selectAll.sql").write(
            """
            -- @@{ queryResult=PersonAddressRow }
            SELECT id, person_id, street, is_primary
            FROM person_address
            ORDER BY id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/comment/selectAll.sql").write(
            """
            -- @@{ queryResult=CommentRow }
            SELECT id, person_id, body
            FROM comment
            ORDER BY id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/person/selectAllWithAddresses.sql").write(
            """
            /* @@{
                queryResult=PersonWithAddressRow,
                collectionKey=person__id } */
            SELECT p.id AS person__id,
                   p.first_name AS person__first_name,

                   a.id AS address__id,
                   a.person_id AS address__person_id,
                   a.street AS address__street,
                   a.is_primary AS address__is_primary,

                   c.id AS comment__id,
                   c.person_id AS comment__person_id,
                   c.body AS comment__body

            /* @@{ dynamicField=addresses,
                   mappingType=collection,
                   propertyType=List<PersonAddressRow>,
                   sourceTable=a,
                   collectionKey=address__id,
                   aliasPrefix=address__,
                   notNull=true } */

            /* @@{ dynamicField=comments,
                   mappingType=collection,
                   propertyType=List<CommentRow>,
                   sourceTable=c,
                   collectionKey=comment__id,
                   aliasPrefix=comment__,
                   notNull=true } */

            FROM person p
                     LEFT JOIN person_address a ON p.id = a.person_id
                     LEFT JOIN comment c ON p.id = c.person_id
            ORDER BY p.id, a.id, c.id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/person/selectLimitedWithAddresses.sql").write(
            """
            -- @@{ queryResult=PersonWithLimitedAddressRow }
            SELECT p.id AS person_id,
                   p.first_name,

                   a.id AS address__id,
                   a.person_id AS address__person_id,
                   a.street AS address__street,
                   a.is_primary AS address__is_primary,

                   c.id AS comment__id,
                   c.person_id AS comment__person_id,
                   c.body AS comment__body

            /* @@{ dynamicField=address,
                   mappingType=perRow,
                   propertyType=PersonAddressRow,
                   sourceTable=a,
                   aliasPrefix=address__ } */

            /* @@{ dynamicField=comment,
                   mappingType=perRow,
                   propertyType=CommentRow,
                   sourceTable=c,
                   aliasPrefix=comment__ } */

            FROM person p
                     LEFT JOIN person_address a ON p.id = a.person_id
                     LEFT JOIN comment c ON p.id = c.person_id
            ORDER BY p.id, a.id, c.id
            LIMIT 100;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createNestedDynamicMappingProductSql(root: File): File {
        val sqlDir = root.resolve("sql/NestedDynamicMappingDatabase")
        sqlDir.resolve("schema/person.sql").write(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY NOT NULL,
                first_name TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("schema/person_address.sql").write(
            """
            CREATE TABLE person_address (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,
                street TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("schema/comment.sql").write(
            """
            CREATE TABLE comment (
                id INTEGER PRIMARY KEY NOT NULL,
                person_id INTEGER NOT NULL,
                body TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/comment/selectAll.sql").write(
            """
            -- @@{ queryResult=CommentRow }
            SELECT id, person_id, body
            FROM comment
            ORDER BY id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/personAddress/selectAllWithComment.sql").write(
            """
            -- @@{ queryResult=PersonAddressWithCommentRow }
            SELECT a.id,
                   a.person_id,
                   a.street,

                   c.id AS comment__id,
                   c.person_id AS comment__person_id,
                   c.body AS comment__body

            /* @@{ dynamicField=comment,
                   mappingType=perRow,
                   propertyType=CommentRow,
                   sourceTable=c,
                   aliasPrefix=comment__ } */

            FROM person_address a
                     LEFT JOIN comment c ON a.person_id = c.person_id
            ORDER BY a.id, c.id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/person/selectAllWithAddress.sql").write(
            """
            -- @@{ queryResult=PersonWithAddressRow }
            SELECT p.id AS person_id,
                   p.first_name,

                   a.id AS address__id,
                   a.person_id AS address__person_id,
                   a.street AS address__street,

                   c.id AS address__comment__id,
                   c.person_id AS address__comment__person_id,
                   c.body AS address__comment__body

            /* @@{ dynamicField=address,
                   mappingType=perRow,
                   propertyType=PersonAddressWithCommentRow,
                   sourceTable=a,
                   aliasPrefix=address__ } */

            FROM person p
                     LEFT JOIN person_address a ON p.id = a.person_id
                     LEFT JOIN comment c ON p.id = c.person_id
            ORDER BY p.id, a.id, c.id;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createHybridSyncProofSql(root: File): File {
        val sqlDir = root.resolve("sql/HybridSyncProofDatabase")
        sqlDir.resolve("schema/docs.sql").write(
            """
            -- @@{ enableSync=true }
            CREATE TABLE docs (
                doc_id TEXT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/selectAll.sql").write(
            """
            -- @@{ queryResult=DocRow }
            SELECT doc_id, title
            FROM docs
            ORDER BY doc_id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/insertOrReplace.sql").write(
            """
            INSERT INTO docs (doc_id, title)
            VALUES (:docId, :title)
            ON CONFLICT(doc_id) DO UPDATE SET title = excluded.title;
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/updateTitle.sql").write(
            """
            UPDATE docs
            SET title = :title
            WHERE doc_id = :docId;
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/deleteById.sql").write(
            """
            DELETE FROM docs
            WHERE doc_id = :docId;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createMultiSyncTableSql(root: File): File {
        val sqlDir = root.resolve("sql/MultiSyncDatabase")
        sqlDir.resolve("schema/sync_tables.sql").write(
            """
            -- @@{ enableSync=true, syncKeyColumnName=doc_id }
            CREATE TABLE docs (
                doc_id TEXT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL
            );

            -- @@{ enableSync=true, syncKeyColumnName=comment_id }
            CREATE TABLE comments (
                comment_id TEXT PRIMARY KEY NOT NULL,
                doc_id TEXT NOT NULL,
                body TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/selectAll.sql").write(
            """
            -- @@{ queryResult=DocRow }
            SELECT doc_id, title
            FROM docs
            ORDER BY doc_id;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createHybridCoreProofSql(root: File): File {
        val sqlDir = root.resolve("sql/HybridCoreProofDatabase")
        sqlDir.resolve("schema/task.sql").write(
            """
            CREATE TABLE task (
                id INTEGER PRIMARY KEY NOT NULL,
                title TEXT NOT NULL,
                -- @@{ field=is_done, propertyType=Boolean }
                is_done INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                payload BLOB,
                priority INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX idx_task_created_at ON task (created_at, id);
            """.trimIndent()
        )
        sqlDir.resolve("init/seed.sql").write(
            """
            INSERT INTO task (id, title, is_done, created_at, payload, priority)
            VALUES (1, 'Seed task', 0, '2026-07-01T00:00:00Z', NULL, 0);
            """.trimIndent()
        )
        sqlDir.resolve("migration/0002_add_priority.sql").write(
            """
            ALTER TABLE task ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
            UPDATE task
            SET title = '\(migration literal)'
            WHERE 0;
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/selectAll.sql").write(
            """
            -- @@{ queryResult=TaskRow }
            SELECT id, title, is_done, created_at, payload, priority
            FROM task
            ORDER BY created_at, id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/selectById.sql").write(
            """
            -- @@{ queryResult=TaskRow }
            SELECT id, title, is_done, created_at, payload, priority
            FROM task
            WHERE id = :id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/selectEscapedTitle.sql").write(
            """
            -- @@{ queryResult=TaskRow }
            SELECT id, title, is_done, created_at, payload, priority
            FROM task
            WHERE title LIKE '\_%' ESCAPE '\'
               OR title = '\(literal)';
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/insert.sql").write(
            """
            INSERT INTO task (id, title, is_done, created_at, payload, priority)
            VALUES (:id, :title, :isDone, :createdAt, :payload, :priority);
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/insertReturning.sql").write(
            """
            -- @@{ queryResult=TaskRow }
            INSERT INTO task (id, title, is_done, created_at, payload, priority)
            VALUES (:id, :title, :isDone, :createdAt, :payload, :priority)
            RETURNING id, title, is_done, created_at, payload, priority;
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/updateDone.sql").write(
            """
            UPDATE task
            SET is_done = :isDone
            WHERE id = :id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/deleteByIds.sql").write(
            """
            DELETE FROM task
            WHERE id IN :ids;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createAdapterProductSql(root: File): File {
        val sqlDir = root.resolve("sql/AdapterDatabase")
        sqlDir.resolve("schema/doc.sql").write(
            """
            CREATE TABLE doc (
                id INTEGER PRIMARY KEY NOT NULL,
                -- @@{ field=status, adapter=custom, propertyType=dev.test.adapter.db.TaskStatus }
                status TEXT NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/doc/selectAll.sql").write(
            """
            -- @@{ queryResult=DocRow }
            SELECT id, status
            FROM doc
            ORDER BY id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/doc/selectByStatuses.sql").write(
            """
            -- @@{ queryResult=DocRow }
            SELECT id, status
            FROM doc
            WHERE status IN :statuses
            ORDER BY id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/doc/insert.sql").write(
            """
            INSERT INTO doc (id, status)
            VALUES (:id, :status);
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createBlobAdapterProductSql(root: File): File {
        val sqlDir = root.resolve("sql/BlobAdapterDatabase")
        sqlDir.resolve("schema/doc.sql").write(
            """
            CREATE TABLE doc (
                id INTEGER PRIMARY KEY NOT NULL,
                -- @@{ field=payload_token, adapter=custom, propertyType=dev.test.blobadapter.db.BlobToken }
                payload_token BLOB NOT NULL
            );
            """.trimIndent()
        )
        sqlDir.resolve("queries/doc/selectByPayloadTokens.sql").write(
            """
            -- @@{ queryResult=DocRow }
            SELECT id, payload_token
            FROM doc
            WHERE payload_token IN :payloadTokens
            ORDER BY id;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun File.write(value: String) {
        parentFile.mkdirs()
        writeText(value)
    }

    private fun File.readGeneratedSwiftFiles(): String =
        walkTopDown()
            .filter { it.isFile && it.extension == "swift" }
            .sortedBy { it.relativeTo(this).invariantSeparatorsPath }
            .joinToString(separator = "\n\n") { it.readText() }
}
