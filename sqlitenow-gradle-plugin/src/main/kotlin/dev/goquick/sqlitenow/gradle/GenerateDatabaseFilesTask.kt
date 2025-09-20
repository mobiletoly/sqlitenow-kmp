package dev.goquick.sqlitenow.gradle

import java.io.File
import java.io.FileNotFoundException
import java.sql.Connection
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.ProjectLayout
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

fun main() {
    val rootScrDir = "/Users/pochkin/Projects/my/sqlitenow-kmp/sample-kmp/composeApp/src/commonMain/sql"
    val schemaDatabaseDir = "/Users/pochkin/Projects/my/sqlitenow-kmp/sample-kmp/composeApp/tmp"

    generateDatabaseFiles(
        dbName = "NowSampleDatabase",
        sqlDir = File("$rootScrDir/NowSampleDatabase"),
        packageName = "dev.goquick.sqlitenow.samplekmp.db",
        outDir = File("/Users/pochkin/Projects/my/sqlitenow-kmp/sample-kmp/composeApp/build/generated/sqlitenow/code"),
        schemaDatabaseFile = File("$schemaDatabaseDir/schema.db"),
        debug = true,
    )
}

abstract class GenerateDatabaseFilesTask @Inject constructor(
    objects: ObjectFactory,
    layout: ProjectLayout,
) : DefaultTask() {

    /** Name of the DB, provided by the user */
    @get:Input
    abstract val dbName: Property<String>

    /** Where to write generated Kotlin code */
    @get:OutputDirectory
    val outputDir: DirectoryProperty =
        objects.directoryProperty().convention(layout.buildDirectory.dir("generated/sqlitenow/code"))

    @get:InputDirectory
    @get:Optional
    val sqlDir: DirectoryProperty =
        objects.directoryProperty().convention(layout.projectDirectory.dir("src/commonMain/sql"))

    @get:Input
    abstract val packageName: Property<String>

    @get:OutputFile
    @get:Optional
    val schemaDatabaseFile: RegularFileProperty = objects.fileProperty()

    @get:Input
    abstract val debug: Property<Boolean>

    @TaskAction
    fun generate() {
        // 1. Ensure the output directory exists
        val outDir: File = outputDir.asFile.get().apply { mkdirs() }

        val dbDir = sqlDir.dir(dbName.get())
        if (!dbDir.get().asFile.exists()) {
            throw FileNotFoundException("SQL database directory '${dbDir.get().asFile.path}' not found")
        }

        val dbFile = if (schemaDatabaseFile.isPresent) schemaDatabaseFile.asFile.get() else null

        val sqlDir = dbDir.get().asFile
        val packageName = packageName.get()

        with (project.logger) {
            generateDatabaseFiles(
                dbName = dbName.get(),
                sqlDir = sqlDir,
                packageName = packageName,
                outDir = outDir,
                schemaDatabaseFile = dbFile,
                debug = debug.get(),
            )
        }
    }
}

fun generateDatabaseFiles(
    dbName: String, sqlDir: File, packageName: String, outDir: File, schemaDatabaseFile: File?,
    debug: Boolean,
) {
    val schemaDir = sqlDir.resolve("schema")
    val initSqlDir = sqlDir.resolve("init")
    val migrationDir = sqlDir.resolve("migration")
    val queriesDirs = sqlDir.resolve("queries")

    val conn: Connection = if (schemaDatabaseFile != null) {
        schemaDatabaseFile.delete()
        TempDatabaseConnector(MigratorTempStorage.File(schemaDatabaseFile)).connection
    } else {
        TempDatabaseConnector(MigratorTempStorage.Memory).connection
    }

    try {
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)
        val sqlBatchInspector = SQLBatchInspector(sqlDirectory = initSqlDir, mandatory = false)
        val migrationInspector = MigrationInspector(sqlDirectory = migrationDir)

        if (outDir.path.contains("/generated/")) {      // just for safety reasons
            outDir.deleteRecursively()
        }

        val migratorCodeGenerator = MigratorCodeGenerator(
            schemaInspector = schemaInspector,
            sqlBatchInspector = sqlBatchInspector,
            migrationInspector = migrationInspector,
            packageName = packageName,
            outputDir = outDir,
            debug = debug
        )
        migratorCodeGenerator.generateCode()

        val dataStructCodeGenerator = DataStructCodeGenerator(
            conn = conn,
            queriesDir = queriesDirs,
            statementExecutors = schemaInspector.statementExecutors,
            packageName = packageName,
            outputDir = outDir
        )
        dataStructCodeGenerator.generateCode()
        val queryCodeGenerator = QueryCodeGenerator(
            dataStructCodeGenerator = dataStructCodeGenerator, packageName = packageName, outputDir = outDir, debug = debug
        )
        queryCodeGenerator.generateCode()

        val allStatements = sqlBatchInspector.sqlStatements
        conn.autoCommit = true
        allStatements.forEach {
            conn.createStatement().use { stmt ->
                try {
                    stmt.executeUpdate(it.sql)
                } catch (e: Throwable) {
                    StandardErrorHandler.handleSqlExecutionError(it.sql, e, "GenerateDatabaseFilesTask")
                }
            }
        }

        val dbCodeGen = DatabaseCodeGenerator(
            nsWithStatements = dataStructCodeGenerator.nsWithStatements,
            createTableStatements = dataStructCodeGenerator.createTableStatements,
            createViewStatements = dataStructCodeGenerator.createViewStatements,
            packageName = packageName,
            outputDir = outDir,
            databaseClassName = dbName,
            debug = debug
        )
        dbCodeGen.generateDatabaseClass()
    } finally {
        conn.close()
    }
}
