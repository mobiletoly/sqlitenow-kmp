package dev.goquick.sqlitenow.gradle.database

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import dev.goquick.sqlitenow.gradle.sqlite.translateSqliteStatementToKotlin
import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging
import java.io.File

/**
 * Generates Kotlin code for executing SQL migration statements.
 * The generated code will include a function that accepts an androidx.sqlite.SQLiteConnection
 * parameter and executes all the SQL statements from the migrations.
 */
internal class MigratorCodeGenerator(
    private val schemaInspector: SchemaInspector,
    private val sqlBatchInspector: SQLBatchInspector,
    private val migrationInspector: MigrationInspector,
    private val packageName: String,
    private val outputDir: File,
    private val debug: Boolean = false,
) {
    // Logger for this class
    private val logger: Logger = Logging.getLogger(MigratorCodeGenerator::class.java)

    // Sync triggers deprecated: generation removed in favor of runtime oversqlite package

    /**
     * Generates the applyMigration function that implements the DatabaseMigrations interface.
     * This function applies migrations based on the current database version.
     */
    private fun generateApplyMigrationFunction(): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.Companion.builder("applyMigration")
            .addModifiers(KModifier.OVERRIDE, KModifier.SUSPEND)
            .addAnnotation(
                AnnotationSpec.Companion.builder(Suppress::class)
                .addMember("%S", "SameReturnValue")
                .build())
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))
            .addParameter("currentVersion", Int::class)
            .returns(Int::class)

        val codeBlockBuilder = CodeBlock.Companion.builder()

        // Wrap entire migration logic with a single withContext
        if (debug) {
            codeBlockBuilder.add("return conn.withContextAndTrace {\n")
        } else {
            codeBlockBuilder.add("return withContext(conn.dispatcher) {\n")
        }

        // Handle initial setup when currentVersion is -1
        codeBlockBuilder.add("    if (currentVersion != ${migrationInspector.latestVersion}) {\n")
        codeBlockBuilder.add("        conn.execSQL(\"PRAGMA user_version = ${migrationInspector.latestVersion};\")\n")
        codeBlockBuilder.add("    }\n")
        codeBlockBuilder.add("\n")
        codeBlockBuilder.add("    if (currentVersion == -1) {\n")
        codeBlockBuilder.add("        executeAllSql(conn)\n")
        // Only add executeInitSql call if there are init statements
        if (sqlBatchInspector.sqlStatements.isNotEmpty()) {
            codeBlockBuilder.add("        executeInitSql(conn)\n")
        }
        if (debug) {
            codeBlockBuilder.add("        return@withContextAndTrace ${migrationInspector.latestVersion}\n")
        } else {
            codeBlockBuilder.add("        return@withContext ${migrationInspector.latestVersion}\n")
        }
        codeBlockBuilder.add("    }\n")
        codeBlockBuilder.add("\n")

        // Generate incremental migration calls for each version
        migrationInspector.sqlStatements.keys.sorted().forEach { version ->
            codeBlockBuilder.add("    if (currentVersion < $version) {\n")
            codeBlockBuilder.add("        migrateToVersion$version(conn)\n")
            codeBlockBuilder.add("    }\n")
            codeBlockBuilder.add("\n")
        }

        // Return the latest version
        codeBlockBuilder.add("    ${migrationInspector.latestVersion}\n")
        codeBlockBuilder.add("}\n")

        return functionBuilder
            .addCode(codeBlockBuilder.build())
            .build()
    }

    /** Generates a function that executes all SQL statements from a specific version. */
    private fun generateMigrateToVersionFunction(version: Int, statements: List<SqlSingleStatement>): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.Companion.builder("migrateToVersion$version")
            .addModifiers(KModifier.PRIVATE)
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))

        // Add the function body
        val codeBlockBuilder = CodeBlock.Companion.builder()
            .addStatement("// Execute SQL statements for version $version")

        // Add each SQL statement
        addSqlStatementsToCodeBlock(codeBlockBuilder, statements)

        // Sync triggers deprecated: no trigger generation (handled by client library if needed)

        // Add the code block to the function
        functionBuilder.addCode(codeBlockBuilder.build())

        return functionBuilder.build()
    }

    /** Helper function to add SQL statement execution code to a CodeBlock. */
    private fun addSqlStatementsToCodeBlock(codeBlockBuilder: CodeBlock.Builder, statements: List<SqlSingleStatement>) {
        statements.forEach { statement ->
            // Convert the SQL statement to a Kotlin string with proper formatting
            val formattedSql = translateSqliteStatementToKotlin(statement.sql)

            // Add the statement to execute the SQL (no individual withLock needed)
            // Handle dollar signs in SQL by using the $$ syntax in the generated code
            if (formattedSql.contains("$")) {
                // Use $$ syntax for SQL containing dollar signs
                codeBlockBuilder.addStatement("conn.execSQL($$\"\"\"$formattedSql\"\"\".trimMargin())")
            } else {
                // Regular case without dollar signs
                codeBlockBuilder.addStatement("conn.execSQL(\"\"\"$formattedSql\"\"\".trimMargin())")
            }
        }
    }

    /** Generates a function that executes all SQL statements from the schema and batch inspector. */
    private fun generateExecuteSqlFunction(): FunSpec {
        // Create the function builder
        val functionBuilder = FunSpec.Companion.builder("executeAllSql")
            .addModifiers(KModifier.PRIVATE)
            .addModifiers(KModifier.SUSPEND)
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))

        // Add the function body
        val codeBlockBuilder = CodeBlock.Companion.builder()
            .addStatement("// Execute schema statements first (CREATE TABLE, CREATE VIEW)")

        // Execute schema statements first
        addSqlStatementsToCodeBlock(codeBlockBuilder, schemaInspector.sqlStatements)

        // Sync triggers deprecated: no trigger generation (handled by client library if needed)

        // Add the code block to the function
        functionBuilder.addCode(codeBlockBuilder.build())

        return functionBuilder.build()
    }

    /** Generates a function that executes init SQL statements (only for initial database creation). */
    private fun generateExecuteInitSqlFunction(): FunSpec? {
        // Only generate this function if there are init statements
        if (sqlBatchInspector.sqlStatements.isEmpty()) {
            return null
        }

        // Create the function builder
        val functionBuilder = FunSpec.Companion.builder("executeInitSql")
            .addModifiers(KModifier.PRIVATE)
            .addModifiers(KModifier.SUSPEND)
            .addParameter("conn", ClassName("dev.goquick.sqlitenow.core", "SafeSQLiteConnection"))

        // Add the function body
        val codeBlockBuilder = CodeBlock.Companion.builder()
            .addStatement("// Execute init statements (INSERT, UPDATE, etc.) - only for initial database creation")

        // Add init statements
        addSqlStatementsToCodeBlock(codeBlockBuilder, sqlBatchInspector.sqlStatements)

        // Add the code block to the function
        functionBuilder.addCode(codeBlockBuilder.build())

        return functionBuilder.build()
    }

    // Sync triggers deprecated: no-op (kept for compatibility)
    private fun addSyncTriggersToCodeBlock(codeBlockBuilder: CodeBlock.Builder) { /* no-op */ }

    /** Checks if any tables have enableSync=true annotation */
    fun hasSyncEnabledTables(): Boolean = false

    /** Adds sync triggers for CREATE TABLE statements with enableSync=true in migration files */
    private fun addSyncTriggersForMigrationToCodeBlock(codeBlockBuilder: CodeBlock.Builder, statements: List<SqlSingleStatement>) { /* no-op */ }

    /** Generates the hasSyncEnabledTables function that checks if sync features are needed. */
    private fun generateHasSyncEnabledTablesFunction(): FunSpec {
        val hasSyncTables = hasSyncEnabledTables()

        return FunSpec.Companion.builder("hasSyncEnabledTables")
            .addModifiers(KModifier.OVERRIDE)
            .returns(Boolean::class)
            .addStatement("return %L", hasSyncTables)
            .build()
    }

    fun generateCode(className: String = "VersionBasedDatabaseMigrations"): FileSpec {
        // Create the output directory if it doesn't exist
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw RuntimeException("Failed to create output directory: ${outputDir.absolutePath}")
        }

        // Create a class to hold the migration functions
        val classBuilder = TypeSpec.Companion.classBuilder(className)
            // Make the class public so it can be accessed from other modules
            .addSuperinterface(ClassName("dev.goquick.sqlitenow.core", "DatabaseMigrations"))
            // Add an empty constructor
            .addFunction(
                FunSpec.Companion.constructorBuilder().build()
            )

        // Add the applyMigration function that implements the DatabaseMigrations interface
        val applyMigrationFunction = generateApplyMigrationFunction()
        classBuilder.addFunction(applyMigrationFunction)

        // Add the hasSyncEnabledTables function
        val hasSyncEnabledTablesFunction = generateHasSyncEnabledTablesFunction()
        classBuilder.addFunction(hasSyncEnabledTablesFunction)

        // Add individual migration functions for each version
        migrationInspector.sqlStatements.forEach { (version, statements) ->
            val versionFunction = generateMigrateToVersionFunction(version, statements)
            classBuilder.addFunction(versionFunction)
        }

        // Add the function to execute all SQL statements
        val executeSqlFunction = generateExecuteSqlFunction()
        classBuilder.addFunction(executeSqlFunction)

        // Add the function to execute init SQL statements (only if there are init statements)
        val executeInitSqlFunction = generateExecuteInitSqlFunction()
        if (executeInitSqlFunction != null) {
            classBuilder.addFunction(executeInitSqlFunction)
        }

        // Build the file spec
        val fileSpecBuilder = FileSpec.Companion.builder(packageName, className)
            .addType(classBuilder.build())
            .addAnnotation(
                AnnotationSpec.Companion.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )
            .addImport("dev.goquick.sqlitenow.core", "SafeSQLiteConnection")
            .addImport("androidx.sqlite", "execSQL")
            .addImport("dev.goquick.sqlitenow.core", "DatabaseMigrations")

        if (debug) {
            // No additional import needed for conn.withContextAndTrace
        } else {
            fileSpecBuilder.addImport("kotlinx.coroutines", "withContext")
        }

        val fileSpec = fileSpecBuilder.build()

        // Write the file
        fileSpec.writeTo(outputDir)

        return fileSpec
    }
}