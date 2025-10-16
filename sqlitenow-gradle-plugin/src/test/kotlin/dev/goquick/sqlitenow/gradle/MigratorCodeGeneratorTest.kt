package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.database.MigratorCodeGenerator
import dev.goquick.sqlitenow.gradle.database.MigrationInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import java.sql.Connection
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertTrue

class MigratorCodeGeneratorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var initFile: File
    private lateinit var outputDir: File
    private lateinit var conn: Connection

    @BeforeEach
    fun setup() {
        initFile = File(tempDir.toFile(), "init.sql")
        outputDir = File(tempDir.toFile(), "output")
        outputDir.mkdir()
        conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
    }

    @Test
    @DisplayName("Test generating migration code")
    fun testGenerateMigrationCode() {
        // Create a sample SQL file with a CREATE TABLE statement
        val createTableSql = """
            -- This is a test table
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );

            -- This is another test table
            CREATE TABLE another_table (
                id INTEGER PRIMARY KEY NOT NULL,
                test_table_id INTEGER NOT NULL,
                value TEXT NOT NULL,
                FOREIGN KEY (test_table_id) REFERENCES test_table(id) ON DELETE CASCADE
            );
        """.trimIndent()

        initFile.writeText(createTableSql)

        // Create separate directories for each inspector
        val schemaDir = File(tempDir.toFile(), "schema")
        val batchDir = File(tempDir.toFile(), "batch")
        val migrationDir = File(tempDir.toFile(), "migrations")
        schemaDir.mkdir()
        batchDir.mkdir()
        migrationDir.mkdir()

        // Copy the init.sql to schema and batch directories
        File(schemaDir, "init.sql").writeText(createTableSql)
        File(batchDir, "init.sql").writeText(createTableSql)
        File(migrationDir, "0001.sql").writeText("CREATE TABLE migration_test (id INTEGER);")

        // Create inspectors with separate directories
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)
        val sqlBatchInspector = SQLBatchInspector(sqlDirectory = batchDir, mandatory = false)
        val migrationInspector = MigrationInspector(sqlDirectory = migrationDir)

        // Create a MigratorCodeGenerator
        val generator = MigratorCodeGenerator(
            schemaInspector = schemaInspector,
            sqlBatchInspector = sqlBatchInspector,
            migrationInspector = migrationInspector,
            packageName = "com.example.db",
            outputDir = outputDir,
        )
        generator.generateCode()

        // Verify that the file was created
        val outputFile = File(outputDir, "com/example/db/VersionBasedDatabaseMigrations.kt")
        assertTrue(outputFile.exists(), "Output file should be created")

        // Read the file content
        val fileContent = outputFile.readText()

        // Verify that the file contains the expected content
        assertTrue(fileContent.contains("class VersionBasedDatabaseMigrations"), "File should contain the class declaration")
        assertTrue(fileContent.contains("override suspend fun applyMigration"), "File should contain the applyMigration function")
        assertTrue(fileContent.contains("private suspend fun executeAllSql"), "File should contain the executeAllSql function")
        assertTrue(fileContent.contains("migrateToVersion1"), "File should contain migration function for version 1")
        assertTrue(fileContent.contains("CREATE TABLE test_table"), "File should contain the CREATE TABLE statement")
        assertTrue(fileContent.contains("CREATE TABLE another_table"), "File should contain the second CREATE TABLE statement")
        assertTrue(fileContent.contains("CREATE TABLE migration_test"), "File should contain the migration statement")
    }

    @Test
    @DisplayName("Test MigratorCodeGenerator with both SchemaInspector and SQLBatchInspector")
    fun testGenerateCodeWithBothInspectors() {
        // Create schema SQL file
        val schemaSql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            );
        """.trimIndent()

        // Create batch SQL file
        val batchSql = """
            INSERT INTO users (id, name, email) VALUES (1, 'Admin', 'admin@example.com');
            CREATE INDEX idx_users_email ON users(email);
        """.trimIndent()

        // Create separate directories for schema, batch, and migrations
        val schemaDir = File(tempDir.toFile(), "schema")
        val batchDir = File(tempDir.toFile(), "batch")
        val migrationDir = File(tempDir.toFile(), "migrations")
        schemaDir.mkdir()
        batchDir.mkdir()
        migrationDir.mkdir()

        File(schemaDir, "schema.sql").writeText(schemaSql)
        File(batchDir, "batch.sql").writeText(batchSql)
        File(migrationDir, "0001.sql").writeText("CREATE TABLE migration_test (id INTEGER);")

        // Create inspectors
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)
        val sqlBatchInspector = SQLBatchInspector(sqlDirectory = batchDir, mandatory = false)
        val migrationInspector = MigrationInspector(sqlDirectory = migrationDir)

        // Create generator
        val generator = MigratorCodeGenerator(
            schemaInspector = schemaInspector,
            sqlBatchInspector = sqlBatchInspector,
            migrationInspector = migrationInspector,
            packageName = "com.example.db",
            outputDir = outputDir,
        )
        generator.generateCode()

        // Verify that the file was created
        val outputFile = File(outputDir, "com/example/db/VersionBasedDatabaseMigrations.kt")
        assertTrue(outputFile.exists(), "Output file should be created")

        // Read the file content
        val fileContent = outputFile.readText()

        // Verify that the generated code contains statements from all inspectors
        assertTrue(fileContent.contains("CREATE TABLE users"), "Should contain schema statement")
        assertTrue(fileContent.contains("INSERT INTO users"), "Should contain batch statement")
        assertTrue(fileContent.contains("CREATE INDEX idx_users_email"), "Should contain batch index statement")
        assertTrue(fileContent.contains("CREATE TABLE migration_test"), "Should contain migration statement")
        assertTrue(fileContent.contains("migrateToVersion1"), "Should contain migration function for version 1")
        assertTrue(fileContent.contains("Execute schema statements first") && fileContent.contains("Execute init statements"), "Should mention both schema and init statements in comments")

        // Verify incremental migration logic in applyMigration function
        assertTrue(fileContent.contains("if (currentVersion == -1)"), "Should contain initial setup logic")
        assertTrue(fileContent.contains("if (currentVersion < 1)"), "Should contain incremental migration check for version 1")
        assertTrue(fileContent.contains("migrateToVersion1(conn)"), "Should call migration function for version 1")
        assertTrue(fileContent.contains("return@withContext 1") || fileContent.contains("    1"), "Should return latest version")
    }

    @Test
    @DisplayName("Test MigratorCodeGenerator with multiple migration versions")
    fun testGenerateCodeWithMultipleMigrationVersions() {
        // Create schema SQL file
        val schemaSql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
        """.trimIndent()

        // Create separate directories
        val schemaDir = File(tempDir.toFile(), "schema")
        val batchDir = File(tempDir.toFile(), "batch")
        val migrationDir = File(tempDir.toFile(), "migrations")
        schemaDir.mkdir()
        batchDir.mkdir()
        migrationDir.mkdir()

        File(schemaDir, "schema.sql").writeText(schemaSql)
        File(batchDir, "batch.sql").writeText("INSERT INTO users (id, name) VALUES (1, 'Admin');")

        // Create multiple migration files
        File(migrationDir, "0001.sql").writeText("ALTER TABLE users ADD COLUMN email TEXT;")
        File(migrationDir, "0003.sql").writeText("CREATE INDEX idx_users_email ON users(email);")
        File(migrationDir, "0005.sql").writeText("ALTER TABLE users ADD COLUMN created_at TIMESTAMP;")

        // Create inspectors
        val schemaInspector = SchemaInspector(schemaDirectory = schemaDir)
        val sqlBatchInspector = SQLBatchInspector(sqlDirectory = batchDir, mandatory = false)
        val migrationInspector = MigrationInspector(sqlDirectory = migrationDir)

        // Create generator
        val generator = MigratorCodeGenerator(
            schemaInspector = schemaInspector,
            sqlBatchInspector = sqlBatchInspector,
            migrationInspector = migrationInspector,
            packageName = "com.example.db",
            outputDir = outputDir,
        )

        // Generate the code
        generator.generateCode()

        // Verify that the file was created
        val outputFile = File(outputDir, "com/example/db/VersionBasedDatabaseMigrations.kt")
        assertTrue(outputFile.exists(), "Output file should be created")

        // Read the file content
        val fileContent = outputFile.readText()

        // Verify migration functions for all versions
        assertTrue(fileContent.contains("migrateToVersion1"), "Should contain migration function for version 1")
        assertTrue(fileContent.contains("migrateToVersion3"), "Should contain migration function for version 3")
        assertTrue(fileContent.contains("migrateToVersion5"), "Should contain migration function for version 5")

        // Verify incremental migration logic for all versions
        assertTrue(fileContent.contains("if (currentVersion < 1)"), "Should contain check for version 1")
        assertTrue(fileContent.contains("if (currentVersion < 3)"), "Should contain check for version 3")
        assertTrue(fileContent.contains("if (currentVersion < 5)"), "Should contain check for version 5")

        // Verify migration function calls
        assertTrue(fileContent.contains("migrateToVersion1(conn)"), "Should call migration function for version 1")
        assertTrue(fileContent.contains("migrateToVersion3(conn)"), "Should call migration function for version 3")
        assertTrue(fileContent.contains("migrateToVersion5(conn)"), "Should call migration function for version 5")

        // Verify latest version return
        assertTrue(fileContent.contains("return@withContext 5") || fileContent.contains("    5"), "Should return latest version 5")
    }
}
