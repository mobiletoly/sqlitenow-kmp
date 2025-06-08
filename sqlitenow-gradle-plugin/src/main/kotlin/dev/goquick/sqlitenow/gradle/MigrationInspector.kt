package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import dev.goquick.sqlitenow.gradle.sqlite.parseSqlStatements
import java.io.File
import java.sql.Connection

/**
 * Inspects migration SQL files and determines the latest version number.
 * This class uses composition with SQLBatchInspector to collect SQL statements and additionally
 * parses version numbers from SQL filenames to determine the latest migration version.
 *
 * Expected filename format: NNNN.sql (e.g., 0001.sql, 0005.sql, 0010.sql)
 * where NNNN is a zero-padded version number.
 */
internal class MigrationInspector(
    sqlDirectory: File
) {
    /**
     * The SQL statements organized by version number.
     * Key: version number, Value: list of SQL statements for that version
     */
    val sqlStatements: LinkedHashMap<Int, List<SqlSingleStatement>>

    /**
     * The latest (highest) version number found in the SQL filenames.
     * Returns 0 if no valid version files are found.
     */
    val latestVersion: Int

    init {
        SqlFileProcessor.validateDirectory(sqlDirectory, "Migration", mandatory = false)
        val sqlFiles = SqlFileProcessor.findSqlFiles(sqlDirectory)

        // Parse files and partition SQL statements by version
        val statementsMap = LinkedHashMap<Int, List<SqlSingleStatement>>()
        var maxVersion = 0

        for (file in sqlFiles.sortedBy { it.name }) {
            val version = parseVersionFromFilename(file.name)
                ?: throw IllegalArgumentException("Migration files must have numeric names (e.g., 0001.sql). Invalid file: ${file.name}")

            // Check for duplicate version numbers
            if (statementsMap.containsKey(version)) {
                throw IllegalArgumentException("Duplicate migration version $version found. File: ${file.name}")
            }

            val fileStatements = SqlFileProcessor.parseAllSqlFiles(listOf(file))

            // Add statements to the version map
            statementsMap[version] = fileStatements

            if (version > maxVersion) {
                maxVersion = version
            }
        }

        // Assign the final map
        sqlStatements = statementsMap
        latestVersion = maxVersion
    }

    /**
     * Parses a version number from a SQL filename.
     * Expected format: NNNN.sql where NNNN is a numeric version.
     *
     * @param filename The filename to parse (e.g., "0005.sql")
     * @return The parsed version number, or null if the filename doesn't match the expected format
     */
    private fun parseVersionFromFilename(filename: String): Int? {
        // Remove the .sql extension
        val nameWithoutExtension = filename.substringBeforeLast(".sql")

        // Check if the remaining part is a valid integer
        return try {
            nameWithoutExtension.toInt()
        } catch (e: NumberFormatException) {
            println("File name does not match the expected format (ignored): $filename")
            null
        }
    }
}
