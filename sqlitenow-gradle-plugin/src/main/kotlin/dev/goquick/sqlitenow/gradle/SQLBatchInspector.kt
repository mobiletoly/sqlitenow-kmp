package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import dev.goquick.sqlitenow.gradle.sqlite.parseSqlStatements
import java.io.File

/**
 * Inspects and collects SQL statements from all .sql files in a directory.
 * This class is similar to SchemaInspector but with simplified functionality
 * that only collects SQL statements without any inspection or analysis.
 */
internal class SQLBatchInspector(
    sqlDirectory: File,
    mandatory: Boolean,
) {
    val sqlStatements: List<SqlSingleStatement>
    val sqlFiles: List<File>

    init {
        SqlFileProcessor.validateDirectory(sqlDirectory, "SQL", mandatory = mandatory)
        sqlFiles = SqlFileProcessor.findSqlFiles(sqlDirectory)
        sqlStatements = SqlFileProcessor.parseAllSqlFiles(sqlFiles)
    }
}
