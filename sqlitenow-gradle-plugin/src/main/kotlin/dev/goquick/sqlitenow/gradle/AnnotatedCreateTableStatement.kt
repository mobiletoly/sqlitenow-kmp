package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.CreateTableStatement

data class AnnotatedCreateTableStatement(
    override val name: String,
    val src: CreateTableStatement,
    override val annotations: StatementAnnotationOverrides,
    val columns: List<Column>
) : AnnotatedStatement {

    data class Column(
        val src: CreateTableStatement.Column,
        val annotations: Map<String, String?>
    ) {
        fun isNullable(): Boolean {
            // Explicit annotations take precedence over database constraints
            return when {
                annotations.containsKey(AnnotationConstants.NULLABLE) -> true
                annotations.containsKey(AnnotationConstants.NON_NULL) -> false
                else -> !src.notNull // Default to database constraint
            }
        }
    }

    /** Finds a column by name (case-insensitive); null if not found */
    fun findColumnByName(columnName: String): Column? {
        return columns.find { col ->
            col.src.name.equals(columnName, ignoreCase = true)
        }
    }

    companion object {
        fun parse(
            name: String,
            createTableStatement: CreateTableStatement,
            topComments: List<String>,
            innerComments: List<String>,
        ): AnnotatedCreateTableStatement {
            val tableAnnotations = StatementAnnotationOverrides.parse(
                extractAnnotations(topComments)
            )
            val fieldAnnotations = extractFieldAssociatedAnnotations(innerComments)

            return AnnotatedCreateTableStatement(
                name = name,
                src = createTableStatement,
                annotations = tableAnnotations,
                columns = createTableStatement.columns.map { column ->
                    Column(
                        src = column,
                        annotations = fieldAnnotations[column.name] ?: emptyMap()
                    )
                }
            )
        }
    }
}
