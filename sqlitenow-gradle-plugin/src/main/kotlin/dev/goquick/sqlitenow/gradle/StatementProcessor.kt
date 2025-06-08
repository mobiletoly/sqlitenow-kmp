package dev.goquick.sqlitenow.gradle

/**
 * Helper class for processing and filtering SQL statements.
 * Eliminates duplicate statement filtering logic across generator classes.
 */
class StatementProcessor(private val allStatements: List<AnnotatedStatement>) {

    /**
     * Lazily filtered SELECT statements.
     * Cached to avoid repeated filtering operations.
     */
    val selectStatements: List<AnnotatedSelectStatement> by lazy {
        allStatements.filterIsInstance<AnnotatedSelectStatement>()
    }

    /**
     * Lazily filtered EXECUTE statements (INSERT/DELETE).
     * Cached to avoid repeated filtering operations.
     */
    val executeStatements: List<AnnotatedExecuteStatement> by lazy {
        allStatements.filterIsInstance<AnnotatedExecuteStatement>()
    }

    /**
     * Processes both SELECT and EXECUTE statements with a unified callback approach.
     * Eliminates the need for separate forEach loops in generator classes.
     *
     * @param onSelectStatement Callback for processing SELECT statements
     * @param onExecuteStatement Callback for processing EXECUTE statements
     */
    fun processStatements(
        onSelectStatement: (AnnotatedSelectStatement) -> Unit,
        onExecuteStatement: (AnnotatedExecuteStatement) -> Unit
    ) {
        selectStatements.forEach(onSelectStatement)
        executeStatements.forEach(onExecuteStatement)
    }
}
