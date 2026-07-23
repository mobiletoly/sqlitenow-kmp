package dev.goquick.sqlitenow.core.sqlite

internal fun <Statement, Result> prepareSqlJsStatement(
    prepare: () -> Statement,
    normalize: (Statement) -> String,
    closeOnFailure: (Statement) -> Unit,
    wrap: (Statement, String) -> Result,
): Result {
    val statement = prepare()
    return try {
        wrap(statement, normalize(statement))
    } catch (primary: Throwable) {
        try {
            closeOnFailure(statement)
        } catch (cleanup: Throwable) {
            primary.addSuppressedIfAbsent(cleanup)
        }
        throw primary
    }
}
