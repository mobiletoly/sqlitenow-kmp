package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.SQLITE_DATA_BLOB
import androidx.sqlite.SQLITE_DATA_FLOAT
import androidx.sqlite.SQLITE_DATA_INTEGER
import androidx.sqlite.SQLITE_DATA_NULL
import androidx.sqlite.SQLITE_DATA_TEXT
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan
import kotlinx.serialization.json.*

/** Valid only for one admitted row iteration; it carries no operation or capture history. */
internal class SnapshotTypedValues(
    private val db: SafeSQLiteConnection,
    val info: TableInfo,
    val source: PayloadSource,
    private var values: List<TypedValue>,
    private var payload: JsonObject?,
    private var span: SqliteNowDiagnosticSpan?,
) {
    private var active = true
    fun requireOwner(connection: SafeSQLiteConnection, table: TableInfo, objectPayload: JsonObject? = null, payloadSource: PayloadSource? = null) {
        check(active && db === connection && info === table && (objectPayload == null || payload === objectPayload) && (payloadSource == null || source == payloadSource)) { "typed row belongs to a different iteration or schema owner" }
    }
    fun value(column: String): TypedValue {
        check(active)
        return values[info.columnNamesLower.indexOf(column.lowercase()).also { check(it>=0) }]
    }
    fun bind(statement: SQLiteStatement) {
        check(active)
        values.forEachIndexed { n,v -> OversqliteValueCodec.bindTypedValue(statement,n+1,info.columns[n],v) }
        span?.aggregateWork("row_processing.typed_bind_reuse",0)
    }
    fun matches(other: JsonObject, source: PayloadSource): Boolean {
        check(active)
        if(other.size!=info.columns.size)return false
        return info.columns.withIndex().all { (n,col) ->
            val value=other[col.name.lowercase()] ?: other[col.name] ?: return false
            val decoded = decodeSnapshotScalar(span) { OversqliteValueCodec.decodePayloadValue(col,value,source) }
            typedValuesEqual(values[n],decoded)
        }
    }
    fun matches(other: SnapshotTypedValues): Boolean {
        check(active && other.active && db === other.db && info === other.info)
        return values.indices.all { typedValuesEqual(values[it],other.values[it]) }
    }
    fun close() { active=false;values=emptyList();payload=null;span=null }
    companion object {
        fun charge(workspace: OversqliteWorkspace, unit: String, info: TableInfo, bytes: Long) =
            WorkspaceCharge(8+2L*info.columns.size,workspace.add(unit,WorkspaceCharge(bytes=workspace.scale(unit,bytes,6)),WorkspaceCharge(bytes=128L*info.columns.size)).bytes)
        /** Reads SQLite's parameter projection, never a business row or reconstructed JSON. */
        fun readProjection(info: TableInfo, statement: SQLiteStatement, offset: Int = 0, canonicalizeReals: Boolean = true): List<TypedValue> =
            info.columns.mapIndexed { columnIndex, column ->
                val index = columnIndex + offset
                val type = statement.getColumnType(index)
                val storage = when (type) {
                    SQLITE_DATA_NULL -> "NULL"
                    SQLITE_DATA_INTEGER -> "INTEGER"
                    SQLITE_DATA_FLOAT -> "REAL"
                    SQLITE_DATA_TEXT -> "TEXT"
                    SQLITE_DATA_BLOB -> "BLOB"
                    else -> error("unknown SQLite projection storage")
                }
                if (type == SQLITE_DATA_NULL) {
                    require(!column.notNull && !column.isPrimaryKey) { "required projection value is null" }
                    TypedValue.Null
                } else {
                    require(storage == sqliteAffinity(column.declaredType)) { "projection storage does not match affinity" }
                    when (type) {
                        SQLITE_DATA_INTEGER -> TypedValue.Integer(statement.getLong(index).toString())
                        SQLITE_DATA_FLOAT -> {
                            val value = statement.getDouble(index)
                            require(value.isFinite()) { "finite projection value required" }
                            TypedValue.Real(if (canonicalizeReals) canonicalizeFiniteJsonNumber(value.toString()) else value.toString())
                        }
                        SQLITE_DATA_TEXT -> TypedValue.Text(statement.getText(index))
                        SQLITE_DATA_BLOB -> TypedValue.Blob(statement.getBlob(index))
                        else -> error("unknown SQLite projection storage")
                    }
                }
            }
        fun decode(db: SafeSQLiteConnection, info: TableInfo, source: PayloadSource, payload: JsonObject, span: SqliteNowDiagnosticSpan?): SnapshotTypedValues {
            require(payload.size==info.columns.size && info.columns.all { it.name.lowercase() in payload || it.name in payload }) { "typed row column set mismatch" }
            return SnapshotTypedValues(db,info,source,info.columns.map { c ->
                decodeSnapshotScalar(span) { OversqliteValueCodec.decodePayloadValue(c,payload[c.name.lowercase()] ?: payload.getValue(c.name),source) }
            },payload,span)
        }
    }
}

/** Numeric call/failure counters only; disabled execution adds no diagnostic producer work. */
internal inline fun <T> decodeSnapshotScalar(span: SqliteNowDiagnosticSpan?, block: () -> T): T {
    if (span == null) return block()
    var failed = false
    try { return block() } catch (failure: Throwable) { failed = true; throw failure }
    finally { span.aggregateWork("row_processing.typed_scalar_decode",0,failed) }
}
