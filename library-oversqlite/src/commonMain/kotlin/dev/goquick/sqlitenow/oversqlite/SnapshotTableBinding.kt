package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive

/** Metadata is borrowed from the admitted opening owner, never obtained per row. */
internal data class BootstrapUniqueKey(val columns: List<Pair<Int, String>>)

internal data class BootstrapForeignKey(
    val column: Int,
    val parentSlot: Int,
    val parentColumn: Int,
    val collation: String,
)

internal data class BootstrapForeignSql(val membership: String, val insert: String, val verification: String)

internal data class BootstrapTableSchema(
    val generationIndex: Int,
    val generationTable: GenerationTable,
    val physical: TableInfo,
    val columns: List<ColumnInfo>,
    val uniqueKeys: List<BootstrapUniqueKey>,
    val foreignKeys: List<BootstrapForeignKey>,
    val triggerSignature: String,
    val indexSignature: String = "",
    val indexWorkComponents: Long = 0,
)

/** Immutable SQL and physical mapping for one canonical local slot. */
internal class SnapshotTableBinding internal constructor(
    val slot: Int,
    val metadata: BootstrapTableSchema,
    val createSql: String,
    val indexSql: List<String>,
    val insertSql: String,
    val insertPairSql: String?,
    val scanSql: String,
    val measureSql: String,
    val auditSql: String,
    val businessSql: String,
    val businessVerificationSql: String,
    val baselineSql: String,
    val authoritativeVerificationSql: String,
    val installPageSql: String,
    val foreignSql: List<BootstrapForeignSql>,
) {
    val tableIndex get() = metadata.generationIndex
    // Generation schemas identify remote rows. Managed physical tables live in main.
    val localSchema get() = "main"
    val schema get() = metadata.generationTable.schema
    val table get() = metadata.generationTable.table
    val columns get() = metadata.columns
    val stageName = "_sync_bootstrap_values_t$slot"
    val keyIndex = columns.indexOfFirst { it.name == metadata.generationTable.syncKeyColumns.single() }
    val projectionInfo = metadata.physical.copy(columns = columns)
}

internal class SnapshotDescriptor private constructor(
    val tables: List<SnapshotTableBinding>,
    val generationId: String,
    val generationFingerprint: String,
    val canonical: String,
    val signature: String,
    val charge: WorkspaceCharge,
    val descriptorBytes: Long,
    val sqlBytes: Long,
) {
    companion object {
        /**
         * The metadata holder owns [schemas] until [admit] transfers its charge.
         * No descriptor/SQL output is retained during the first counting pass.
         */
        suspend fun create(
            generation: SyncGeneration,
            schemas: List<BootstrapTableSchema>,
            parameterLimit: Int,
            admit: suspend (WorkspaceCharge) -> Unit,
        ): SnapshotDescriptor {
            require(schemas.size == generation.tables.size && schemas.isNotEmpty())
            var columns = 0L; var foreignKeys = 0L; var uniqueComponents = 0L
            val seen = mutableSetOf<Int>()
            for ((slot, schema) in schemas.withIndex()) {
                require(schema.generationIndex in generation.tables.indices && seen.add(schema.generationIndex))
                require(schema.generationTable == generation.tables[schema.generationIndex])
                require(schema.physical.tableKind == "table" && schema.physical.table == schema.generationTable.table)
                require(schema.columns.size == schema.generationTable.columns.size && schema.columns.isNotEmpty())
                require(schema.columns.size.toLong() + 5 <= parameterLimit.toLong()) { "bootstrap SQL parameter limit" }
                if (slot > 0) {
                    val before = schemas[slot - 1].generationTable
                    val after = schema.generationTable
                    require(before.schema < after.schema || before.schema == after.schema && before.table < after.table)
                }
                // Generation identifiers are admitted ASCII, hence this is UTF-8 byte order.
                for ((index, column) in schema.columns.withIndex()) {
                    if (index > 0) require(schema.columns[index - 1].name < column.name)
                    val wire = schema.generationTable.columns.single { it.name == column.name }
                    require(column.hidden == 0 && column.wireType == wire.wireType)
                    require(schema.physical.columns.single { it.name == column.name } == column)
                    require(wire.nullable != (column.notNull || column.isPrimaryKey))
                    wire.requireCompatibleStorage(column)
                }
                val key = schema.columns.single { it.name == schema.generationTable.syncKeyColumns.single() }
                require(key.wireType == "text" || key.wireType == "uuid")
                require(sqliteAffinity(key.declaredType) in setOf("TEXT", "BLOB"))
                require(isCanonicalSha256(schema.triggerSignature))
                require(schema.indexSignature.isEmpty() || isCanonicalSha256(schema.indexSignature))
                require(schema.indexWorkComponents >= 0)
                for (unique in schema.uniqueKeys) {
                    require(unique.columns.isNotEmpty())
                    for ((index, collation) in unique.columns) {
                        require(index in schema.columns.indices && collation in bootstrapCollations)
                        uniqueComponents = bootstrapCheckedAdd(uniqueComponents, 1)
                    }
                }
                for (foreign in schema.foreignKeys) {
                    require(foreign.column in schema.columns.indices && foreign.parentSlot in schemas.indices &&
                        foreign.parentColumn in schemas[foreign.parentSlot].columns.indices && foreign.collation in bootstrapCollations)
                }
                columns = bootstrapCheckedAdd(columns, schema.columns.size.toLong())
                foreignKeys = bootstrapCheckedAdd(foreignKeys, schema.foreignKeys.size.toLong())
            }
            val descriptorCounter = BootstrapTextSink()
            emitBootstrapDescriptor(descriptorCounter, generation, schemas)
            var sqlBytes = 0L
            for ((slot, schema) in schemas.withIndex()) {
                emitBootstrapTableSql(slot, schema, schemas, 2L * (schema.columns.size + 5) <= parameterLimit) { _, emit ->
                    val counter = BootstrapTextSink(); emit(counter)
                    sqlBytes = bootstrapCheckedAdd(sqlBytes, counter.bytes)
                }
            }
            val charge = bootstrapDescriptorCharge(descriptorCounter.bytes, sqlBytes,
                schemas.size.toLong(), columns, foreignKeys, uniqueComponents)
            admit(charge)
            val descriptorSink = BootstrapTextSink(StringBuilder())
            emitBootstrapDescriptor(descriptorSink, generation, schemas)
            check(descriptorSink.bytes == descriptorCounter.bytes)
            val canonical = descriptorSink.result()
            var renderedBytes = 0L
            val bindings = schemas.mapIndexed { slot, schema ->
                val sql = linkedMapOf<String, String>()
                emitBootstrapTableSql(slot, schema, schemas, 2L * (schema.columns.size + 5) <= parameterLimit) { name, emit ->
                    val sink = BootstrapTextSink(StringBuilder()); emit(sink)
                    renderedBytes = bootstrapCheckedAdd(renderedBytes, sink.bytes)
                    check(sql.put(name, sink.result()) == null)
                }
                SnapshotTableBinding(slot, schema, sql.getValue("create"),
                    sql.filterKeys { it.startsWith("index.") }.values.toList(),
                    sql.getValue("insert"), sql["insert.pair"], sql.getValue("scan"), sql.getValue("measure"), sql.getValue("audit"),
                    sql.getValue("business"), sql.getValue("verify.business"), sql.getValue("baseline"),
                    sql.getValue("verify.authoritative"), sql.getValue("install.page"),
                    schema.foreignKeys.indices.map { foreign -> BootstrapForeignSql(sql.getValue("foreign.$foreign.membership"),
                        sql.getValue("foreign.$foreign.insert"), sql.getValue("foreign.$foreign.verify")) })
            }
            check(renderedBytes == sqlBytes)
            return SnapshotDescriptor(bindings, generation.id, generation.fingerprint, canonical, sha256Hex(canonical.encodeToByteArray()),
                charge, descriptorCounter.bytes, sqlBytes)
        }
    }
}

private val bootstrapCollations = setOf("BINARY", "NOCASE", "RTRIM")

internal fun bootstrapDescriptorCharge(d: Long, q: Long, tables: Long, columns: Long,
    foreignKeys: Long, uniqueComponents: Long): WorkspaceCharge {
    fun sum(vararg values: Long) = values.fold(0L, ::bootstrapCheckedAdd)
    fun times(value: Long, factor: Long): Long {
        require(value >= 0 && value <= Long.MAX_VALUE / factor) { "bootstrap descriptor overflow" }
        return value * factor
    }
    return WorkspaceCharge(
        sum(16, times(tables, 8), times(columns, 4), times(foreignKeys, 8), times(uniqueComponents, 8)),
        sum(4096, times(d, 12), times(q, 12), times(sum(tables, columns, foreignKeys, uniqueComponents), 256)),
    )
}

/** One emitter supports both exact sizing and rendering. */
private class BootstrapTextSink(private val output: StringBuilder? = null) {
    var bytes = 0L
        private set
    fun text(value: String) {
        bytes = bootstrapCheckedAdd(bytes, utf8Size(value)); output?.append(value)
    }
    fun json(value: String) = text(JsonPrimitive(value).toString())
    fun identifier(value: String) {
        text("\"")
        for (character in value) text(if (character == '"') "\"\"" else character.toString())
        text("\"")
    }
    fun result(): String = checkNotNull(output).toString()
}

private fun emitBootstrapDescriptor(out: BootstrapTextSink, generation: SyncGeneration,
    schemas: List<BootstrapTableSchema>) {
    out.text("["); out.json("oversqlite.bootstrap.descriptor.v1"); out.text(",")
    out.json(generation.id); out.text(","); out.json(generation.fingerprint); out.text(",[")
    for ((slot, schema) in schemas.withIndex()) {
        if (slot > 0) out.text(",")
        out.text("["); out.json(slot.toString()); out.text(","); out.json(schema.generationIndex.toString())
        out.text(","); out.json(schema.generationTable.schema); out.text(","); out.json(schema.generationTable.table)
        out.text(","); out.json("main"); out.text(","); out.json(schema.physical.table)
        out.text(","); out.json(schema.triggerSignature)
        out.text(","); out.json(schema.indexSignature); out.text(","); out.json(schema.indexWorkComponents.toString())
        out.text(","); out.text(schema.physical.withoutRowId.toString())
        out.text(","); out.text(schema.physical.strict.toString()); out.text(",[")
        for ((index, column) in schema.columns.withIndex()) {
            if (index > 0) out.text(",")
            out.text("["); out.json(column.name); out.text(","); out.json(column.declaredType)
            out.text(","); out.json(checkNotNull(column.wireType)); out.text(",")
            out.text(if (column.notNull || column.isPrimaryKey) "false" else "true")
            out.text(","); out.json(column.primaryKeyOrdinal.toString()); out.text("]")
        }
        out.text("],[")
        for ((index, key) in schema.uniqueKeys.withIndex()) {
            if (index > 0) out.text(",")
            out.text("[")
            for ((component, pair) in key.columns.withIndex()) {
                if (component > 0) out.text(",")
                out.text("["); out.json(pair.first.toString()); out.text(","); out.json(pair.second); out.text("]")
            }
            out.text("]")
        }
        out.text("],[")
        for ((index, foreign) in schema.foreignKeys.withIndex()) {
            if (index > 0) out.text(",")
            out.text("["); out.json(foreign.column.toString()); out.text(","); out.json(foreign.parentSlot.toString())
            out.text(","); out.json(foreign.parentColumn.toString()); out.text(","); out.json(foreign.collation); out.text("]")
        }
        out.text("]]")
    }
    out.text("]]")
}

internal enum class BootstrapRefusal(val label: String) {
    UNSUPPORTED_KEY("unsupported_key"), UNSUPPORTED_SCHEMA("unsupported_schema"),
    UNSUPPORTED_INDEX("unsupported_index"), MISSING_CHILD_INDEX("missing_child_index"),
    ADDITIONAL_TRIGGER("additional_trigger"), TRIGGER_DEFINITION("trigger_definition"),
    UNMANAGED_FOREIGN_KEY("unmanaged_foreign_key"),
}

internal data class BootstrapChildIndexRequirement(val table: String, val column: String, val collation: String)

internal sealed interface BootstrapSchemaAdmission {
    data class Eligible(val descriptor: SnapshotDescriptor, val pin: BootstrapSchemaPin) : BootstrapSchemaAdmission
    data class Unavailable(val reason: BootstrapRefusal, val missingChildIndex: BootstrapChildIndexRequirement? = null) : BootstrapSchemaAdmission
}

/** In-process metadata proof; a reopened connection must obtain a new proof. */
internal class BootstrapSchemaPin internal constructor(
    private val db: SafeSQLiteConnection,
    private val validated: ValidatedConfig,
    private val mainVersion: Long,
    private val tempVersion: Long,
) {
    suspend fun requireCurrent(connection: SafeSQLiteConnection, metadata: ValidatedConfig) {
        require(connection === db && metadata === validated && db.inTransaction()) { "bootstrap schema owner changed" }
        require(db.scalarLong("PRAGMA main.schema_version") == mainVersion &&
            db.scalarLong("PRAGMA temp.schema_version") == tempVersion) { "bootstrap schema changed" }
    }
}

private class BootstrapCapabilityRefused(
    val reason: BootstrapRefusal, val missingChildIndex: BootstrapChildIndexRequirement?,
) : RuntimeException()
private fun bootstrapRefuse(reason: BootstrapRefusal, missingChildIndex: BootstrapChildIndexRequirement? = null): Nothing =
    throw BootstrapCapabilityRefused(reason, missingChildIndex)

/**
 * The caller reads physical metadata after [metadataSchemaVersion] in this same
 * writer. SQL/quota/cancellation failures propagate; only explicit capabilities
 * below can select the general engine before staging.
 */
internal suspend fun admitBootstrapSchema(
    db: SafeSQLiteConnection,
    validated: ValidatedConfig,
    metadataSchemaVersion: Long,
    workspace: OversqliteWorkspace,
    unit: String,
    cache: StatementCache,
    independentHolder: WorkspaceCharge = WorkspaceCharge(),
): BootstrapSchemaAdmission {
    cache.requireConnectionOwner(db)
    require(db.inTransaction() && workspace.connectionOwner === db)
    val pin = BootstrapSchemaPin(db, validated, metadataSchemaVersion, db.scalarLong("PRAGMA temp.schema_version"))
    pin.requireCurrent(db, validated)
    return try {
        val descriptor = SnapshotReadWorkspace.use(workspace, unit, cache) { read ->
            BootstrapSchemaReader(db, validated, workspace, unit, cache, independentHolder = independentHolder, readWorkspace = read).read()
        }
        pin.requireCurrent(db, validated)
        BootstrapSchemaAdmission.Eligible(descriptor, pin)
    } catch (refusal: BootstrapCapabilityRefused) {
        if (refusal.suppressedExceptions.isNotEmpty()) throw refusal
        // Even a capability refusal cannot adopt metadata changed during callbacks.
        pin.requireCurrent(db, validated)
        BootstrapSchemaAdmission.Unavailable(refusal.reason, refusal.missingChildIndex)
    }
}

private data class BootstrapIndexSummary(
    val unique: List<BootstrapUniqueKey>,
    val leading: Set<Pair<Int, String>>,
    val primaryCollation: String,
    val signature: String,
    val workComponents: Long,
)

private class BootstrapSchemaReader(
    val db: SafeSQLiteConnection,
    val validated: ValidatedConfig,
    val workspace: OversqliteWorkspace,
    val unit: String,
    val cache: StatementCache,
    val eligibility: Boolean = true,
    val captureSignatures: Map<String, String>? = null,
    val independentHolder: WorkspaceCharge = WorkspaceCharge(),
    val readWorkspace: SnapshotReadWorkspace? = null,
) {
    private var retained = independentHolder
    private suspend fun retain(extra: WorkspaceCharge) {
        retained = workspace.add(unit, retained, extra)
        workspace.check(unit, retained, statementCache = cache, read = readWorkspace)
    }
    private suspend fun transient(entries: Long, bytes: Long) =
        workspace.check(unit, workspace.add(unit, retained, WorkspaceCharge(entries, bytes)), statementCache = cache, read = readWorkspace)
    private fun times(value: Long, factor: Long) = workspace.scale(unit, value, factor)
    private suspend fun <T> one(sql: String, vararg args: String, read: (SQLiteStatement) -> T): T =
        db.withPreparedStatement(sql, cache) { st ->
            args.forEachIndexed { index, value -> st.bindText(index + 1, value) }
            check(st.step()); read(st)
        }

    suspend fun read(): SnapshotDescriptor {
        val generation = checkNotNull(validated.generation) { "bootstrap requires admitted generation" }
        generation.requireMatchingLocalSchema(validated.tableInfoByName, validated.keyByTable)
        val c = validated.tableInfoByName.values.sumOf { it.columns.size.toLong() }
        val f = validated.tableInfoByName.values.sumOf { it.foreignKeys.size.toLong() }
        retain(WorkspaceCharge(16 + 8L * generation.tables.size + 4 * c + 8 * f,
            4096 + times(c + f + generation.tables.size, 256)))
        if (generation.tables.isEmpty()) bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_SCHEMA)
        val ordered = generation.tables.withIndex().sortedWith(compareBy({ it.value.schema }, { it.value.table }))
        val columns = ordered.map { validated.tableInfoByName.getValue(it.value.table).columns.sortedBy { col -> col.name } }
        for ((slot, entry) in ordered.withIndex()) {
            val info = validated.tableInfoByName.getValue(entry.value.table)
            val key = info.columns.single { it.isPrimaryKey }
            if (info.tableKind != "table" || info.columns.any { it.hidden != 0 }) bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_SCHEMA)
            if (key.wireType !in setOf("text", "uuid") || sqliteAffinity(key.declaredType) !in setOf("TEXT", "BLOB") ||
                key.wireType == "text" && sqliteAffinity(key.declaredType) != "TEXT") bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_KEY)
            check(columns[slot].isNotEmpty())
        }
        if (eligibility && one("""SELECT EXISTS(SELECT 1 FROM sqlite_schema t JOIN pragma_foreign_key_list(t.name) f
            JOIN _sync_managed_tables m ON m.table_name=f."table" COLLATE NOCASE
            WHERE t.type='table' AND NOT EXISTS(SELECT 1 FROM _sync_managed_tables c WHERE c.table_name=t.name COLLATE NOCASE))""") { it.getLong(0) } != 0L)
            bootstrapRefuse(BootstrapRefusal.UNMANAGED_FOREIGN_KEY)
        if (eligibility && one("""SELECT EXISTS(SELECT 1 FROM sqlite_temp_schema t JOIN pragma_foreign_key_list(t.name,'temp') f
            JOIN _sync_managed_tables m ON m.table_name=f."table" COLLATE NOCASE WHERE t.type='table')""") { it.getLong(0) } != 0L)
            bootstrapRefuse(BootstrapRefusal.UNMANAGED_FOREIGN_KEY)
        if (eligibility) inspectInstallerTriggers()
        val triggers = ordered.mapIndexed { slot, entry ->
            currentCoroutineContext().ensureActive()
            captureSignatures?.getValue(entry.value.table) ?:
                captureSignature(entry.value, validated.tableInfoByName.getValue(entry.value.table), columns[slot])
        }
        val indexes = ordered.mapIndexed { slot, entry -> readIndexes(entry.value.table, columns[slot]) }.toMutableList()
        val schemas = ordered.mapIndexed { slot, entry ->
            val table = entry.value
            val info = validated.tableInfoByName.getValue(table.table)
            val foreign = table.foreignKeys.map { fk ->
                val parent = ordered.indexOfFirst { it.value.schema == fk.referencedSchema && it.value.table == fk.referencedTable }
                if (parent < 0) bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_SCHEMA)
                val childColumn = columns[slot].indexOfFirst { it.name == fk.columns.single() }
                val parentColumn = columns[parent].indexOfFirst { it.name == fk.referencedColumns.single() }
                val comparison = indexes[parent].primaryCollation
                if (childColumn to comparison !in indexes[slot].leading) bootstrapRefuse(BootstrapRefusal.MISSING_CHILD_INDEX,
                    BootstrapChildIndexRequirement(table.table, columns[slot][childColumn].name, comparison))
                BootstrapForeignKey(childColumn, parent, parentColumn, comparison)
            }
            BootstrapTableSchema(entry.index, table, info, columns[slot], indexes[slot].unique,
                foreign, triggers[slot], indexes[slot].signature, indexes[slot].workComponents)
        }
        // Leading-index helper sets are no longer part of the retained descriptor.
        indexes.clear()
        val parameters = maxOf(20, columns.maxOf { it.size } + 5)
        transient(parameters.toLong(), times(parameters.toLong(), 256))
        db.withPreparedStatement("SELECT ?$parameters", cache) { /* prepare proves the actual driver limit */ }
        return SnapshotDescriptor.create(generation, schemas, parameters) { descriptorCharge ->
            // The settled descriptor charge takes ownership of schemas and their
            // unique/FK lists. Temporary metadata/trigger copies have ended.
            workspace.check(unit, workspace.add(unit, independentHolder, descriptorCharge), statementCache = cache, read = readWorkspace)
        }
    }

    private suspend fun captureSignature(table: GenerationTable, info: TableInfo, columns: List<ColumnInfo>): String {
        val measure = one("""SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))+length(CAST(COALESCE(sql,'') AS BLOB))),0)
            FROM (SELECT name,sql,tbl_name FROM sqlite_schema WHERE type='trigger'
            UNION ALL SELECT name,sql,tbl_name FROM sqlite_temp_schema WHERE type='trigger') WHERE lower(tbl_name)=?""", table.table) {
            it.getLong(0) to it.getLong(1)
        }
        if (measure.first != 6L) bootstrapRefuse(BootstrapRefusal.ADDITIONAL_TRIGGER)
        // Six fixed templates; every expansion is linear in the admitted column,
        // FK and <=63-byte generation identifiers. This also owns token copies.
        val templateBound = 65536 + times((columns.size + info.foreignKeys.size).toLong(), 16384)
        transient(64 + times((columns.size + info.foreignKeys.size).toLong(), 64),
            workspace.add(unit, WorkspaceCharge(bytes = times(templateBound, 16)), WorkspaceCharge(bytes = times(measure.second, 16))).bytes)
        val expected = expectedCaptureTriggers(table.schema, ValidatedSyncTable(table.table, table.syncKeyColumns.single()), info)
        check(expected.sumOf { utf8Size(it.first) + utf8Size(it.second) } <= templateBound) { "bootstrap trigger template bound changed" }
        val digest = newSnapshotDigest()
        db.withPreparedStatement("""SELECT name,sql,tbl_name,temporary FROM
            (SELECT name,sql,tbl_name,0 temporary FROM sqlite_schema WHERE type='trigger'
            UNION ALL SELECT name,sql,tbl_name,1 temporary FROM sqlite_temp_schema WHERE type='trigger')
            WHERE lower(tbl_name)=? ORDER BY name""", cache) { st ->
            st.bindText(1, table.table)
            while (st.step()) {
                currentCoroutineContext().ensureActive()
                val name = st.getText(0)
                if (st.getLong(3) != 0L || st.getText(2) != table.table || expected.none { it.first == name })
                    bootstrapRefuse(BootstrapRefusal.ADDITIONAL_TRIGGER)
                val actual = st.getText(1)
                if (retainedSqlSignature(actual) != retainedSqlSignature(expected.single { it.first == name }.second))
                    bootstrapRefuse(BootstrapRefusal.TRIGGER_DEFINITION)
                val frame = canonicalizeJsonElement(JsonArray(listOf(JsonPrimitive(name), JsonPrimitive(actual)))).encodeToByteArray()
                digest.append(bootstrapBigEndian(frame.size.toLong(), 8)); digest.append(frame)
            }
        }
        return digest.finishHex()
    }

    private suspend fun inspectInstallerTriggers() {
        // Fixed templates only; admit their independent construction/token holder.
        transient(256, 1048576)
        val expected = bootstrapBaselineGuardDdl + snapshotStageAccountingDdl + mapOf(
            "_sync_provenance_context_immutable" to provenanceImmutableTriggerDdl,
        ) + listOf("insert", "update", "delete").flatMap { operation -> listOf(
            "_sync_conflict_revision_$operation" to expectedConflictRevisionTriggerSql(operation),
            "_sync_baseline_revision_$operation" to expectedBaselineRevisionTriggerSql(operation),
        ) }.toMap()
        val condition = oversqliteRetainedTables.joinToString(",") { snapshotSqlText(it) }
        val query = """SELECT name,sql,tbl_name,temporary FROM
            (SELECT name,sql,tbl_name,0 temporary FROM sqlite_schema WHERE type='trigger'
            UNION ALL SELECT name,sql,tbl_name,1 temporary FROM sqlite_temp_schema WHERE type='trigger')
            WHERE lower(tbl_name) IN ($condition) OR lower(tbl_name) IN
                (SELECT '_sync_bootstrap_values_t'||slot FROM _sync_bootstrap_tables)"""
        val bytes = one("SELECT COALESCE(sum(length(CAST(COALESCE(sql,'') AS BLOB))+length(CAST(name AS BLOB))),0) FROM ($query)") { it.getLong(0) }
        transient(expected.size.toLong() * 8 + 32, 1048576 + times(bytes, 16))
        var seen = 0
        db.withPreparedStatement(query, cache) { st ->
            while (st.step()) {
                val name = st.getText(0)
                val definition = expected[name]
                if (st.getLong(3) != 0L || definition == null) bootstrapRefuse(BootstrapRefusal.ADDITIONAL_TRIGGER)
                val owner = when {
                    name in snapshotStageAccountingDdl -> "_sync_snapshot_stage"
                    name == "_sync_provenance_context_immutable" -> "_sync_provenance_contexts"
                    name.startsWith("_sync_conflict_revision_") -> "_sync_dirty_rows"
                    else -> "_sync_row_state"
                }
                if (st.getText(2) != owner) bootstrapRefuse(BootstrapRefusal.TRIGGER_DEFINITION)
                if (retainedSqlSignature(st.getText(1)) != retainedSqlSignature(definition)) bootstrapRefuse(BootstrapRefusal.TRIGGER_DEFINITION)
                seen++
            }
        }
        if (seen != expected.size) bootstrapRefuse(BootstrapRefusal.TRIGGER_DEFINITION)
    }

    private suspend fun readIndexes(table: String, columns: List<ColumnInfo>): BootstrapIndexSummary {
        val measured = one("SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM pragma_index_list(?)", table) {
            it.getLong(0) to it.getLong(1)
        }
        transient(16 + times(measured.first, 4), 4096 + times(measured.second, 12))
        val names = mutableListOf<Triple<String, String, Boolean>>()
        db.withPreparedStatement("SELECT name,origin,\"unique\",partial FROM pragma_index_list(?) ORDER BY name", cache) { st ->
            st.bindText(1, table)
            while (st.step()) {
                if (st.getLong(3) != 0L) bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_INDEX)
                names += Triple(st.getText(0), st.getText(1), st.getLong(2) != 0L)
            }
        }
        val unique = mutableListOf<BootstrapUniqueKey>(); val leading = mutableSetOf<Pair<Int, String>>()
        val digest = newSnapshotDigest(); var primary: String? = null; var work = 0L
        // Index names stay independently admitted until this table's metadata ends.
        val nameHolder = WorkspaceCharge(16 + times(measured.first, 4), 4096 + times(measured.second, 12))
        for ((name, origin, isUnique) in names) {
            currentCoroutineContext().ensureActive()
            val components = one("""SELECT count(*),COALESCE(sum(length(CAST(COALESCE(name,'') AS BLOB))+length(CAST(COALESCE(coll,'') AS BLOB))),0),
                COALESCE(max(cid<0 OR name IS NULL OR coll IS NULL),0) FROM pragma_index_xinfo(?) WHERE "key"=1""", name) {
                Triple(it.getLong(0), it.getLong(1), it.getLong(2))
            }
            if (components.first == 0L || components.third != 0L) bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_INDEX)
            transient(nameHolder.entries + 16 + times(components.first, 8),
                nameHolder.bytes + 4096 + times(components.second, 16))
            val keys = mutableListOf<Pair<Int, String>>()
            val identity = mutableListOf(name, origin, isUnique.toString())
            db.withPreparedStatement("SELECT name,coll,\"desc\" FROM pragma_index_xinfo(?) WHERE \"key\"=1 ORDER BY seqno", cache) { st ->
                st.bindText(1, name)
                while (st.step()) {
                    val column = columns.indexOfFirst { it.name == st.getText(0) }
                    val comparison = bootstrapCollations.singleOrNull { it == st.getText(1) }
                    if (column < 0 || comparison == null) bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_INDEX)
                    keys += column to comparison
                    identity += columns[column].name; identity += comparison; identity += st.getLong(2).toString()
                }
            }
            work = bootstrapCheckedAdd(work, keys.size.toLong())
            if (origin == "pk") {
                if (keys.size != 1 || columns[keys.single().first].name != validated.pkByTable.getValue(table))
                    bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_INDEX)
                primary = keys.single().second
            }
            retain(WorkspaceCharge(2, 128))
            leading += keys.first()
            if (isUnique) {
                retain(WorkspaceCharge(times(keys.size.toLong(), 8), times(keys.size.toLong(), 256)))
                unique += BootstrapUniqueKey(keys)
            }
            val frame = canonicalizeJsonElement(JsonArray(identity.map(::JsonPrimitive))).encodeToByteArray()
            digest.append(bootstrapBigEndian(frame.size.toLong(), 8)); digest.append(frame)
        }
        return BootstrapIndexSummary(unique, leading, primary ?: bootstrapRefuse(BootstrapRefusal.UNSUPPORTED_INDEX), digest.finishHex(), work)
    }
}

/** Read-only format audit returns no eligibility capability and cannot select an engine. */
internal suspend fun bootstrapDescriptorForAudit(
    db: SafeSQLiteConnection, validated: ValidatedConfig, workspace: OversqliteWorkspace,
    unit: String, cache: StatementCache, proof: RetainedCaptureProof, independentHolder: WorkspaceCharge,
): SnapshotDescriptor {
    require(proof.connection === db && proof.schemaVersion == db.scalarLong("PRAGMA main.schema_version")) {
        "bootstrap metadata owner changed"
    }
    return try {
        SnapshotReadWorkspace.use(workspace, unit, cache) { read ->
        BootstrapSchemaReader(db, validated, workspace, unit, cache, eligibility = false,
            captureSignatures = proof.signatures, independentHolder = independentHolder, readWorkspace = read).read().also {
            require(proof.schemaVersion == db.scalarLong("PRAGMA main.schema_version")) { "bootstrap schema changed" }
        }
        }
    } catch (refusal: BootstrapCapabilityRefused) {
        throw IllegalArgumentException("incompatible retained bootstrap layout; recreate this development database explicitly", refusal)
    }
}

/** Clean-gate priority is owned by the existing lifecycle before this query. */
internal suspend fun bootstrapManagedStateEmpty(db: SafeSQLiteConnection, validated: ValidatedConfig, cache: StatementCache): Boolean {
    require(db.inTransaction())
    for (table in validated.tables) {
        currentCoroutineContext().ensureActive()
        if (db.withPreparedStatement("SELECT EXISTS(SELECT 1 FROM main.${quoteIdent(table.tableName)} LIMIT 1)", cache) { check(it.step()); it.getLong(0) } != 0L) return false
    }
    return db.withPreparedStatement("""SELECT EXISTS(SELECT 1 FROM _sync_row_state LIMIT 1)
        OR EXISTS(SELECT 1 FROM _sync_relationships LIMIT 1) OR EXISTS(SELECT 1 FROM _sync_apply_plan LIMIT 1)
        OR EXISTS(SELECT 1 FROM _sync_apply_rows LIMIT 1) OR EXISTS(SELECT 1 FROM _sync_apply_edges LIMIT 1)""", cache) {
        check(it.step()); it.getLong(0) == 0L
    }
}

private fun emitBootstrapTableSql(slot: Int, schema: BootstrapTableSchema, schemas: List<BootstrapTableSchema>, pairs: Boolean,
    statement: (String, (BootstrapTextSink) -> Unit) -> Unit) {
    val stage = "_sync_bootstrap_values_t$slot"
    fun target(out: BootstrapTextSink) {
        out.identifier("main"); out.text("."); out.identifier(schema.physical.table)
    }
    fun range(out: BootstrapTextSink, alias: String = "") {
        out.text(" WHERE ${alias}snapshot_id=? AND ${alias}row_ordinal>? AND ${alias}row_ordinal<=?")
    }
    fun literal(value: String) = snapshotSqlText(value)
    fun scalar(index: Int) = "s.v$index"
    fun scalarText(index: Int): String {
        val value = scalar(index)
        return "CASE typeof($value) WHEN 'blob' THEN lower(hex($value)) WHEN 'real' THEN printf('%!.17g',$value) WHEN 'null' THEN NULL ELSE CAST($value AS TEXT) END"
    }
    fun payload(out: BootstrapTextSink) {
        out.text("json_object(")
        for (index in schema.columns.indices) {
            if (index > 0) out.text(",")
            out.text(literal(schema.columns[index].name))
            out.text(",json_object('storage',typeof(${scalar(index)}),'value',${scalarText(index)})")
        }
        out.text(")")
    }
    statement("install.page") { out ->
        out.text("SELECT count(*),COALESCE(max(row_ordinal),0) FROM (SELECT row_ordinal FROM ")
        out.identifier(stage); out.text(" WHERE snapshot_id=? AND row_ordinal>? ORDER BY row_ordinal LIMIT ?)")
    }
    statement("baseline") { out ->
        out.text("INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known,server_revision) SELECT ")
        out.text(literal(schema.generationTable.schema)); out.text(","); out.text(literal(schema.generationTable.table))
        out.text(",s.key_json,s.row_version,'live',"); payload(out)
        out.text(","); out.text(baselineProvenanceSql("'snapshot'", "?", "?")); out.text(",1,?+s.row_ordinal FROM ")
        out.identifier(stage); out.text(" s"); range(out, "s."); out.text(" ORDER BY s.row_ordinal")
    }
    statement("verify.authoritative") { out ->
        out.text("SELECT EXISTS(SELECT 1 FROM "); out.identifier(stage)
        out.text(" s LEFT JOIN _sync_row_state a ON a.schema_name="); out.text(literal(schema.generationTable.schema))
        out.text(" AND a.table_name="); out.text(literal(schema.generationTable.table))
        out.text(" AND a.key_json=s.key_json COLLATE BINARY WHERE s.snapshot_id=? AND (a.key_json IS NULL OR a.server_kind!='live'")
        out.text(" OR a.row_version!=s.row_version OR a.relationships_known!=1 OR a.server_revision!=?+s.row_ordinal OR a.server_context_id!=?")
        out.text(" OR a.server_provenance!="); out.text(baselineProvenanceSql("'snapshot'", "?", "?"))
        // The stored payload must be exactly the canonical object the baseline statement builds from this stage row:
        // equal bytes fix the member set, each member's storage tag and its canonical value text, and reject any other
        // representation. Regenerating it costs one json_object per row instead of JSON functions per column.
        out.text(" COLLATE BINARY OR a.server_payload IS NOT "); payload(out); out.text(" COLLATE BINARY))")
    }
    for ((foreignIndex, foreign) in schema.foreignKeys.withIndex()) {
        val parent = schemas[foreign.parentSlot]
        val join = " FROM ${quoteIdent(stage)} s JOIN ${quoteIdent("_sync_bootstrap_values_t${foreign.parentSlot}")} p ON p.snapshot_id=s.snapshot_id AND p.v${foreign.parentColumn}=s.v${foreign.column} COLLATE ${quoteIdent(foreign.collation)}"
        statement("foreign.$foreignIndex.membership") { out ->
            out.text("SELECT EXISTS(SELECT 1 FROM "); out.identifier(stage)
            out.text(" s WHERE s.snapshot_id=? AND s.v${foreign.column} IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ")
            out.identifier("_sync_bootstrap_values_t${foreign.parentSlot}")
            out.text(" p WHERE p.snapshot_id=s.snapshot_id AND p.v${foreign.parentColumn}=s.v${foreign.column} COLLATE ")
            out.identifier(foreign.collation); out.text("))")
        }
        fun edgeColumns(base: String): List<Pair<String, String>> = listOf("view" to "'server'",
            "child_schema" to literal(schema.generationTable.schema), "child_table" to literal(schema.generationTable.table),
            "child_key" to "s.key_json", "fk_column" to literal(schema.columns[foreign.column].name.lowercase()),
            "parent_schema" to literal(parent.generationTable.schema), "parent_table" to literal(parent.generationTable.table),
            "parent_key" to "p.key_json", "owner_revision" to "$base+s.row_ordinal")
        fun edge(): String = edgeColumns("?").joinToString(",") { (name, value) -> "$value AS $name" }
        statement("foreign.$foreignIndex.insert") { out ->
            out.text("INSERT INTO _sync_relationships(view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision) SELECT ")
            out.text(edge()); out.text(join); range(out, "s."); out.text(" AND s.v${foreign.column} IS NOT NULL")
        }
        statement("foreign.$foreignIndex.verify") { out ->
            out.text("WITH expected AS (SELECT "); out.text(edge()); out.text(join)
            out.text(" WHERE s.snapshot_id=? AND s.v${foreign.column} IS NOT NULL) SELECT ")
            val equality = listOf("view", "child_schema", "child_table", "child_key", "fk_column", "parent_schema", "parent_table", "parent_key", "owner_revision").joinToString(" AND ") { "r.$it=e.$it COLLATE BINARY" }
            out.text("EXISTS(SELECT 1 FROM expected e WHERE NOT EXISTS(SELECT 1 FROM _sync_relationships r WHERE $equality)) OR ")
            // An expected edge's owner revision is base + its child's row ordinal, so the only staged
            // row that can match a relationship is the one at owner_revision - base: read it by key.
            // ?1 and ?2 are the base revision and snapshot id bound for the expected set above.
            val stagedEquality = edgeColumns("?1").joinToString(" AND ") { (name, value) -> "r.$name=($value) COLLATE BINARY" }
            out.text("EXISTS(SELECT 1 FROM _sync_relationships r WHERE r.view='server' AND r.child_schema=${literal(schema.generationTable.schema)} AND r.child_table=${literal(schema.generationTable.table)} AND r.fk_column=${literal(schema.columns[foreign.column].name.lowercase())} AND NOT EXISTS(SELECT 1")
            out.text(join); out.text(" WHERE s.snapshot_id=?2 AND s.row_ordinal=r.owner_revision-?1 AND s.v${foreign.column} IS NOT NULL AND $stagedEquality)),(SELECT count(*) FROM expected)")
        }
    }
    statement("create") { out ->
        out.text("CREATE TABLE "); out.identifier(stage)
        out.text("(snapshot_id TEXT NOT NULL,row_ordinal INTEGER NOT NULL CHECK(typeof(row_ordinal)='integer' AND row_ordinal>0),")
        out.text("key_json TEXT NOT NULL CHECK(json_valid(key_json) AND json_type(key_json)='object'),")
        out.text("row_version INTEGER NOT NULL CHECK(typeof(row_version)='integer' AND row_version>0),")
        out.text("charge INTEGER NOT NULL CHECK(typeof(charge)='integer' AND charge>=0)")
        for ((index, column) in schema.columns.withIndex()) {
            val affinity = sqliteAffinity(column.declaredType)
            out.text(",v$index $affinity")
            if (column.notNull || column.isPrimaryKey) out.text(" NOT NULL")
            out.text(" CHECK(")
            if (!column.notNull && !column.isPrimaryKey) out.text("v$index IS NULL OR ")
            out.text("(typeof(v$index)='"); out.text(affinity.lowercase()); out.text("'")
            if (column.wireType == "uuid" && affinity == "BLOB") out.text(" AND length(v$index)=16")
            out.text("))")
        }
        out.text(",PRIMARY KEY(snapshot_id,row_ordinal)) WITHOUT ROWID")
    }
    for ((index, key) in schema.uniqueKeys.withIndex()) statement("index.unique.$index") { out ->
        out.text("CREATE UNIQUE INDEX "); out.identifier("${stage}_unique_$index")
        out.text(" ON "); out.identifier(stage); out.text("(snapshot_id")
        for ((column, collation) in key.columns) { out.text(",v$column COLLATE "); out.identifier(collation) }
        out.text(")")
    }
    for ((index, foreign) in schema.foreignKeys.withIndex()) statement("index.foreign.$index") { out ->
        out.text("CREATE INDEX "); out.identifier("${stage}_foreign_$index")
        out.text(" ON "); out.identifier(stage); out.text("(snapshot_id,v${foreign.column} COLLATE ")
        out.identifier(foreign.collation); out.text(",row_ordinal)")
    }
    fun insert(out: BootstrapTextSink, rows: Int) {
        out.text("INSERT INTO "); out.identifier(stage); out.text("(snapshot_id,row_ordinal,key_json,row_version,charge")
        for (index in schema.columns.indices) out.text(",v$index")
        out.text(") VALUES")
        repeat(rows) { row ->
            if (row > 0) out.text(",")
            out.text("(?,?,?,?,?")
            for (index in schema.columns.indices) out.text(",?")
            out.text(")")
        }
    }
    statement("insert") { insert(it, 1) }
    if (pairs) statement("insert.pair") { insert(it, 2) }
    statement("scan") { out ->
        out.text("SELECT row_ordinal,key_json,row_version,charge")
        for (index in schema.columns.indices) out.text(",v$index")
        out.text(" FROM "); out.identifier(stage); range(out); out.text(" ORDER BY row_ordinal")
    }
    statement("measure") { out ->
        out.text("SELECT count(*),COALESCE(sum(charge),0),COALESCE(sum(32+length(CAST(key_json AS BLOB))")
        for ((index, column) in schema.columns.withIndex()) {
            out.text("+1+CASE WHEN v$index IS NULL THEN 0 ELSE ")
            out.text(if (sqliteAffinity(column.declaredType) in setOf("TEXT", "BLOB")) "8+length(CAST(v$index AS BLOB))" else "8")
            out.text(" END")
        }
        out.text("),0) FROM "); out.identifier(stage); range(out)
    }
    statement("audit") { out ->
        fun body() {
            out.text("32+length(CAST(key_json AS BLOB))")
            for ((index, column) in schema.columns.withIndex()) {
                out.text("+1+CASE WHEN v$index IS NULL THEN 0 ELSE ")
                out.text(if (sqliteAffinity(column.declaredType) in setOf("TEXT", "BLOB")) "8+length(CAST(v$index AS BLOB))" else "8")
                out.text(" END")
            }
        }
        out.text("SELECT count(*),COALESCE(sum(charge),0),COALESCE(sum(96+6*length(CAST(key_json AS BLOB))+2*(")
        body(); out.text(")),0),COALESCE(sum("); body(); out.text("),0),COALESCE(max("); body()
        out.text("),0),COALESCE(max(length(CAST(key_json AS BLOB))),0),COALESCE(sum(CASE WHEN snapshot_id!=? COLLATE BINARY OR row_version>? OR row_ordinal<=? OR row_ordinal>?")
        out.text(" OR typeof(snapshot_id)!='text' OR typeof(row_ordinal)!='integer' OR row_ordinal<=0 OR typeof(key_json)!='text' OR typeof(row_version)!='integer' OR row_version<=0 OR typeof(charge)!='integer'")
        out.text(" OR key_json!=json_object("); out.text(snapshotSqlText(schema.generationTable.syncKeyColumns.single().lowercase())); out.text(",")
        val keyIndex = schema.columns.indexOfFirst { it.name == schema.generationTable.syncKeyColumns.single() }
        if (sqliteAffinity(schema.columns[keyIndex].declaredType) == "BLOB") out.text("lower(hex(v$keyIndex))") else out.text("v$keyIndex")
        out.text(") COLLATE BINARY")
        for ((index, column) in schema.columns.withIndex()) {
            out.text(" OR (")
            if (!column.notNull && !column.isPrimaryKey) out.text("v$index IS NOT NULL AND ")
            out.text("typeof(v$index)!='${sqliteAffinity(column.declaredType).lowercase()}')")
            if (column.wireType == "uuid" && sqliteAffinity(column.declaredType) == "BLOB") out.text(" OR (v$index IS NOT NULL AND length(v$index)!=16)")
        }
        out.text(" THEN 1 ELSE 0 END),0) FROM "); out.identifier(stage)
    }
    statement("business") { out ->
        out.text("INSERT INTO "); target(out); out.text("(")
        for ((index, column) in schema.columns.withIndex()) {
            if (index > 0) out.text(","); out.identifier(column.name)
        }
        out.text(") SELECT ")
        for (index in schema.columns.indices) { if (index > 0) out.text(","); out.text("v$index") }
        out.text(" FROM "); out.identifier(stage); range(out); out.text(" ORDER BY row_ordinal")
    }
    statement("verify.business") { out ->
        out.text("SELECT (SELECT count(*) FROM "); target(out)
        out.text(")!=(SELECT count(*) FROM "); out.identifier(stage); out.text(" WHERE snapshot_id=?) OR EXISTS(SELECT 1 FROM ")
        out.identifier(stage); out.text(" s LEFT JOIN "); target(out); out.text(" b ON b.")
        val keyIndex = schema.columns.indexOfFirst { it.name == schema.generationTable.syncKeyColumns.single() }
        out.identifier(schema.columns[keyIndex].name); out.text("=s.v$keyIndex WHERE s.snapshot_id=? AND (b.")
        out.identifier(schema.columns[keyIndex].name); out.text(" IS NULL")
        for ((index, column) in schema.columns.withIndex()) {
            out.text(" OR typeof(b."); out.identifier(column.name); out.text(")!=typeof(s.v$index) OR NOT (b.")
            out.identifier(column.name); out.text(" IS s.v$index COLLATE BINARY)")
        }
        out.text("))")
    }
}
