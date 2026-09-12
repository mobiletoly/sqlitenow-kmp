package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import java.security.MessageDigest
import kotlin.test.assertSame

/** JVM test-only access: no accessor or observation object is shipped with the library. */
internal fun replayValidatedOwner(client: DefaultOversqliteClient): ValidatedConfig {
    val field = DefaultOversqliteClient::class.java.getDeclaredField("sessionState")
    field.isAccessible = true
    return (field.get(client) as OversqliteClientSessionState).requireValidatedConfig()
}

/** Test-only raw-connection observation, restored after replay. */
internal class ReplaySqlObservation private constructor(
    private val db: SafeSQLiteConnection,
    private val delegate: SQLiteConnection,
) : SQLiteConnection by delegate {
    data class Counts(var prepares: Long = 0, var steps: Long = 0, var resets: Long = 0,
        var clears: Long = 0, var closes: Long = 0)

    val counts = linkedMapOf<String, Counts>()
    private val bindingsDigest = MessageDigest.getInstance("SHA-256")
    var selectedExecutions = 0L
        private set

    override fun prepare(sql: String): SQLiteStatement {
        val count = counts.getOrPut(sql) { Counts() }
        count.prepares++
        val statement = delegate.prepare(sql)
        val bindings = sortedMapOf<Int, String>()
        return object : SQLiteStatement by statement {
            override fun bindNull(index: Int) { statement.bindNull(index); bindings[index] = "null" }
            override fun bindLong(index: Int, value: Long) { statement.bindLong(index, value); bindings[index] = "integer:$value" }
            override fun bindDouble(index: Int, value: Double) { statement.bindDouble(index, value); bindings[index] = "real:${value.toBits()}" }
            override fun bindText(index: Int, value: String) { statement.bindText(index, value); bindings[index] = "text:$value" }
            override fun bindBlob(index: Int, value: ByteArray) { statement.bindBlob(index, value); bindings[index] = "blob:${value.toHex()}" }
            override fun step(): Boolean {
                count.steps++
                if (selected(sql)) {
                    selectedExecutions++
                    digestField(sql)
                    digestField(bindings.size.toString())
                    bindings.forEach { (index, value) -> digestField(index.toString()); digestField(value) }
                }
                return statement.step()
            }
            override fun reset() { statement.reset(); count.resets++ }
            override fun clearBindings() { statement.clearBindings(); bindings.clear(); count.clears++ }
            override fun close() { statement.close(); bindings.clear(); count.closes++ }
        }
    }

    private fun digestField(value: String) {
        val bytes = value.encodeToByteArray()
        bindingsDigest.update(bytes.size.toString().encodeToByteArray())
        bindingsDigest.update(0.toByte())
        bindingsDigest.update(bytes)
    }

    fun bindingTraceHash(): String = bindingsDigest.digest().toHex()

    suspend fun restore() = db.withExclusiveAccess {
        val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
        assertSame(this@ReplaySqlObservation, field.get(db), "observation must still own its installed wrapper")
        field.set(db, delegate)
    }

    companion object {
        suspend fun install(db: SafeSQLiteConnection): ReplaySqlObservation = db.withExclusiveAccess {
            val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
            ReplaySqlObservation(db, field.get(db) as SQLiteConnection).also { field.set(db, it) }
        }

        // Only synthetic business-row queries: excludes random source/reservation identities.
        fun selected(sql: String): Boolean =
            sql.contains("FROM \"nodes\"") || sql.startsWith("INSERT INTO \"nodes\"") ||
                (sql.startsWith("SELECT json_object(") && sql.contains("AS observed"))
    }
}

internal fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

/** The optional object exists only in the separately instrumented measurement checkout. */
internal class ReplayConstructionObservation private constructor(private val type: Class<*>, private val instance: Any) {
    fun reset() { type.getMethod("reset").invoke(instance) }
    fun snapshot(): String = type.getMethod("snapshot").invoke(instance) as String

    companion object {
        fun load(): ReplayConstructionObservation? {
            val type = try { Class.forName("dev.goquick.sqlitenow.oversqlite.ReplaySqlConstructionCounters") }
                catch (_: ClassNotFoundException) { return null }
            return ReplayConstructionObservation(type, type.getField("INSTANCE").get(null))
        }
    }
}
