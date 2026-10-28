package dev.goquick.sqlitenow.core

internal class RecordingDiagnosticRuntime : SqliteNowDiagnosticRuntime {
    var clockFactory: () -> (() -> Long) = { { 0L } }
    var sink: (String) -> Boolean = { true }
    var formatter: (String) -> String? = { sqlStatementShape(it, 4_096, 512) }
    override fun clock() = clockFactory()
    override fun emit(message: String) = sink(message)
    override fun shape(sql: String) = formatter(sql)
}

internal fun recordingDiagnostics(enabled: Boolean = true, maxSqlFamilies: Int = 1_024, maxActiveSpans: Int = 128) =
    SqliteNowDiagnostics(enabled, maxSqlFamilies, maxActiveSpans, RecordingDiagnosticRuntime())

internal val SqliteNowDiagnostics.recording: RecordingDiagnosticRuntime
    get() = runtime as RecordingDiagnosticRuntime
