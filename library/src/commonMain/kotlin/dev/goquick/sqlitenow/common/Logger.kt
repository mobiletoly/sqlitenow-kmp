package dev.goquick.sqlitenow.common

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Message
import co.touchlab.kermit.MessageStringFormatter
import co.touchlab.kermit.Severity
import co.touchlab.kermit.Tag
import co.touchlab.kermit.loggerConfigInit
import co.touchlab.kermit.platformLogWriter

private class CustomLogFormatter(
    private val scope: String,
    useTag: Boolean,
) : MessageStringFormatter {
    private val appTag = if (useTag) Tag(scope) else null
    override fun formatMessage(severity: Severity?, tag: Tag?, message: Message): String {
        return super.formatMessage(severity, appTag, Message("[${scope}] " + message.message))
    }
}

class SqliteNowLogger(severity: Severity) : Logger(
    config = loggerConfigInit(
        platformLogWriter(
            CustomLogFormatter("sqlitenow", platform() != PlatformType.ANDROID)
        ),
        minSeverity = severity
    ),
    tag = "sqlitenow"
)

internal val originalSqliteNowLogger = SqliteNowLogger(severity = Severity.Info)
var sqliteNowLogger = originalSqliteNowLogger
