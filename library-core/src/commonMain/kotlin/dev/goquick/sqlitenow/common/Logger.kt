/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.common

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Message
import co.touchlab.kermit.MessageStringFormatter
import co.touchlab.kermit.Severity
import co.touchlab.kermit.Tag
import co.touchlab.kermit.loggerConfigInit
import co.touchlab.kermit.platformLogWriter

enum class LogLevel {
    Error,
    Warn,
    Info,
    Debug,
}

interface SqliteNowLogger {
    fun e(throwable: Throwable? = null, message: () -> String)
    fun w(throwable: Throwable? = null, message: () -> String)
    fun i(throwable: Throwable? = null, message: () -> String)
    fun d(throwable: Throwable? = null, message: () -> String)
}

object NoopSqliteNowLogger : SqliteNowLogger {
    override fun e(throwable: Throwable?, message: () -> String) = Unit
    override fun w(throwable: Throwable?, message: () -> String) = Unit
    override fun i(throwable: Throwable?, message: () -> String) = Unit
    override fun d(throwable: Throwable?, message: () -> String) = Unit
}

private class CustomLogFormatter(
    private val scope: String,
    useTag: Boolean,
) : MessageStringFormatter {
    private val appTag = if (useTag) Tag(scope) else null
    override fun formatMessage(severity: Severity?, tag: Tag?, message: Message): String {
        return super.formatMessage(severity, appTag, Message("[${scope}] " + message.message))
    }
}

class KermitSqliteNowLogger(
    minLevel: LogLevel = LogLevel.Info,
) : SqliteNowLogger {
    private val delegate = Logger(
        config = loggerConfigInit(
            platformLogWriter(
                CustomLogFormatter("sqlitenow", platform() != PlatformType.ANDROID)
            ),
            minSeverity = minLevel.toSeverity(),
        ),
        tag = "sqlitenow",
    )

    override fun e(throwable: Throwable?, message: () -> String) {
        delegate.e(throwable) { message() }
    }

    override fun w(throwable: Throwable?, message: () -> String) {
        delegate.w(throwable) { message() }
    }

    override fun i(throwable: Throwable?, message: () -> String) {
        delegate.i(throwable) { message() }
    }

    override fun d(throwable: Throwable?, message: () -> String) {
        delegate.d(throwable) { message() }
    }
}

private fun LogLevel.toSeverity(): Severity = when (this) {
    LogLevel.Error -> Severity.Error
    LogLevel.Warn -> Severity.Warn
    LogLevel.Info -> Severity.Info
    LogLevel.Debug -> Severity.Debug
}

internal val originalSqliteNowLogger: SqliteNowLogger = KermitSqliteNowLogger(minLevel = LogLevel.Info)
var sqliteNowLogger: SqliteNowLogger = originalSqliteNowLogger
