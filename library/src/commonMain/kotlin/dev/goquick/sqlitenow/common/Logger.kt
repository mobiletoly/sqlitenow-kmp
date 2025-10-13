/*
 * Copyright 2025 Anatoliy Pochkin
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
