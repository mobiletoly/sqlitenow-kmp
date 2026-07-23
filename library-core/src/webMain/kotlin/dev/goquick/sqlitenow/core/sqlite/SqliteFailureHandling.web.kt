/*
 * Copyright 2026 Toly Pochkin
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
package dev.goquick.sqlitenow.core.sqlite

internal fun Throwable.toSqliteExceptionPreservingSuppressed(): SqliteException {
    if (this is SqliteException) return this
    return SqliteException(message, this).also { normalized ->
        suppressedExceptions.forEach(normalized::addSuppressedIfAbsent)
    }
}

internal fun Throwable.addSuppressedIfAbsent(additional: Throwable) {
    if (additional !== this && suppressedExceptions.none { it === additional }) {
        addSuppressed(additional)
    }
}
