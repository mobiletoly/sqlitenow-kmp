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
package dev.goquick.sqlitenow.oversqlite

internal data class ValidatedSyncTable(
    val tableName: String,
    val syncKeyColumnName: String,
)

internal data class ValidatedConfig(
    val schema: String,
    val tables: List<ValidatedSyncTable>,
    val pkByTable: Map<String, String>,
    val keyByTable: Map<String, List<String>>,
    val tableOrder: Map<String, Int>,
    val tableInfoByName: Map<String, TableInfo>,
)
