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
package dev.goquick.sqlitenow.gradle.dart

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement

internal data class DartField(
    val index: Int,
    val propertyName: String,
    val dartType: String,
    val sqliteReadCall: String,
    val nullable: Boolean,
    val adapter: DartAdapterColumn?,
    val source: AnnotatedSelectStatement.Field? = null,
    val dynamicField: AnnotatedSelectStatement.Field? = null,
)

internal data class DartMappingColumn(
    val sourcePropertyName: String,
    val constructorPropertyName: String,
    val field: DartField,
)

internal data class DartParameter(
    val propertyName: String,
    val dartType: String,
    val adapter: DartAdapterColumn?,
)

internal data class DartAdapterColumn(
    val encodeName: String,
    val decodeName: String,
)

internal data class DartMigrationStep(
    val version: Int,
    val statements: List<String>,
    val freshOnly: Boolean = false,
)
