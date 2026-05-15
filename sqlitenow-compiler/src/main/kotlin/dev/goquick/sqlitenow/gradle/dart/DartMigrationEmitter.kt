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

import dev.goquick.sqlitenow.gradle.database.MigrationInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector

internal class DartMigrationEmitter(
    private val schemaInspector: SchemaInspector,
    private val sqlBatchInspector: SQLBatchInspector,
    private val migrationInspector: MigrationInspector,
) {
    fun emit(writer: DartWriter) {
        writer.line("static final List<SqliteNowMigrationStep> _migrations = [")
        writer.indent {
            migrationSteps().forEach { step ->
                if (step.freshOnly) {
                    line("SqliteNowMigrationStep.fresh(${step.version}, (connection) async {")
                } else {
                    line("SqliteNowMigrationStep(${step.version}, (connection) async {")
                }
                indent {
                    step.statements.forEach { statement ->
                        line("await connection.execute(${statement.toDartRawString()});")
                    }
                }
                line("}),")
            }
        }
        writer.line("];")
    }

    private fun migrationSteps(): List<DartMigrationStep> {
        if (migrationInspector.sqlStatements.isNotEmpty()) {
            val bootstrapStatements =
                schemaInspector.sqlStatements.map { it.sql } + sqlBatchInspector.sqlStatements.map { it.sql }
            return buildList {
                if (bootstrapStatements.isNotEmpty()) {
                    add(DartMigrationStep(migrationInspector.latestVersion, bootstrapStatements, freshOnly = true))
                }
                migrationInspector.sqlStatements.forEach { (version, statements) ->
                    add(DartMigrationStep(version, statements.map { it.sql }))
                }
            }
        }
        val statements = schemaInspector.sqlStatements.map { it.sql } + sqlBatchInspector.sqlStatements.map { it.sql }
        return if (statements.isEmpty()) {
            emptyList()
        } else {
            listOf(DartMigrationStep(1, statements))
        }
    }
}
