package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SchemaEvolutionCompatibilityTest {
    @Test
    fun `schema evolution compatibility accepts additive tables columns and nullable foreign keys`() {
        val old = generation(
            id = "1",
            tables = listOf(notes()),
        )
        val parent = table("parents")
        val child = table("children")
        val childWithNullableParent = child.copy(
            columns = child.columns + column("parent_id", "uuid", true),
            foreignKeys = listOf(foreignKey()),
        )

        listOf(
            "nullable addition" to generation("2", listOf(notes(columns = baseColumns + column("subtitle", "text", true)))),
            "non-null addition" to generation("2", listOf(notes(columns = baseColumns + column("rank", "int64", false)))),
            "old-column omission" to generation("2", listOf(notes(columns = listOf(baseColumns.first())))),
            "replacement columns" to generation(
                "2",
                listOf(notes(columns = listOf(baseColumns.first(), column("body", "text", true)))),
            ),
            "table addition" to generation("2", listOf(notes(), parent)),
            "nullable foreign key between retained tables" to generation(
                "2",
                listOf(parent, childWithNullableParent),
            ),
            "new parent plus nullable child foreign key" to generation(
                "2",
                listOf(
                    notes(
                        columns = baseColumns + column("parent_id", "uuid", true),
                        foreignKeys = listOf(foreignKey(childColumn = "parent_id")),
                    ),
                    parent,
                ),
            ),
        ).forEach { (name, target) ->
            val installed = when (name) {
                "nullable foreign key between retained tables" -> generation("1", listOf(parent, child))
                else -> old
            }
            requireSchemaEvolutionCompatibility(installed, target)
            assertTrue(name.isNotBlank())
        }
    }

    @Test
    fun `schema evolution compatibility rejects forbidden deltas in deterministic order`() {
        val old = generation("1", listOf(notes()))
        val parent = table("parents")
        val child = table(
            name = "children",
            columns = listOf(column("id", "uuid", false), column("parent_id", "uuid", true)),
        )
        val existingForeignKeyChild = child.copy(foreignKeys = listOf(foreignKey()))
        val oldWithForeignKey = generation("1", listOf(parent, existingForeignKeyChild))
        data class Rejection(
            val name: String,
            val installed: SyncGeneration,
            val target: SyncGeneration,
            val reason: String,
        )

        listOf(
            Rejection("table omission", generation("1", listOf(notes(), parent)), generation("2", listOf(notes())), "table_omission"),
            Rejection(
                "key change",
                old,
                generation("2", listOf(notes(
                        key = "alternate_id",
                        columns = baseColumns + column("alternate_id", "uuid", false),
                    ))),
                "sync_key",
            ),
            Rejection(
                "shared codec before nullability",
                old,
                generation("2", listOf(notes(columns = listOf(baseColumns.first(), column("title", "json", true))))),
                "shared_wire_type",
            ),
            Rejection(
                "shared nullability",
                old,
                generation("2", listOf(notes(columns = listOf(baseColumns.first(), column("title", "text", true))))),
                "shared_nullability",
            ),
            Rejection(
                "existing foreign key removal",
                oldWithForeignKey,
                generation("2", listOf(parent, child)),
                "old_foreign_key",
            ),
            Rejection(
                "existing foreign key retarget",
                oldWithForeignKey,
                generation(
                    "2",
                    listOf(
                        parent,
                        table("other_parent"),
                        child.copy(foreignKeys = listOf(foreignKey().copy(referencedTable = "other_parent"))),
                    ),
                ),
                "old_foreign_key",
            ),
            Rejection(
                "existing foreign key action change",
                oldWithForeignKey,
                generation(
                    "2",
                    listOf(parent, child.copy(foreignKeys = listOf(foreignKey().copy(onDelete = "cascade")))),
                ),
                "old_foreign_key",
            ),
            Rejection(
                "foreign key on old-visible child column",
                generation("1", listOf(parent, child)),
                generation("2", listOf(parent, child.copy(foreignKeys = listOf(foreignKey())))),
                "new_foreign_key_old_child",
            ),
            Rejection(
                "non-null added foreign key child",
                generation("1", listOf(parent, table("children"))),
                generation(
                    "2",
                    listOf(
                        parent,
                        table(
                            "children",
                            columns = listOf(column("id", "uuid", false), column("parent_id", "uuid", false)),
                            foreignKeys = listOf(foreignKey()),
                        ),
                    ),
                ),
                "new_foreign_key_nullability",
            ),
            Rejection(
                "composite added foreign key",
                generation("1", listOf(parent, table("children"))),
                generation(
                    "2",
                    listOf(
                        parent,
                        table(
                            "children",
                            columns = listOf(
                                column("id", "uuid", false),
                                column("parent_id", "uuid", true),
                                column("parent_id_2", "uuid", true),
                            ),
                            foreignKeys = listOf(
                                foreignKey().copy(
                                    columns = listOf("parent_id", "parent_id_2"),
                                    referencedColumns = listOf("id", "id"),
                                ),
                            ),
                        ),
                    ),
                ),
                "new_foreign_key_shape",
            ),
            Rejection(
                "added foreign key missing parent",
                old,
                generation(
                    "2",
                    listOf(notes(
                        columns = baseColumns + column("parent_id", "uuid", true),
                        foreignKeys = listOf(foreignKey()),
                    )),
                ),
                "new_foreign_key_endpoint",
            ),
            Rejection(
                "added foreign key wire mismatch",
                generation("1", listOf(parent, table("children"))),
                generation(
                    "2",
                    listOf(
                        parent,
                        table(
                            "children",
                            columns = listOf(column("id", "uuid", false), column("parent_id", "text", true)),
                            foreignKeys = listOf(foreignKey()),
                        ),
                    ),
                ),
                "new_foreign_key_wire_type",
            ),
            Rejection(
                "unsupported added foreign key action",
                generation("1", listOf(parent, table("children"))),
                generation(
                    "2",
                    listOf(
                        parent,
                        table(
                            "children",
                            columns = listOf(column("id", "uuid", false), column("parent_id", "uuid", true)),
                            foreignKeys = listOf(foreignKey().copy(onDelete = "set_default")),
                        ),
                    ),
                ),
                "new_foreign_key_action",
            ),
            Rejection(
                "old key column omission",
                old,
                generation("2", listOf(notes(columns = listOf(baseColumns.last())))),
                "old_only_key",
            ),
            Rejection(
                "old foreign key endpoint omission",
                oldWithForeignKey,
                generation(
                    "2",
                    listOf(
                        parent,
                        existingForeignKeyChild.copy(
                            columns = existingForeignKeyChild.columns.filterNot { it.name == "parent_id" },
                        ),
                    ),
                ),
                "old_only_foreign_key",
            ),
        ).forEach { scenario ->
            val failure = assertFailsWith<OversqliteUpgradePlanMismatchException>(scenario.name) {
                requireSchemaEvolutionCompatibility(scenario.installed, scenario.target)
            }
            assertEquals("schema_evolution_compatibility_${scenario.reason}", failure.message, scenario.name)
        }
    }

    @Test
    fun schemaEvolutionFixtureOwnsEveryMatrixDecisionAndTheKmpRuntimeAdapter() {
        val fixture = Json.parseToJsonElement(
            oversqliteContractFixture("schema-evolution/compatibility/v1.json").toFile().readText(),
        ).jsonObject
        assertEquals("sqlitenow-schema-evolution-compatibility/v1", fixture.getValue("format").jsonPrimitive.content)
        assertEquals(
            (1..30).map { "M%02d".format(it) }.toSet(),
            fixture.getValue("cases").jsonArray.map { it.jsonObject.getValue("id").jsonPrimitive.content }.toSet(),
        )
        val root = oversqliteContractFixture("").parent
        fixture.getValue("adapters").jsonObject.values.forEach { adapter ->
            val owner = adapter.jsonObject
            val source = root.resolve(owner.getValue("source").jsonPrimitive.content)
            assertTrue(Files.isRegularFile(source), "missing adapter source $source")
            assertTrue(source.toFile().readText().contains(owner.getValue("test").jsonPrimitive.content))
        }
    }

    private fun generation(id: String, tables: List<GenerationTable>): SyncGeneration {
        val provisional = SyncGeneration(id = id, fingerprint = "0".repeat(64), tables = tables)
        return provisional.copy(fingerprint = provisional.computeFingerprint())
    }

    private fun notes(
        key: String = "id",
        columns: List<GenerationColumn> = baseColumns,
        foreignKeys: List<GenerationForeignKey> = emptyList(),
    ): GenerationTable = GenerationTable(
        schema = "business",
        table = "notes",
        syncKeyColumns = listOf(key),
        columns = columns,
        foreignKeys = foreignKeys,
    )

    private fun table(
        name: String,
        columns: List<GenerationColumn> = listOf(column("id", "uuid", false)),
        foreignKeys: List<GenerationForeignKey> = emptyList(),
    ) = GenerationTable("business", name, listOf("id"), columns, foreignKeys)

    private fun foreignKey(childColumn: String = "parent_id") = GenerationForeignKey(
        columns = listOf(childColumn),
        referencedSchema = "business",
        referencedTable = "parents",
        referencedColumns = listOf("id"),
        onDelete = "set_null",
        onUpdate = "cascade",
    )

    private fun column(name: String, wireType: String, nullable: Boolean) =
        GenerationColumn(name = name, wireType = wireType, nullable = nullable)

    private companion object {
        val baseColumns = listOf(
            GenerationColumn(name = "id", wireType = "uuid", nullable = false),
            GenerationColumn(name = "title", wireType = "text", nullable = false),
        )
    }
}
