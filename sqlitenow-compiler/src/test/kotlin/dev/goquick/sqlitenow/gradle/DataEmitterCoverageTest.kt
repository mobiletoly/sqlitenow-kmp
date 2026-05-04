package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.generator.data.DataStructJoinedEmitter
import dev.goquick.sqlitenow.gradle.generator.data.DataStructPropertyEmitter
import dev.goquick.sqlitenow.gradle.generator.data.DataStructResultEmitter
import dev.goquick.sqlitenow.gradle.generator.data.DataStructResultFileEmitter
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class DataEmitterCoverageTest {

    @Test
    @DisplayName("DataStructJoinedEmitter makes joined aliases nullable unless overridden with notNull")
    fun dataStructJoinedEmitterAdjustsJoinNullability() {
        val statement = annotatedSelectStatement(
            name = "SelectJoined",
            src = selectStatement(
                fields = listOf(
                    fieldSource("id", "p", originalColumnName = "id", dataType = "INTEGER"),
                    fieldSource("city", "a", originalColumnName = "city"),
                    fieldSource("forced_city", "a", originalColumnName = "city"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person", "a" to "address"),
            ),
            fields = listOf(
                regularField("id", "p", originalColumnName = "id", dataType = "INTEGER"),
                regularField("city", "a", originalColumnName = "city"),
                AnnotatedSelectStatement.Field(
                    src = fieldSource("forced_city", "a", originalColumnName = "city"),
                    annotations = FieldAnnotationOverrides(
                        propertyName = null,
                        propertyType = null,
                        notNull = true,
                        adapter = null,
                    ),
                ),
            ),
        )
        val ctx = generatorContext(selectStatements = listOf(statement))
        val emitter = DataStructJoinedEmitter(ctx)

        val rendered = emitter.generateJoinedDataClass(
            joinedClassName = "SelectJoined_Joined",
            fields = statement.fields,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
        ).toString()

        assertTrue(rendered.contains("city: kotlin.String?"))
        assertTrue(rendered.contains("forcedCity: kotlin.String"))
    }

    @Test
    @DisplayName("DataStructResultEmitter uses provided shared result names and preserves property shapes")
    fun dataStructResultEmitterUsesProvidedClassNames() {
        val statement = annotatedSelectStatement(
            name = "SelectSummary",
            src = selectStatement(
                fields = listOf(
                    fieldSource("person_id", "p", originalColumnName = "id", dataType = "INTEGER"),
                    fieldSource("birth_date", "p", originalColumnName = "birth_date"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(
                regularField("person_id", "p", originalColumnName = "id", dataType = "INTEGER"),
                regularField(
                    "birth_date",
                    "p",
                    originalColumnName = "birth_date",
                    propertyName = "customBirthDate",
                ),
            ),
            queryResult = "SharedPersonRow",
        )
        val ctx = generatorContext(selectStatements = listOf(statement))
        val emitter = DataStructResultEmitter(ctx, DataStructPropertyEmitter())

        val type = emitter.generateSelectResult(statement, className = "SharedPersonRow")
        val rendered = type.toString()

        assertEquals("SharedPersonRow", type.name)
        assertTrue(rendered.contains("val personId"))
        assertTrue(rendered.contains("val customBirthDate"))
    }

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("DataStructResultFileEmitter writes imports, file opt-ins, and array-safe equality")
    fun dataStructResultFileEmitterWritesImportsAndArraySafeEquality() {
        val statement = annotatedSelectStatement(
            name = "SelectBinaryPayload",
            src = selectStatement(
                fields = listOf(
                    fieldSource("created_at", "p", originalColumnName = "created_at"),
                    fieldSource("payload", "p", originalColumnName = "payload", dataType = "BLOB"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(
                regularField(
                    "created_at",
                    "p",
                    originalColumnName = "created_at",
                    propertyType = "kotlinx.datetime.LocalDateTime",
                ),
                regularField(
                    "payload",
                    "p",
                    originalColumnName = "payload",
                    dataType = "BLOB",
                ),
            ),
            queryResult = "PayloadRow",
        )
        val outputDir = tempDir.resolve("generated").toFile()
        val ctx = generatorContext(selectStatements = listOf(statement))
        val emitter = DataStructResultFileEmitter(
            generatorContext = ctx,
            joinedEmitter = DataStructJoinedEmitter(ctx),
            resultEmitter = DataStructResultEmitter(ctx, DataStructPropertyEmitter()),
            outputDir = outputDir,
        )

        emitter.writeSelectResultFile(statement, namespace = "person", packageName = "fixture.db")

        val generated = outputDir.resolve("fixture/db/PayloadRow.kt").readText()
        assertTrue(generated.contains("import kotlinx.datetime.LocalDateTime"))
        assertTrue(generated.contains("@file:Suppress(\"UNNECESSARY_NOT_NULL_ASSERTION\")"))
        assertTrue(generated.contains("ExperimentalUuidApi::class"))
        assertTrue(generated.contains("override fun equals("))
        assertTrue(generated.contains("contentEquals"))
        assertTrue(generated.contains("contentHashCode"))
    }
}
