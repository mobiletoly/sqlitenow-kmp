package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.database.DatabaseAdapterPlanner
import dev.goquick.sqlitenow.gradle.database.UniqueAdapter
import dev.goquick.sqlitenow.gradle.processing.SharedResultManager
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import kotlin.test.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class DatabaseAdapterPlannerTest {

    @Test
    @DisplayName("adapter provider tie prefers declaring namespace")
    fun adapterProviderTiePrefersDeclaringNamespace() {
        val stringType = ClassName("kotlin", "String")
        val planner = DatabaseAdapterPlanner(
            nsWithStatements = emptyMap(),
            adapterConfig = AdapterConfig(
                columnLookup = ColumnLookup(emptyList(), emptyList()),
                createTableStatements = emptyList(),
                createViewStatements = emptyList(),
                packageName = "fixture.db",
            ),
            tableLookup = CaseInsensitiveMap(emptyList()),
            sharedResultManager = SharedResultManager(),
        )
        val adaptersByNamespace = mapOf(
            "alpha" to listOf(
                UniqueAdapter(
                    functionName = "sqlValueToBirthDate",
                    inputType = stringType,
                    outputType = stringType,
                    isNullable = false,
                    providerNamespace = "person",
                )
            ),
            "person" to listOf(
                UniqueAdapter(
                    functionName = "sqlValueToBirthDate",
                    inputType = stringType,
                    outputType = stringType,
                    isNullable = false,
                    providerNamespace = "person",
                )
            ),
        )

        assertEquals(
            "person",
            planner.computeBestProviders(adaptersByNamespace).getValue("sqlValueToBirthDate"),
        )
        assertEquals(
            "person",
            planner.findBestProviderByName("sqlValueToBirthDate", adaptersByNamespace)?.first,
        )
    }

    @Test
    @DisplayName("signature-aware provider selection preserves overloaded adapter names")
    fun signatureAwareProviderSelectionPreservesOverloadedAdapterNames() {
        val stringType = ClassName("kotlin", "String")
        val intType = ClassName("kotlin", "Int")
        val planner = DatabaseAdapterPlanner(
            nsWithStatements = emptyMap(),
            adapterConfig = AdapterConfig(
                columnLookup = ColumnLookup(emptyList(), emptyList()),
                createTableStatements = emptyList(),
                createViewStatements = emptyList(),
                packageName = "fixture.db",
            ),
            tableLookup = CaseInsensitiveMap(emptyList()),
            sharedResultManager = SharedResultManager(),
        )
        val adaptersByNamespace = mapOf(
            "doc" to listOf(
                UniqueAdapter(
                    functionName = "statusToSqlValue",
                    inputType = stringType,
                    outputType = stringType,
                    isNullable = false,
                    providerNamespace = "doc",
                ),
                UniqueAdapter(
                    functionName = "statusToSqlValue",
                    inputType = intType,
                    outputType = stringType,
                    isNullable = false,
                    providerNamespace = "doc",
                ),
            ),
            "task" to listOf(
                UniqueAdapter(
                    functionName = "statusToSqlValue",
                    inputType = stringType,
                    outputType = stringType,
                    isNullable = false,
                    providerNamespace = "task",
                )
            ),
        )

        val byName = planner.computeBestProviders(adaptersByNamespace)
        assertEquals(1, byName.size)

        val bySignature = planner.computeBestProvidersBySignature(adaptersByNamespace)
        assertEquals(2, bySignature.size)
        assertEquals("doc", bySignature.getValue("statusToSqlValue__kotlin.Int__kotlin.String"))
        assertEquals("doc", bySignature.getValue("statusToSqlValue__kotlin.String__kotlin.String"))
    }
}
