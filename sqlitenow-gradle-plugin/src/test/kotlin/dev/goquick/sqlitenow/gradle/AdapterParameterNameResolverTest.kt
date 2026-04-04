package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.ClassName
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterNameResolver
import kotlin.test.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class AdapterParameterNameResolverTest {

    private val resolver = AdapterParameterNameResolver()

    @Test
    @DisplayName("canonicalizeAdapterNameForNamespace removes redundant namespace prefixes")
    fun canonicalizeAdapterNameForNamespaceRemovesNamespacePrefix() {
        assertEquals(
            "sqlValueToBirthDate",
            resolver.canonicalizeAdapterNameForNamespace("person", "sqlValueToPersonBirthDate"),
        )
    }

    @Test
    @DisplayName("normalizeAliasNoiseForNamespace compresses duplicate alias tokens")
    fun normalizeAliasNoiseForNamespaceCompressesDuplicateTokens() {
        assertEquals(
            "sqlValueToAddressType",
            resolver.normalizeAliasNoiseForNamespace("sqlValueToAddressAddressType"),
        )
        assertEquals(
            "AddressTypeToSqlValue",
            resolver.normalizeAliasNoiseForNamespace("AddressAddressTypeToSqlValue"),
        )
    }

    @Test
    @DisplayName("chooseAdapterParamNames keeps canonical names stable and disambiguates by signature")
    fun chooseAdapterParamNamesDisambiguatesBySignature() {
        val stringType = ClassName("kotlin", "String")
        val intType = ClassName("kotlin", "Int")
        val stringConfigs = listOf(
            paramConfig(
                adapterFunctionName = "sqlValueToPersonBirthDate",
                inputType = stringType,
                outputType = stringType,
                providerNamespace = "person",
            ),
            paramConfig(
                adapterFunctionName = "sqlValueToPersonBirthDate",
                inputType = stringType,
                outputType = stringType,
                providerNamespace = "person",
            ),
        )

        val stringNames = resolver.chooseAdapterParamNames(stringConfigs)
        assertEquals(
            setOf("sqlValueToBirthDate"),
            stringNames.values.toSet(),
        )

        val disambiguated = resolver.chooseAdapterParamNames(
            listOf(
                paramConfig(
                    adapterFunctionName = "sqlValueToPersonBirthDate",
                    inputType = stringType,
                    outputType = stringType,
                    providerNamespace = "person",
                ),
                paramConfig(
                    adapterFunctionName = "sqlValueToBirthDate",
                    inputType = intType,
                    outputType = stringType,
                    providerNamespace = "person",
                ),
            ),
        )

        assertEquals(
            listOf(
                "sqlValueToBirthDateForStringToString",
                "sqlValueToBirthDateForIntToString",
            ),
            disambiguated.values.toList(),
        )
    }

    private fun paramConfig(
        adapterFunctionName: String,
        inputType: com.squareup.kotlinpoet.TypeName,
        outputType: com.squareup.kotlinpoet.TypeName,
        providerNamespace: String?,
    ) = AdapterConfig.ParamConfig(
        paramName = adapterFunctionName,
        adapterFunctionName = adapterFunctionName,
        inputType = inputType,
        outputType = outputType,
        isNullable = outputType.isNullable,
        providerNamespace = providerNamespace,
        kind = AdapterConfig.AdapterKind.RESULT_FIELD,
    )
}
