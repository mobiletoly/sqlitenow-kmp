package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.context.TypeMapping
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementUtils
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/**
 * Service responsible for generating parameter binding code for SQL statements.
 * Centralizes the logic for handling nullable parameters, adapter parameters, and regular parameters.
 */
class ParameterBinding(
    private val columnLookup: ColumnLookup,
    private val typeMapping: TypeMapping,
    private val dataStructCodeGenerator: DataStructCodeGenerator,
    private val debug: Boolean = false,
) {

    /**
     * Generates parameter binding code for a single parameter.
     * Handles both adapter and regular parameters with proper null checking.
     *
     * @param fnBld The function builder to add statements to
     * @param paramName The SQL parameter name
     * @param paramIndex The parameter index (0-based)
     * @param propertyName The Kotlin property name
     * @param statement The statement containing the parameter
     * @param namespace The namespace for type resolution
     * @param className The class name for type resolution
     * @param processedAdapterVars Map to track processed adapter variables
     */
    fun generateParameterBinding(
        fnBld: FunSpec.Builder,
        paramName: String,
        paramIndex: Int,
        propertyName: String,
        statement: AnnotatedStatement,
        namespace: String,
        className: String,
        processedAdapterVars: MutableMap<String, String>
    ) {
        if (needsAdapter(statement, paramName)) {
            generateAdapterParameterBinding(
                fnBld, paramName, paramIndex, propertyName, statement, processedAdapterVars
            )
        } else {
            generateRegularParameterBinding(
                fnBld, paramName, paramIndex, propertyName, statement, namespace, className
            )
        }
    }

    /**
     * Generates binding code for parameters that need adapter functions.
     * Handles adapter variable creation and null checking.
     */
    private fun generateAdapterParameterBinding(
        fnBld: FunSpec.Builder,
        paramName: String,
        paramIndex: Int,
        propertyName: String,
        statement: AnnotatedStatement,
        processedAdapterVars: MutableMap<String, String>
    ) {
        // Use actual column name for adapter function name (ignore property name customizations)
        val column = columnLookup.findColumnForParameter(statement, paramName)!!
        val columnName = PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(column.src.name)
        val adapterParamName = getInputAdapterFunctionName(columnName)

        val underlyingType = getUnderlyingTypeForParameter(statement, paramName)
        val isNullable = columnLookup.isParameterNullable(statement, paramName)

        val tempVarName = if (processedAdapterVars.containsKey(propertyName)) {
            processedAdapterVars[propertyName]!!
        } else {
            processedAdapterVars[propertyName] = propertyName
            fnBld.addStatement("val %L = %L(params.%L)", propertyName, adapterParamName, propertyName)
            propertyName
        }

        if (isNullable) {
            generateNullableBinding(
                fnBld = fnBld,
                paramIndex = paramIndex,
                nullableExpr = tempVarName,
                valueExpr = tempVarName,
                paramNameForLog = paramName
            ) {
                typeMapping.getBindingCall(underlyingType.copy(nullable = false), paramIndex, tempVarName)
            }
        } else {
            val bindingCall = typeMapping.getBindingCall(underlyingType, paramIndex, tempVarName)
            fnBld.addStatement(bindingCall)
            if (debug) {
                fnBld.addStatement(
                    "if (__seenParams.add(%S)) __paramsLog.add(%S + sqliteNowPreview(%L) + %S)",
                    paramName, "$paramName={", tempVarName, "}"
                )
            }
        }
    }

    /**
     * Generates binding code for regular parameters (no adapter).
     * Handles type resolution and null checking.
     */
    private fun generateRegularParameterBinding(
        fnBld: FunSpec.Builder,
        paramName: String,
        paramIndex: Int,
        propertyName: String,
        statement: AnnotatedStatement,
        namespace: String,
        className: String
    ) {
        val isParameterNullable = columnLookup.isParameterNullable(statement, paramName)
        val actualType = getActualParameterType(namespace, className, propertyName)
        val valueExpr = if (actualType == "Collection") "Json.encodeToString(params.$propertyName)" else "params.$propertyName"

        if (isParameterNullable) {
            generateNullableBinding(
                fnBld = fnBld,
                paramIndex = paramIndex,
                nullableExpr = "params.$propertyName",
                valueExpr = valueExpr,
                paramNameForLog = paramName
            ) {
                getBindingMethodCall(paramIndex, propertyName, actualType)
            }
        } else {
            fnBld.addStatement(getBindingMethodCall(paramIndex, propertyName, actualType))
            if (debug) {
                fnBld.addStatement(
                    "if (__seenParams.add(%S)) __paramsLog.add(%S + sqliteNowPreview(%L) + %S)",
                    paramName, "$paramName={", valueExpr, "}"
                )
            }
        }
    }

    /**
     * Generates the standard nullable parameter binding pattern.
     * This centralizes the duplicated null checking logic.
     */
    private fun generateNullableBinding(
        fnBld: FunSpec.Builder,
        paramIndex: Int,
        nullableExpr: String,
        valueExpr: String,
        paramNameForLog: String,
        bindingCallGenerator: () -> String
    ) {
        fnBld.addStatement("if (%L == null) {", nullableExpr)
        fnBld.addStatement("  statement.bindNull(%L)", paramIndex + 1)
        if (debug) {
            fnBld.addStatement(
                "if (__seenParams.add(%S)) __paramsLog.add(%S)",
                paramNameForLog,
                "$paramNameForLog={null}"
            )
        }
        fnBld.addStatement("} else {")
        fnBld.addStatement("  %L", bindingCallGenerator())
        if (debug) {
            fnBld.addStatement(
                "if (__seenParams.add(%S)) __paramsLog.add(%S + sqliteNowPreview(%L) + %S)",
                paramNameForLog, "$paramNameForLog={", valueExpr, "}"
            )
        }
        fnBld.addStatement("}")
    }

    /**
     * Helper function to check if a parameter needs an adapter.
     */
    private fun needsAdapter(statement: AnnotatedStatement, paramName: String): Boolean {
        val column = columnLookup.findColumnForParameter(statement, paramName) ?: return false
        return column.annotations.containsKey(AnnotationConstants.ADAPTER)
    }

    /**
     * Helper function to get the underlying SQLite type for a parameter that needs an adapter.
     */
    private fun getUnderlyingTypeForParameter(statement: AnnotatedStatement, paramName: String): TypeName {
        val column = columnLookup.findColumnForParameter(statement, paramName)
            ?: return ClassName("kotlin", "String")

        return SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(column.src.dataType)
    }

    /**
     * Generates an adapter function name for input parameters.
     */
    private fun getInputAdapterFunctionName(propertyName: String): String {
        return "${propertyName}ToSqlValue"
    }

    /**
     * Helper function to get the actual parameter type from the generated Params data class.
     * This accesses the DataStructCodeGenerator to determine the real Kotlin type.
     */
    private fun getActualParameterType(
        namespace: String,
        className: String,
        propertyName: String,
    ): String {
        val statements = dataStructCodeGenerator.nsWithStatements[namespace] ?: return "String"

        // Find the statement that matches the className
        val matchingStatement = statements.find { stmt ->
            val stmtClassName = stmt.getDataClassName()
            stmtClassName == className
        } ?: return "String"

        val namedParameters = StatementUtils.getNamedParameters(matchingStatement)

        // Use the same type inference logic as DataStructCodeGenerator
        val paramName = namedParameters.find { param ->
            val convertedName = matchingStatement.annotations.propertyNameGenerator.convertToPropertyName(param)
            convertedName == propertyName
        } ?: return "String"

        // Get the inferred type using the same logic as DataStructCodeGenerator
        val inferredType = dataStructCodeGenerator.inferParameterType(paramName, matchingStatement)

        val typeString = inferredType.toString()
        if (typeString.startsWith("kotlin.collections.Collection<")) {
            return "Collection"
        }

        // Extract the base type name, handling both nullable and non-nullable types
        val baseTypeName = typeString.removePrefix("kotlin.").removeSuffix("?")

        return when (baseTypeName) {
            "Int", "Long", "Double", "Float", "Boolean", "ByteArray" -> baseTypeName
            else -> "String"
        }
    }

    /**
     * Helper function to determine the appropriate binding method based on parameter type.
     */
    private fun getBindingMethodCall(paramIndex: Int, propertyName: String, propertyType: String): String {
        // Handle Collection types specially - they need JSON encoding
        if (propertyType == "Collection") {
            return typeMapping.getCollectionBindingCall(paramIndex, propertyName)
        }

        val isStd = (propertyType in SqliteTypeToKotlinCodeConverter.Companion.KOTLIN_STDLIB_TYPES)
        val kotlinType = ClassName(
            "kotlin",
            if (isStd) propertyType else "String"
        )
        return typeMapping.getBindingCall(kotlinType, paramIndex, "params.$propertyName")
    }
}
