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
package dev.goquick.sqlitenow.gradle.swift

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.model.ResultMappingPlan
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants

internal class SwiftProductNamespaceEmitter(
    private val databaseName: String,
    private val context: GeneratorContext,
    private val model: SwiftProductGenerationModel,
) {
    fun emit(
        writer: SwiftWriter,
        namespace: String,
        emittedResultNames: MutableSet<String>,
    ) {
        with(writer) {
            emitNamespaceResultTypes(namespace, emittedResultNames)
            emitNamespaceParamsTypes(namespace)
            emitNamespaceType(namespace)
            emitTransactionNamespaceType(namespace)
        }
    }

    private fun SwiftWriter.emitNamespaceResultTypes(
        namespace: String,
        emittedResultNames: MutableSet<String>,
    ) {
        model.namespaceResults(namespace).forEach { result ->
            if (emittedResultNames.add(result.name)) {
                emitResultType(result)
            }
        }
    }

    private fun SwiftWriter.emitResultType(result: SwiftProductResult) {
        val conformances = buildList {
            if (result.fields.all { it.swiftType.supportsSwiftEquatable() }) add("Equatable")
            add(if (result.fields.all { it.swiftType.supportsSwiftSendable() }) "Sendable" else "@unchecked Sendable")
        }
        val suffix = if (conformances.isEmpty()) "" else ": ${conformances.joinToString(", ")}"
        line("public struct ${result.name}$suffix {")
        indent {
            result.fields.forEach { field ->
                line("public let ${field.propertyName.swiftIdentifier()}: ${field.swiftType}")
            }
            line()
            line("public init(")
            indent {
                result.fields.forEachIndexed { index, field ->
                    val trailing = if (index == result.fields.lastIndex) "" else ","
                    line("${field.propertyName.swiftIdentifier()}: ${field.swiftType}$trailing")
                }
            }
            line(") {")
            indent {
                result.fields.forEach { field ->
                    line("self.${field.propertyName.swiftIdentifier()} = ${field.propertyName.swiftIdentifier()}")
                }
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitNamespaceParamsTypes(namespace: String) {
        context.nsWithStatements[namespace].orEmpty().forEach { statement ->
            if (
                (statement is AnnotatedSelectStatement || statement is AnnotatedExecuteStatement) &&
                model.parameterDescriptors(namespace, statement).isNotEmpty()
            ) {
                emitParamsType(namespace, statement)
            }
        }
    }

    private fun SwiftWriter.emitParamsType(namespace: String, statement: AnnotatedStatement) {
        val params = model.parameterDescriptors(namespace, statement)
        if (params.isEmpty()) return
        val paramsName = statement.swiftParamsName(namespace)
        val conformances = buildList {
            if (params.all { it.swiftType.supportsSwiftEquatable() }) add("Equatable")
            add(if (params.all { it.swiftType.supportsSwiftSendable() }) "Sendable" else "@unchecked Sendable")
        }
        val suffix = if (conformances.isEmpty()) "" else ": ${conformances.joinToString(", ")}"
        line("public struct $paramsName$suffix {")
        indent {
            params.forEach { param ->
                line("public let ${param.propertyName.swiftIdentifier()}: ${param.swiftType}")
            }
            line()
            line("public init(")
            indent {
                params.forEachIndexed { index, param ->
                    val trailing = if (index == params.lastIndex) "" else ","
                    line("${param.propertyName.swiftIdentifier()}: ${param.swiftType}$trailing")
                }
            }
            line(") {")
            indent {
                params.forEach { param ->
                    line("self.${param.propertyName.swiftIdentifier()} = ${param.propertyName.swiftIdentifier()}")
                }
            }
            line("}")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitTransactionNamespaceType(namespace: String) {
        val executeStatements = context.nsWithStatements[namespace].orEmpty()
            .filterIsInstance<AnnotatedExecuteStatement>()
            .filterNot { it.hasReturningClause() }
        line("public final class ${namespace.swiftNamespaceTypeName("TransactionQueries")} {")
        indent {
            line("private let batch: SQLiteNowCoreRuntimeMutationBatch")
            line("private let adapters: ${databaseName}Adapters")
            line()
            line("internal init(batch: SQLiteNowCoreRuntimeMutationBatch, adapters: ${databaseName}Adapters) {")
            indent {
                line("self.batch = batch")
                line("self.adapters = adapters")
            }
            line("}")
            executeStatements.forEach { statement ->
                line()
                emitTransactionExecuteMethod(namespace, statement)
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitTransactionExecuteMethod(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ) {
        val params = model.parameterDescriptors(namespace, statement)
        val signature = if (params.isEmpty()) {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}() throws"
        } else {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}(_ params: ${statement.swiftParamsName(namespace)}) throws"
        }
        line("$signature {")
        indent {
            line("let bindValues = try Self.${model.statementIdentifier(statement)}BindValues(${model.bindValuesArguments(params)})")
            line("batch.add(")
            indent {
                line("sql: ${namespace.swiftNamespaceTypeName("Queries")}.${model.statementIdentifier(statement)}Sql,")
                line("bindValues: bindValues,")
                line("affectedTables: ${namespace.swiftNamespaceTypeName("Queries")}.${model.statementIdentifier(statement)}AffectedTables")
            }
            line(")")
        }
        line("}")
        line()
        emitBindValuesFunction(namespace, statement)
    }

    private fun SwiftWriter.emitNamespaceType(namespace: String) {
        val typeName = namespace.swiftNamespaceTypeName("Queries")
        line("public final class $typeName: @unchecked Sendable {")
        indent {
            line("private let runtime: SQLiteNowCoreRuntimeDatabase")
            line("private let adapters: ${databaseName}Adapters")
            line()
            line("internal init(runtime: SQLiteNowCoreRuntimeDatabase, adapters: ${databaseName}Adapters) {")
            indent {
                line("self.runtime = runtime")
                line("self.adapters = adapters")
            }
            line("}")
            context.nsWithStatements[namespace].orEmpty().forEach { statement ->
                when (statement) {
                    is AnnotatedSelectStatement -> {
                        line()
                        emitSelectStatement(namespace, statement)
                    }
                    is AnnotatedExecuteStatement -> {
                        line()
                        if (statement.hasReturningClause()) {
                            emitExecuteReturningStatement(namespace, statement)
                        } else {
                            emitExecuteStatement(namespace, statement)
                        }
                    }
                    else -> Unit
                }
            }
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitSelectStatement(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ) {
        val resultName = model.resultClassName(namespace, statement)
        val params = model.parameterDescriptors(namespace, statement)
        val resultFields = model.resultFields(namespace, statement)
        val columnFields = model.selectColumnFields(namespace, statement)
        emitStatementConstants(statement, columnFields)
        line()
        val signature = if (params.isEmpty()) {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}() -> SQLiteNowSelectQuery<$resultName>"
        } else {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}(_ params: ${statement.swiftParamsName(namespace)}) -> SQLiteNowSelectQuery<$resultName>"
        }
        line("$signature {")
        indent {
            line("SQLiteNowSelectQuery(")
            indent {
                line("list: { [runtime, adapters] in")
                indent {
                    line("try await Self.${model.statementIdentifier(statement)}LoadRows(${model.streamArguments(params)})")
                }
                line("},")
                line("one: { [runtime, adapters] in")
                indent {
                    line("let rows = try await Self.${model.statementIdentifier(statement)}LoadRows(${model.streamArguments(params)})")
                    line("return try sqliteNowRequireOne(rows, resultName: ${resultName.toSwiftStringLiteral()})")
                }
                line("},")
                line("oneOrNull: { [runtime, adapters] in")
                indent {
                    line("let rows = try await Self.${model.statementIdentifier(statement)}LoadRows(${model.streamArguments(params)})")
                    line("return try sqliteNowRequireOneOrNull(rows, resultName: ${resultName.toSwiftStringLiteral()})")
                }
                line("},")
                line("stream: { [runtime, adapters] in")
                indent {
                    line("Self.${model.statementIdentifier(statement)}Stream(${model.streamArguments(params)})")
                }
                line("}")
            }
            line(")")
        }
        line("}")
        line()
        emitBindValuesFunction(namespace, statement)
        emitLoadRowsFunction(namespace, statement, params, resultName, SwiftLoadRowsOperation.QUERY)
        emitMapRowsFunctions(model.statementIdentifier(statement), resultName, resultFields, columnFields, statement)
        emitStreamFunction(namespace, statement, params, resultName)
    }

    private fun SwiftWriter.emitExecuteStatement(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ) {
        val params = model.parameterDescriptors(namespace, statement)
        emitStatementConstants(statement, emptyList())
        line()
        val signature = if (params.isEmpty()) {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}() async throws"
        } else {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}(_ params: ${statement.swiftParamsName(namespace)}) async throws"
        }
        line("$signature {")
        indent {
            line("let bindValues = try Self.${model.statementIdentifier(statement)}BindValues(${model.bindValuesArguments(params)})")
            line("_ = try await mapRuntimeErrors {")
            indent {
                line("try await runtime.execute(")
                indent {
                    line("sql: Self.${model.statementIdentifier(statement)}Sql,")
                    line("bindValues: bindValues,")
                    line("affectedTables: Self.${model.statementIdentifier(statement)}AffectedTables")
                }
                line(")")
            }
            line("}")
        }
        line("}")
        line()
        emitBindValuesFunction(namespace, statement)
    }

    private fun SwiftWriter.emitExecuteReturningStatement(
        namespace: String,
        statement: AnnotatedExecuteStatement,
    ) {
        val resultName = model.resultClassName(namespace, statement)
        val params = model.parameterDescriptors(namespace, statement)
        val fields = model.returningFields(namespace, statement)
        emitStatementConstants(statement, fields)
        line()
        val signature = if (params.isEmpty()) {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}() -> SQLiteNowExecuteReturningQuery<$resultName>"
        } else {
            "public func ${statement.swiftFunctionName().swiftIdentifier()}(_ params: ${statement.swiftParamsName(namespace)}) -> SQLiteNowExecuteReturningQuery<$resultName>"
        }
        line("$signature {")
        indent {
            line("SQLiteNowExecuteReturningQuery(")
            indent {
                line("list: { [runtime, adapters] in")
                indent {
                    line("try await Self.${model.statementIdentifier(statement)}LoadRows(${model.streamArguments(params)})")
                }
                line("},")
                line("one: { [runtime, adapters] in")
                indent {
                    line("let rows = try await Self.${model.statementIdentifier(statement)}LoadRows(${model.streamArguments(params)})")
                    line("return try sqliteNowRequireOne(rows, resultName: ${resultName.toSwiftStringLiteral()})")
                }
                line("},")
                line("oneOrNull: { [runtime, adapters] in")
                indent {
                    line("let rows = try await Self.${model.statementIdentifier(statement)}LoadRows(${model.streamArguments(params)})")
                    line("return try sqliteNowRequireOneOrNull(rows, resultName: ${resultName.toSwiftStringLiteral()})")
                }
                line("}")
            }
            line(")")
        }
        line("}")
        line()
        emitBindValuesFunction(namespace, statement)
        emitLoadRowsFunction(namespace, statement, params, resultName, SwiftLoadRowsOperation.EXECUTE_RETURNING)
        emitMapRowsFunctions(model.statementIdentifier(statement), resultName, fields, fields)
    }

    private fun SwiftWriter.emitStatementConstants(
        statement: AnnotatedStatement,
        fields: List<SwiftProductField>,
    ) {
        line("internal static let ${model.statementIdentifier(statement)}Sql =")
        emitSwiftMultilineStringLiteral(model.statementSql(statement))
        line("internal static let ${model.statementIdentifier(statement)}AffectedTables: [String] = ${model.affectedTables(statement).toSwiftArrayLiteral()}")
        if (fields.isNotEmpty()) {
            line("private static let ${model.statementIdentifier(statement)}ColumnTypes: [String] = ${fields.map { it.columnType }.toSwiftArrayLiteral()}")
        }
    }

    private fun SwiftWriter.emitBindValuesFunction(
        namespace: String,
        statement: AnnotatedStatement,
    ) {
        val params = model.parameterDescriptors(namespace, statement)
        val paramsParam = if (params.isEmpty()) "" else "_ params: ${statement.swiftParamsName(namespace)}, "
        line("private static func ${model.statementIdentifier(statement)}BindValues(${paramsParam}adapters: ${databaseName}Adapters) throws -> [SQLiteNowCoreRuntimeBindValue] {")
        indent {
            if (params.isEmpty()) {
                line("[]")
            } else {
                line("[")
                indent {
                    statement.srcNamedParameters().forEachIndexed { index, paramName ->
                        val param = params.first { it.sqlName == paramName }
                        val suffix = if (index == statement.srcNamedParameters().lastIndex) "" else ","
                        line("${param.bindExpression("params.${param.propertyName.swiftIdentifier()}")}$suffix")
                    }
                }
                line("]")
            }
        }
        line("}")
    }


    private fun SwiftWriter.emitLoadRowsFunction(
        namespace: String,
        statement: AnnotatedStatement,
        params: List<SwiftProductParameter>,
        resultName: String,
        operation: SwiftLoadRowsOperation,
    ) {
        val statementId = model.statementIdentifier(statement)
        emitLoadRowsFunctionHeader(namespace, statement, params, resultName)
        indent {
            line("let bindValues = try ${statementId}BindValues(${model.bindValuesArguments(params)})")
            line("let rows = try await mapRuntimeErrors {")
            indent {
                val runtimeFunction = when (operation) {
                    SwiftLoadRowsOperation.QUERY -> "query"
                    SwiftLoadRowsOperation.EXECUTE_RETURNING -> "executeReturning"
                }
                line("try await runtime.$runtimeFunction(")
                indent {
                    line("sql: Self.${statementId}Sql,")
                    line("bindValues: bindValues,")
                    if (operation == SwiftLoadRowsOperation.EXECUTE_RETURNING) {
                        line("columnTypes: Self.${statementId}ColumnTypes,")
                        line("affectedTables: Self.${statementId}AffectedTables")
                    } else {
                        line("columnTypes: Self.${statementId}ColumnTypes")
                    }
                }
                line(")")
            }
            line("}")
            line("return try ${statementId}MapRows(rows, adapters: adapters)")
        }
        line("}")
        line()
    }

    private fun SwiftWriter.emitLoadRowsFunctionHeader(
        namespace: String,
        statement: AnnotatedStatement,
        params: List<SwiftProductParameter>,
        resultName: String,
    ) {
        line("private static func ${model.statementIdentifier(statement)}LoadRows(")
        indent {
            if (params.isNotEmpty()) {
                line("_ params: ${statement.swiftParamsName(namespace)},")
            }
            line("runtime: SQLiteNowCoreRuntimeDatabase,")
            line("adapters: ${databaseName}Adapters")
        }
        line(") async throws -> [$resultName] {")
    }

    private fun SwiftWriter.emitMapRowsFunctions(
        statementId: String,
        resultName: String,
        resultFields: List<SwiftProductField>,
        columnFields: List<SwiftProductField>,
        statement: AnnotatedSelectStatement? = null,
    ) {
        val dynamicEntries = statement?.mappingPlan?.includedDynamicEntries.orEmpty()
        val hasDynamicMappings = dynamicEntries.isNotEmpty()
        val collectionGroupField = if (hasDynamicMappings && statement != null) {
            statement.annotations.collectionKey
                ?.let { collectionKey ->
                    columnFields.firstOrNull { it.columnName == collectionKey }
                        ?: error("Product Swift SELECT '${statement.name}' collectionKey '$collectionKey' is not selected.")
                }
        } else {
            null
        }

        line("private static func ${statementId}MapRows(_ rowSet: SQLiteNowCoreRuntimeRowSet, adapters: ${databaseName}Adapters) throws -> [$resultName] {")
        indent {
            if (collectionGroupField != null) {
                line("var groupedRows = [String: [SQLiteNowCoreRuntimeRow]]()")
                line("var orderedKeys = [String]()")
                line("for index in 0..<Int(rowSet.count) {")
                indent {
                    line("let row = rowSet.rowAt(index: Int32(index))")
                    line("let key = try sqliteNowCellKey(row.cellAt(index: ${collectionGroupField.index}), columnType: ${collectionGroupField.columnType.toSwiftStringLiteral()}, column: ${collectionGroupField.columnName.toSwiftStringLiteral()})")
                    line("if groupedRows[key] == nil {")
                    indent {
                        line("groupedRows[key] = []")
                        line("orderedKeys.append(key)")
                    }
                    line("}")
                    line("groupedRows[key]?.append(row)")
                }
                line("}")
                line("return try orderedKeys.map { key in")
                indent {
                    line("let rowsForKey = groupedRows[key] ?? []")
                    line("guard let firstRow = rowsForKey.first else {")
                    indent {
                        line("throw SQLiteNowError.misuse(message: \"Expected at least one $resultName row for grouped key\")")
                    }
                    line("}")
                    line("return try ${statementId}MapRow(firstRow, rows: rowsForKey, adapters: adapters)")
                }
                line("}")
            } else {
                line("var rows = [$resultName]()")
                line("rows.reserveCapacity(Int(rowSet.count))")
                line("for index in 0..<Int(rowSet.count) {")
                indent {
                    if (hasDynamicMappings) {
                        line("let row = rowSet.rowAt(index: Int32(index))")
                        line("rows.append(try ${statementId}MapRow(row, rows: [row], adapters: adapters))")
                    } else {
                        line("rows.append(try ${statementId}MapRow(rowSet.rowAt(index: Int32(index)), adapters: adapters))")
                    }
                }
                line("}")
                line("return rows")
            }
        }
        line("}")
        line()
        if (hasDynamicMappings) {
            line("private static func ${statementId}MapRow(_ row: SQLiteNowCoreRuntimeRow, rows: [SQLiteNowCoreRuntimeRow], adapters: ${databaseName}Adapters) throws -> $resultName {")
        } else {
            line("private static func ${statementId}MapRow(_ row: SQLiteNowCoreRuntimeRow, adapters: ${databaseName}Adapters) throws -> $resultName {")
        }
        indent {
            line("guard Int(row.count) == ${columnFields.size} else {")
            indent {
                line("throw SQLiteNowError.misuse(message: \"Expected ${columnFields.size} $resultName columns, got \\(row.count)\")")
            }
            line("}")
            line("return $resultName(")
            indent {
                resultFields.forEachIndexed { index, field ->
                    val suffix = if (index == resultFields.lastIndex) "" else ","
                    if (field.dynamicFieldName == null) {
                        line("${field.propertyName.swiftIdentifier()}: ${field.readExpression("row.cellAt(index: ${field.index})")}$suffix")
                    } else {
                        line("${field.propertyName.swiftIdentifier()}: try ${model.dynamicFieldHelperName(statementId, field.propertyName)}(rows, adapters: adapters)$suffix")
                    }
                }
            }
            line(")")
        }
        line("}")
        line()
        if (statement != null) {
            dynamicEntries.forEach { entry ->
                emitDynamicFieldMapper(statementId, statement, entry)
            }
        }
    }

    private fun SwiftWriter.emitDynamicFieldMapper(
        statementId: String,
        parentStatement: AnnotatedSelectStatement,
        entry: ResultMappingPlan.DynamicFieldEntry,
    ) {
        val dynamicField = entry.field
        val mapping = parentStatement.mappingPlan.dynamicMappingsByField[dynamicField.src.fieldName]
            ?: error("Product Swift dynamic field '${parentStatement.name}.${dynamicField.src.fieldName}' has no mapping.")
        val targetResultName = model.dynamicFieldElementType(dynamicField)
        val targetStatement = context.findSelectStatementByResultName(targetResultName)
            ?: error("Product Swift dynamic field '${parentStatement.name}.${dynamicField.src.fieldName}' targets unknown result type '$targetResultName'.")
        val targetFields = model.dynamicMappingTargetFields(parentStatement, targetStatement, mapping)
        val payloadCondition = dynamicPayloadCondition(targetFields, rowVariable = "row")
        val helperName = model.dynamicFieldHelperName(statementId, dynamicField.swiftPropertyName(parentStatement))

        when (entry.mappingType) {
            AnnotationConstants.MappingType.COLLECTION -> {
                val collectionKey = dynamicField.annotations.collectionKey
                    ?: error("Product Swift collection dynamic field '${parentStatement.name}.${dynamicField.src.fieldName}' requires collectionKey.")
                val keyField = targetFields.firstOrNull { it.columnName == collectionKey }
                    ?: error("Product Swift collection key '$collectionKey' is not selected by '${parentStatement.name}'.")
                line("private static func $helperName(_ rows: [SQLiteNowCoreRuntimeRow], adapters: ${databaseName}Adapters) throws -> [$targetResultName] {")
                indent {
                    line("var result = [$targetResultName]()")
                    line("var seenKeys = Set<String>()")
                    line("for row in rows {")
                    indent {
                        line("guard $payloadCondition else { continue }")
                        line("let key = try sqliteNowCellKey(row.cellAt(index: ${keyField.index}), columnType: ${keyField.columnType.toSwiftStringLiteral()}, column: ${keyField.columnName.toSwiftStringLiteral()})")
                        line("guard seenKeys.insert(key).inserted else { continue }")
                        emitDynamicTargetAppend(targetResultName, targetFields)
                    }
                    line("}")
                    line("return result")
                }
                line("}")
                line()
            }
            AnnotationConstants.MappingType.PER_ROW -> {
                val returnType = model.dynamicFieldSwiftType(dynamicField)
                line("private static func $helperName(_ rows: [SQLiteNowCoreRuntimeRow], adapters: ${databaseName}Adapters) throws -> $returnType {")
                indent {
                    line("for row in rows {")
                    indent {
                        line("guard $payloadCondition else { continue }")
                        line("return ${dynamicTargetConstructor(targetResultName, targetFields)}")
                    }
                    line("}")
                    if (dynamicField.annotations.notNull == true) {
                        line("throw SQLiteNowError.misuse(message: \"Required dynamic field ${dynamicField.src.fieldName} is null\")")
                    } else {
                        line("return nil")
                    }
                }
                line("}")
                line()
            }
            else -> error("Unsupported Product Swift dynamic mapping type: ${entry.mappingType}.")
        }
    }

    private fun SwiftWriter.emitDynamicTargetAppend(
        targetResultName: String,
        targetFields: List<SwiftProductField>,
    ) {
        line("result.append(")
        indent {
            line(dynamicTargetConstructor(targetResultName, targetFields))
        }
        line(")")
    }

    private fun dynamicTargetConstructor(
        targetResultName: String,
        targetFields: List<SwiftProductField>,
    ): String =
        buildString {
            append("$targetResultName(")
            targetFields.forEachIndexed { index, field ->
                if (index > 0) append(", ")
                append("${field.propertyName.swiftIdentifier()}: ${field.readExpression("row.cellAt(index: ${field.index})")}")
            }
            append(")")
        }

    private fun dynamicPayloadCondition(fields: List<SwiftProductField>, rowVariable: String): String =
        fields.joinToString(" || ") { field ->
            "!$rowVariable.cellAt(index: ${field.index}).isNull"
        }.ifBlank { "true" }

    private fun SwiftWriter.emitStreamFunction(
        namespace: String,
        statement: AnnotatedSelectStatement,
        params: List<SwiftProductParameter>,
        resultName: String,
    ) {
        val statementId = model.statementIdentifier(statement)
        line("private static func ${statementId}Stream(")
        indent {
            if (params.isNotEmpty()) {
                line("_ params: ${statement.swiftParamsName(namespace)},")
            }
            line("runtime: SQLiteNowCoreRuntimeDatabase,")
            line("adapters: ${databaseName}Adapters")
        }
        line(") -> AsyncThrowingStream<[$resultName], Error> {")
        indent {
            line("AsyncThrowingStream { continuation in")
            indent {
                line("let refreshCoordinator = SQLiteNowStreamRefreshCoordinator<$resultName>(")
                indent {
                    line("continuation: continuation,")
                    line("loadRows: {")
                    indent {
                        line("try await Self.${statementId}LoadRows(${model.streamArguments(params)})")
                    }
                    line("}")
                }
                line(")")
                line()
                line("let observer = RuntimeTableObserver(")
                indent {
                    line("onChanged: {")
                    indent {
                        line("refreshCoordinator.refresh()")
                    }
                    line("},")
                    line("onError: { payload in")
                    indent {
                        line("continuation.finish(throwing: SQLiteNowError.from(payload))")
                    }
                    line("}")
                }
                line(")")
                line("let handle = runtime.observeTables(tableNames: Self.${statementId}AffectedTables, observer: observer)")
                line("refreshCoordinator.refresh()")
                line("continuation.onTermination = { _ in")
                indent {
                    line("handle.cancel()")
                    line("observer.cancel()")
                    line("refreshCoordinator.cancel()")
                }
                line("}")
            }
            line("}")
        }
        line("}")
        line()
    }
}

private enum class SwiftLoadRowsOperation {
    QUERY,
    EXECUTE_RETURNING,
}
