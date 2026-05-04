package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.ExecuteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import java.io.File
import kotlin.io.path.createTempDirectory

internal fun regularFieldSource(
    fieldName: String,
    tableName: String,
    originalColumnName: String = fieldName,
    dataType: String = "TEXT",
    isNullable: Boolean = false,
): SelectStatement.FieldSource = SelectStatement.FieldSource(
    fieldName = fieldName,
    tableName = tableName,
    originalColumnName = originalColumnName,
    dataType = dataType,
    isNullable = isNullable,
)

internal fun fieldSource(
    fieldName: String,
    tableName: String,
    originalColumnName: String = fieldName,
    dataType: String = "TEXT",
    isNullable: Boolean = false,
): SelectStatement.FieldSource = regularFieldSource(
    fieldName = fieldName,
    tableName = tableName,
    originalColumnName = originalColumnName,
    dataType = dataType,
    isNullable = isNullable,
)

internal fun regularField(
    fieldName: String,
    tableName: String,
    originalColumnName: String = fieldName,
    dataType: String = "TEXT",
    isNullable: Boolean = false,
    propertyName: String? = null,
    propertyType: String? = null,
    adapter: Boolean? = null,
    aliasPath: List<String> = emptyList(),
): AnnotatedSelectStatement.Field = AnnotatedSelectStatement.Field(
    src = regularFieldSource(
        fieldName = fieldName,
        tableName = tableName,
        originalColumnName = originalColumnName,
        dataType = dataType,
        isNullable = isNullable,
    ),
    annotations = FieldAnnotationOverrides(
        propertyName = propertyName,
        propertyType = propertyType,
        notNull = null,
        adapter = adapter,
    ),
    aliasPath = aliasPath,
)

internal fun dynamicField(
    fieldName: String,
    mappingType: String,
    propertyType: String,
    sourceTable: String,
    aliasPrefix: String? = null,
    collectionKey: String? = null,
    notNull: Boolean? = true,
    suppressProperty: Boolean = false,
    aliasPath: List<String> = emptyList(),
): AnnotatedSelectStatement.Field = AnnotatedSelectStatement.Field(
    src = SelectStatement.FieldSource(
        fieldName = fieldName,
        tableName = "",
        originalColumnName = fieldName,
        dataType = "DYNAMIC",
        isNullable = notNull != true,
    ),
    annotations = FieldAnnotationOverrides(
        propertyName = null,
        propertyType = propertyType,
        notNull = notNull,
        adapter = false,
        isDynamicField = true,
        aliasPrefix = aliasPrefix,
        mappingType = mappingType,
        sourceTable = sourceTable,
        collectionKey = collectionKey,
        suppressProperty = suppressProperty,
    ),
    aliasPath = aliasPath,
)

internal fun selectStatement(
    fields: List<SelectStatement.FieldSource>,
    fromTable: String? = null,
    tableAliases: Map<String, String> = emptyMap(),
    joinConditions: List<SelectStatement.JoinCondition> = emptyList(),
): SelectStatement = SelectStatement(
    sql = "SELECT fixture",
    fromTable = fromTable ?: tableAliases.values.firstOrNull(),
    joinTables = tableAliases.values.drop(1),
    fields = fields,
    namedParameters = emptyList(),
    namedParametersToColumns = emptyMap(),
    offsetNamedParam = null,
    limitNamedParam = null,
    parameterCastTypes = emptyMap(),
    tableAliases = tableAliases,
    joinConditions = joinConditions,
)

internal fun annotatedSelectStatement(
    name: String = "FixtureSelect",
    src: SelectStatement,
    fields: List<AnnotatedSelectStatement.Field>,
    queryResult: String? = null,
    collectionKey: String? = null,
    mapTo: String? = null,
): AnnotatedSelectStatement = AnnotatedSelectStatement(
    name = name,
    src = src,
    annotations = StatementAnnotationOverrides(
        name = null,
        propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
        queryResult = queryResult,
        collectionKey = collectionKey,
        mapTo = mapTo,
    ),
    fields = fields,
)

internal fun annotatedSelectStatement(
    name: String = "FixtureSelect",
    sources: List<SelectStatement.FieldSource>,
    dynamicFields: List<AnnotatedSelectStatement.Field> = emptyList(),
    regularFields: List<AnnotatedSelectStatement.Field> = sources.map {
        regularField(
            fieldName = it.fieldName,
            tableName = it.tableName,
            originalColumnName = it.originalColumnName,
            dataType = it.dataType,
            isNullable = it.isNullable,
        )
    },
    fromTable: String? = null,
    tableAliases: Map<String, String> = emptyMap(),
    joinConditions: List<SelectStatement.JoinCondition> = emptyList(),
    queryResult: String? = null,
    collectionKey: String? = null,
    mapTo: String? = null,
): AnnotatedSelectStatement = annotatedSelectStatement(
    name = name,
    src = selectStatement(
        fields = sources,
        fromTable = fromTable,
        tableAliases = tableAliases,
        joinConditions = joinConditions,
    ),
    fields = regularFields + dynamicFields,
    queryResult = queryResult,
    collectionKey = collectionKey,
    mapTo = mapTo,
)

internal fun selectStatementWithParameters(
    fields: List<SelectStatement.FieldSource>,
    namedParameters: List<String>,
    namedParametersToColumns: Map<String, AssociatedColumn>,
    sql: String = "SELECT fixture",
    fromTable: String? = null,
    tableAliases: Map<String, String> = emptyMap(),
    joinConditions: List<SelectStatement.JoinCondition> = emptyList(),
): SelectStatement = SelectStatement(
    sql = sql,
    fromTable = fromTable ?: tableAliases.values.firstOrNull(),
    joinTables = tableAliases.values.drop(1),
    fields = fields,
    namedParameters = namedParameters,
    namedParametersToColumns = namedParametersToColumns,
    offsetNamedParam = null,
    limitNamedParam = null,
    parameterCastTypes = emptyMap(),
    tableAliases = tableAliases,
    joinConditions = joinConditions,
)

internal fun statementAnnotations(
    name: String? = null,
    queryResult: String? = null,
    collectionKey: String? = null,
    mapTo: String? = null,
): StatementAnnotationOverrides = StatementAnnotationOverrides(
    name = name,
    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
    queryResult = queryResult,
    collectionKey = collectionKey,
    mapTo = mapTo,
)

internal fun annotatedInsertStatement(
    name: String,
    table: String,
    namedParameters: List<String> = emptyList(),
    parameterToColumnNames: Map<String, String> = emptyMap(),
    returningColumns: List<String> = emptyList(),
    sql: String = "INSERT INTO $table DEFAULT VALUES",
    annotations: StatementAnnotationOverrides = statementAnnotations(),
): AnnotatedExecuteStatement = AnnotatedExecuteStatement(
    name = name,
    src = InsertStatement(
        sql = sql,
        table = table,
        namedParameters = namedParameters,
        columnNamesAssociatedWithNamedParameters = parameterToColumnNames,
        withSelectStatements = emptyList(),
        parameterCastTypes = emptyMap(),
        hasReturningClause = returningColumns.isNotEmpty(),
        returningColumns = returningColumns,
    ),
    annotations = annotations,
)

internal fun annotatedUpdateStatement(
    name: String,
    table: String,
    namedParameters: List<String> = emptyList(),
    parameterToColumns: Map<String, AssociatedColumn> = emptyMap(),
    parameterToColumnNames: Map<String, String> = emptyMap(),
    returningColumns: List<String> = emptyList(),
    sql: String = "UPDATE $table SET id = id",
    annotations: StatementAnnotationOverrides = statementAnnotations(),
): AnnotatedExecuteStatement = AnnotatedExecuteStatement(
    name = name,
    src = UpdateStatement(
        sql = sql,
        table = table,
        namedParameters = namedParameters,
        namedParametersToColumns = parameterToColumns,
        namedParametersToColumnNames = parameterToColumnNames,
        withSelectStatements = emptyList(),
        parameterCastTypes = emptyMap(),
        hasReturningClause = returningColumns.isNotEmpty(),
        returningColumns = returningColumns,
    ),
    annotations = annotations,
)

internal fun annotatedDeleteStatement(
    name: String,
    table: String,
    namedParameters: List<String> = emptyList(),
    parameterToColumns: Map<String, AssociatedColumn> = emptyMap(),
    returningColumns: List<String> = emptyList(),
    sql: String = "DELETE FROM $table",
    annotations: StatementAnnotationOverrides = statementAnnotations(),
): AnnotatedExecuteStatement = AnnotatedExecuteStatement(
    name = name,
    src = DeleteStatement(
        sql = sql,
        table = table,
        namedParameters = namedParameters,
        namedParametersToColumns = parameterToColumns,
        withSelectStatements = emptyList(),
        parameterCastTypes = emptyMap(),
        hasReturningClause = returningColumns.isNotEmpty(),
        returningColumns = returningColumns,
    ),
    annotations = annotations,
)

internal fun annotatedCreateTable(
    tableName: String,
    columns: List<AnnotatedCreateTableStatement.Column>,
): AnnotatedCreateTableStatement = AnnotatedCreateTableStatement(
    name = tableName.replaceFirstChar(Char::uppercaseChar),
    src = CreateTableStatement(
        sql = "CREATE TABLE $tableName (...)",
        tableName = tableName,
        columns = columns.map { it.src },
    ),
    annotations = StatementAnnotationOverrides(
        name = null,
        propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
        queryResult = null,
        collectionKey = null,
    ),
    columns = columns,
)

internal fun annotatedTableColumn(
    name: String,
    dataType: String,
    notNull: Boolean = true,
    propertyName: String? = null,
    propertyType: String? = null,
    adapter: Boolean = false,
): AnnotatedCreateTableStatement.Column {
    val annotations = buildMap<String, Any?> {
        propertyName?.let { put(AnnotationConstants.PROPERTY_NAME, it) }
        propertyType?.let { put(AnnotationConstants.PROPERTY_TYPE, it) }
        if (adapter) {
            put(AnnotationConstants.ADAPTER, AnnotationConstants.ADAPTER_CUSTOM)
        }
    }
    return AnnotatedCreateTableStatement.Column(
        src = CreateTableStatement.Column(
            name = name,
            dataType = dataType,
            notNull = notNull,
            primaryKey = false,
            autoIncrement = false,
            unique = false,
        ),
        annotations = annotations,
    )
}

internal fun generatorContext(
    packageName: String = "fixture.db",
    createTableStatements: List<AnnotatedCreateTableStatement> = emptyList(),
    createViewStatements: List<AnnotatedCreateViewStatement> = emptyList(),
    selectStatements: List<AnnotatedSelectStatement> = emptyList(),
): GeneratorContext {
    val tmpDir = createTempDirectory(prefix = "phase3-mapping-").toFile()
    val namespaces: Map<String, List<AnnotatedStatement>> =
        if (selectStatements.isEmpty()) {
            emptyMap()
        } else {
            mapOf("fixture" to selectStatements)
        }
    return GeneratorContext(
        packageName = packageName,
        outputDir = File(tmpDir, "generated"),
        createTableStatements = createTableStatements,
        createViewStatements = createViewStatements,
        nsWithStatements = namespaces,
    )
}
