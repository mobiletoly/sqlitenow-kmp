package dev.goquick.sqlitenow.gradle.database

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterNameResolver
import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.util.lowercaseFirst
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveSet
import dev.goquick.sqlitenow.gradle.util.pascalize
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.SharedResultManager
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementUtils
import java.io.File

/**
 * Generates a high-level database class that simplifies usage of generated data structures
 * and queries.
 */
class DatabaseCodeGenerator(
    private val nsWithStatements: Map<String, List<AnnotatedStatement>>,
    private val createTableStatements: List<AnnotatedCreateTableStatement>,
    createViewStatements: List<AnnotatedCreateViewStatement>,
    private val packageName: String,
    private val outputDir: File,
    private val databaseClassName: String,
    private val debug: Boolean = false,
) {
    private val columnLookup = ColumnLookup(createTableStatements, createViewStatements)
    private val adapterConfig = AdapterConfig(
        columnLookup = columnLookup,
        createTableStatements = createTableStatements,
        packageName = packageName
    )
    private val tableLookup = CaseInsensitiveMap(createTableStatements.map { it.src.tableName to it })
    private val sharedResultManager = SharedResultManager()
    private val adapterNameResolver = AdapterParameterNameResolver()

    // ---------- Adapter selection helpers ----------
    private fun isCustomType(t: TypeName): Boolean {
        val s = t.toString()
        return !(s.startsWith("kotlin.") || s.startsWith("kotlinx."))
    }

    private fun normalizedTypeString(t: TypeName): String = t.toString().removeSuffix("?")

    private fun adapterScore(u: UniqueAdapter): Int {
        val customScore = if (isCustomType(u.outputType)) 2 else 0
        val nnInput = if (!u.inputType.isNullable) 1 else 0
        val nonIdentity =
            if (normalizedTypeString(u.inputType) != normalizedTypeString(u.outputType)) 1 else 0
        return customScore + nnInput + nonIdentity
    }

    // Give precedence to adapters emitted from the namespace that owns the source column/result
    private fun providerPreferenceScore(namespace: String, adapter: UniqueAdapter): Int {
        return if (adapter.providerNamespace?.equals(namespace, ignoreCase = true) == true) 1 else 0
    }

    private fun baseFunctionKey(name: String): String = name.substringBefore("For")

    private fun computeBestProviders(
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): Map<String, String> {
        val winners = mutableMapOf<String, Pair<String, UniqueAdapter>>()
        adaptersByNamespace.toSortedMap().forEach { (ns, adapters) ->
            // Iterate deterministically so ties break in a stable manner across platforms
            adapters.sortedBy { it.functionName }.forEach { ua ->
                val key = baseFunctionKey(ua.functionName)
                val current = winners[key]
                if (current == null) {
                    winners[key] = ns to ua
                } else {
                    val currentPref = providerPreferenceScore(current.first, current.second)
                    val candidatePref = providerPreferenceScore(ns, ua)
                    when {
                        candidatePref > currentPref -> winners[key] = ns to ua
                        candidatePref < currentPref -> { /* keep current */ }
                        else -> {
                            val currentScore = adapterScore(current.second)
                            val candidateScore = adapterScore(ua)
                            if (candidateScore > currentScore || (candidateScore == currentScore && ns < current.first)) {
                                winners[key] = ns to ua
                            }
                        }
                    }
                }
            }
        }
        return winners.mapValues { it.value.first }
    }

    private fun baseNameForNamespace(namespace: String): String {
        val table = tableLookup[namespace]
        return table?.annotations?.name ?: pascalize(namespace)
    }

    private fun adapterClassNameFor(namespace: String): String =
        baseNameForNamespace(namespace) + "Adapters"

    private fun adapterPropertyNameFor(namespace: String): String {
        val cls = adapterClassNameFor(namespace)
        return cls.lowercaseFirst()
    }

    private fun queryNamespaceName(namespace: String): String = pascalize(namespace) + "Query"
    private fun routerClassNameFor(namespace: String): String =
        baseNameForNamespace(namespace) + "Router"

    private fun routerPropertyNameFor(namespace: String): String =
        baseNameForNamespace(namespace).lowercaseFirst()

    /** Data class representing a unique adapter with its function signature. */
    data class UniqueAdapter(
        val functionName: String,
        val inputType: TypeName,
        val outputType: TypeName,
        val isNullable: Boolean,
        val providerNamespace: String?
    ) {
        fun toParameterSpec(): ParameterSpec {
            val lambdaType = LambdaTypeName.Companion.get(
                parameters = arrayOf(inputType),
                returnType = outputType
            )
            return ParameterSpec.Companion.builder(functionName, lambdaType).build()
        }

        /** Create a signature key for deduplication that considers the actual TypeName structure */
        fun signatureKey(): String {
            return "${inputType}__$outputType"
        }
    }

    /** Collects and deduplicates adapters per namespace. */
    private fun collectAdaptersByNamespace(): Map<String, List<UniqueAdapter>> {
        val adaptersByNamespace = mutableMapOf<String, MutableList<UniqueAdapter>>()

        // Collect adapters for each namespace separately
        nsWithStatements.forEach { (namespace, statements) ->
            val namespaceAdapters = mutableListOf<UniqueAdapter>()
            val processedSharedResults = mutableMapOf<String, Boolean>()

            // First, register all shared results for this namespace
            statements.filterIsInstance<AnnotatedSelectStatement>().forEach { statement ->
                sharedResultManager.registerSharedResult(statement, namespace)
            }

            statements.forEach { statement ->
                val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace).toMutableList()
                if (statement is AnnotatedSelectStatement && statement.annotations.queryResult != null) {
                    val sharedResultKey = "${namespace}.${statement.annotations.queryResult}"
                    val alreadyProcessed = processedSharedResults[sharedResultKey] ?: false
                    if (alreadyProcessed) {
                        statementAdapters.removeIf { it.kind == AdapterConfig.AdapterKind.MAP_RESULT }
                    }
                    val mapCollected = statement.annotations.mapTo != null
                    processedSharedResults[sharedResultKey] = alreadyProcessed || mapCollected
                }
                statementAdapters.forEach { config ->
                    namespaceAdapters.add(
                        UniqueAdapter(
                            functionName = config.adapterFunctionName,
                            inputType = config.inputType,
                            outputType = config.outputType,
                            isNullable = config.isNullable,
                            providerNamespace = config.providerNamespace
                        )
                    )
                }
            }

            val deduplicatedAdapters = deduplicateAdaptersForNamespace(namespaceAdapters)
            adaptersByNamespace[namespace] = deduplicatedAdapters.toMutableList()
        }

        return adaptersByNamespace
    }

    /** Deduplicates adapters within a single namespace. */
    private fun deduplicateAdaptersForNamespace(adapters: List<UniqueAdapter>): List<UniqueAdapter> {
        val adaptersByName = adapters.groupBy { it.functionName }
        val result = mutableListOf<UniqueAdapter>()

        adaptersByName.forEach { (baseName, adapterList) ->
            // Remove exact duplicates first (same signature)
            val uniqueBySignature = adapterList.distinctBy { it.signatureKey() }

            val best = uniqueBySignature.maxByOrNull { adapterScore(it) }
            val bestScore = best?.let { adapterScore(it) } ?: 0
            val top = uniqueBySignature.filter { adapterScore(it) == bestScore }

            if (top.size == 1) {
                result.add(top.first().copy(functionName = baseName))
            } else {
                // Tie: keep all, but disambiguate names deterministically
                top.forEach { adapter ->
                    val inputLabel = sanitizeTypeLabel(adapter.inputType)
                    val outputLabel = sanitizeTypeLabel(adapter.outputType)
                    val uniqueName = "${baseName}For${inputLabel}To${outputLabel}"
                    result.add(adapter.copy(functionName = uniqueName))
                }
            }
        }

        return result
    }

    private fun sanitizeTypeLabel(type: TypeName): String {
        // Build a short, identifier-safe type label (e.g. StringNullable, ByteArray, ListString)
        var s = type.toString()
        // Keep only leaf class names for qualified types
        s = s.split('.').last()
        // Replace nullability with a readable token
        s = s.replace("?", "Nullable")
        // Remove generic punctuation and non-alphanumerics
        s = s.replace(Regex("[<>.,\\s]"), "")
        // Capitalize first char for consistency
        return s.replaceFirstChar { it.uppercase() }
    }

    /**
     * Simple helper function to find the correct adapter name.
     * Uses direct lookup by expected function name pattern.
     */
    private fun findAdapterName(
        namespace: String,
        expectedFunctionName: String,
        expectedInputType: TypeName,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val deduplicatedAdapters = adaptersByNamespace[namespace] ?: return expectedFunctionName

        // Look for exact match first
        val exactMatch = deduplicatedAdapters.find { it.functionName == expectedFunctionName }
        if (exactMatch != null) {
            return exactMatch.functionName
        }

        // Look for renamed version (e.g., phoneToSqlValueForString)
        val renamedMatch = deduplicatedAdapters.find {
            it.functionName.startsWith(expectedFunctionName + "For") && it.inputType == expectedInputType
        }

        return renamedMatch?.functionName ?: expectedFunctionName
    }

    private fun findBestProviderByName(
        expectedFunctionName: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): Pair<String, UniqueAdapter>? {
        fun isCustom(t: TypeName): Boolean {
            val s = t.toString()
            return !(s.startsWith("kotlin.") || s.startsWith("kotlinx."))
        }

        fun normalized(t: TypeName) = t.toString().removeSuffix("?")

        var best: Pair<String, UniqueAdapter>? = null
        adaptersByNamespace.forEach { (ns, adapters) ->
            adapters.filter {
                it.functionName == expectedFunctionName || it.functionName.startsWith(
                    expectedFunctionName + "For"
                )
            }.forEach { cand ->
                val cur = best
                if (cur == null) {
                    best = ns to cand
                } else {
                    val currentPref = providerPreferenceScore(cur.first, cur.second)
                    val candidatePref = providerPreferenceScore(ns, cand)
                    // Prefer adapters from the namespace that declared the column/result; when that
                    // still ties, fall back to quality score and finally namespace name for stability.
                    when {
                        candidatePref > currentPref -> best = ns to cand
                        candidatePref < currentPref -> {
                            // keep current
                        }
                        else -> {
                            val currentScore = adapterScore(cur.second)
                            val candidateScore = adapterScore(cand)
                            if (candidateScore > currentScore || (candidateScore == currentScore && ns < cur.first)) {
                                best = ns to cand
                            }
                        }
                    }
                }
            }
        }
        return best
    }

    /**
     * Generates the main database class file.
     */
    fun generateDatabaseClass() {
        val fileBuilder = FileSpec.Companion.builder(packageName, databaseClassName)
            .addFileComment("Generated database class with unified adapter management")
            .addFileComment("Do not modify this file manually")
            .addAnnotation(
                AnnotationSpec.Companion.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )

        fileBuilder.addImport("dev.goquick.sqlitenow.core", "DatabaseMigrations")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "SqliteNowDatabase")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "SelectRunners")
        fileBuilder.addImport("dev.goquick.sqlitenow.core", "ExecuteRunners")
        fileBuilder.addImport("kotlinx.coroutines.flow", "Flow")

        val databaseClass = generateMainDatabaseClass()
        fileBuilder.addType(databaseClass)
        fileBuilder.build().writeTo(outputDir)
    }

    /** Generates the main database class with constructor and router properties. */
    private fun generateMainDatabaseClass(): TypeSpec {
        val classBuilder = TypeSpec.Companion.classBuilder(databaseClassName)
            .addModifiers(KModifier.PUBLIC)
            .superclass(ClassName("dev.goquick.sqlitenow.core", "SqliteNowDatabase"))

        val adaptersByNamespace = collectAdaptersByNamespace()

        // Compute best provider per adapter function and filter adapters accordingly
        val bestProviderForFunction: Map<String, String> = computeBestProviders(adaptersByNamespace)
        val namespacesWithFilteredAdapters = adaptersByNamespace
            .mapValues { (ns, adapters) ->
                adapters.filter {
                    bestProviderForFunction[baseFunctionKey(
                        it.functionName
                    )] == ns
                }
            }
            .filterValues { it.isNotEmpty() }

        // Build constructor and private adapter properties
        val ctor = buildConstructorWithAdapters(namespacesWithFilteredAdapters)
        classBuilder.primaryConstructor(ctor)
        // Superclass constructor call
        classBuilder.addSuperclassConstructorParameter("dbName = dbName")
        classBuilder.addSuperclassConstructorParameter("migration = migration")
        classBuilder.addSuperclassConstructorParameter("debug = debug")

        // Add private adapter properties matching constructor params
        addAdapterPrivateProperties(classBuilder, namespacesWithFilteredAdapters.keys)

        // Add router properties (one per namespace)
        addRouterProperties(classBuilder)

        // Generate adapter wrapper classes (best providers only)
        generateAdapterWrapperClasses(classBuilder, namespacesWithFilteredAdapters)

        // Generate router classes
        nsWithStatements.forEach { (namespace, statements) ->
            val routerClass = generateRouterClass(namespace, statements, adaptersByNamespace)
            classBuilder.addType(routerClass)
        }

        // Oversqlite integration helpers: derive sync tables from enableSync annotations
        val syncTables = createTableStatements
            .filter { it.annotations.enableSync }
            .distinct()
        if (syncTables.isNotEmpty()) {
            // Generate SyncTable objects with primary key detection
            val syncTableInitializers = syncTables.map { table ->
                // Use explicit syncKeyColumnName annotation if provided, otherwise auto-detect
                val explicitSyncKey = table.annotations.syncKeyColumnName
                val primaryKeyColumn = explicitSyncKey ?: findPrimaryKeyColumn(table)

                val syncKeyParam = if (primaryKeyColumn != null && primaryKeyColumn != "id") {
                    ", syncKeyColumnName = \"$primaryKeyColumn\""
                } else if (explicitSyncKey != null) {
                    // Explicit annotation provided, even if it's "id"
                    ", syncKeyColumnName = \"$explicitSyncKey\""
                } else {
                    "" // Use default "id"
                }
                "%T(tableName = \"${table.name}\"$syncKeyParam)"
            }

            val listInitializer = syncTableInitializers.joinToString(", ")
            val companion = TypeSpec.Companion.companionObjectBuilder()
                .addProperty(
                    PropertySpec.Companion.builder(
                        "syncTables",
                        ClassName(
                            "kotlin.collections",
                            "List"
                        ).parameterizedBy(
                            ClassName(
                                "dev.goquick.sqlitenow.oversqlite",
                                "SyncTable"
                            )
                        )
                    ).initializer(
                        "listOf($listInitializer)",
                        *Array(syncTables.size) {
                            ClassName(
                                "dev.goquick.sqlitenow.oversqlite",
                                "SyncTable"
                            )
                        })
                        .addKdoc("Tables annotated with enableSync in schema; used to configure oversqlite.")
                        .build()
                )
                .build()
            classBuilder.addType(companion)

            // fun buildOversqliteConfig(schema: String, uploadLimit: Int = 200, downloadLimit: Int = 1000): OversqliteConfig
            classBuilder.addFunction(
                FunSpec.Companion.builder("buildOversqliteConfig")
                    .addKdoc("Builds oversqlite config using enableSync tables.")
                    .addParameter("schema", String::class)
                    .addParameter(
                        ParameterSpec.Companion.builder("uploadLimit", Int::class).defaultValue("200").build()
                    )
                    .addParameter(
                        ParameterSpec.Companion.builder("downloadLimit", Int::class).defaultValue("1000")
                            .build()
                    )
                    .addParameter(
                        ParameterSpec.Companion.builder("verboseLogs", Boolean::class).defaultValue("false")
                            .build()
                    )
                    .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig"))
                    .addStatement(
                        "return %T(schema, syncTables, uploadLimit, downloadLimit, verboseLogs = verboseLogs)",
                        ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteConfig")
                    )
                    .build()
            )

            // fun newOversqliteClient(schema: String, httpClient: HttpClient, resolver: Resolver = ServerWinsResolver,...)
            classBuilder.addFunction(
                FunSpec.Companion.builder("newOversqliteClient")
                    .addKdoc("Creates a DefaultOversqliteClient bound to this DB using a pre-configured HttpClient with authentication and base URL.")
                    .addParameter("schema", String::class)
                    .addParameter("httpClient", ClassName("io.ktor.client", "HttpClient"))
                    .addParameter(
                        ParameterSpec.Companion.builder(
                            "resolver",
                            ClassName("dev.goquick.sqlitenow.oversqlite", "Resolver")
                        ).defaultValue(
                            "%T",
                            ClassName("dev.goquick.sqlitenow.oversqlite", "ServerWinsResolver")
                        ).build()
                    )
                    .addParameter(
                        ParameterSpec.Companion.builder("uploadLimit", Int::class).defaultValue("200").build()
                    )
                    .addParameter(
                        ParameterSpec.Companion.builder("downloadLimit", Int::class).defaultValue("1000")
                            .build()
                    )
                    .addParameter(
                        ParameterSpec.Companion.builder("verboseLogs", Boolean::class).defaultValue("false")
                            .build()
                    )
                    .returns(ClassName("dev.goquick.sqlitenow.oversqlite", "OversqliteClient"))
                    .addStatement("val cfg = buildOversqliteConfig(schema, uploadLimit, downloadLimit, verboseLogs)")
                    .addStatement(
                        "return %T(db = this.connection(), config = cfg, http = httpClient, resolver = resolver, tablesUpdateListener = { notifyTablesChanged(it) })",
                        ClassName("dev.goquick.sqlitenow.oversqlite", "DefaultOversqliteClient")
                    )
                    .build()
            )
        }

        return classBuilder.build()
    }

    private fun buildConstructorWithAdapters(
        namespacesWithAdapters: Map<String, List<UniqueAdapter>>
    ): FunSpec {
        val b = FunSpec.Companion.constructorBuilder()
            .addParameter("dbName", String::class)
            .addParameter(
                "migration",
                ClassName("dev.goquick.sqlitenow.core", "DatabaseMigrations")
            )
            .addParameter(
                ParameterSpec.Companion.builder("debug", Boolean::class).defaultValue("%L", debug).build()
            )
        namespacesWithAdapters.keys.forEach { ns ->
            val adapterClassName = adapterClassNameFor(ns)
            val adapterPropName = adapterPropertyNameFor(ns)
            b.addParameter(adapterPropName, ClassName("", adapterClassName))
        }
        return b.build()
    }

    private fun addAdapterPrivateProperties(
        classBuilder: TypeSpec.Builder,
        namespaces: Set<String>
    ) {
        namespaces.forEach { ns ->
            val adapterClassName = adapterClassNameFor(ns)
            val adapterPropName = adapterPropertyNameFor(ns)
            classBuilder.addProperty(
                PropertySpec.Companion.builder(adapterPropName, ClassName("", adapterClassName))
                    .addModifiers(KModifier.PRIVATE)
                    .initializer(adapterPropName)
                    .build()
            )
        }
    }

    private fun addRouterProperties(classBuilder: TypeSpec.Builder) {
        nsWithStatements.keys.forEach { namespace ->
            val routerClassName = routerClassNameFor(namespace)
            val routerPropName = routerPropertyNameFor(namespace)
            classBuilder.addProperty(
                PropertySpec.Companion.builder(routerPropName, ClassName("", routerClassName))
                    .initializer("$routerClassName(ref = this)")
                    .build()
            )
        }
    }

    private fun generateAdapterWrapperClasses(
        classBuilder: TypeSpec.Builder,
        namespacesWithAdapters: Map<String, List<UniqueAdapter>>
    ) {
        namespacesWithAdapters.forEach { (namespace, adapters) ->
            val adapterClass = generateAdapterClass(namespace, adapters)
            classBuilder.addType(adapterClass)
        }
    }

    // ---------- Object expression helpers (Select/Execute runners) ----------

    private fun resolveAdapterProvider(
        config: AdapterConfig.ParamConfig,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): Pair<String, String> {
        val providerNs =
            findBestProviderByName(config.adapterFunctionName, adaptersByNamespace)?.first
                ?: namespace
        val providerProp = adapterPropertyNameFor(providerNs)
        val actualAdapterName =
            findAdapterName(providerNs, config.adapterFunctionName, config.inputType, adaptersByNamespace)
        return providerProp to actualAdapterName
    }

    private fun buildCommonParamsLines(
        hasParams: Boolean,
        statementAdapters: List<AdapterConfig.ParamConfig>,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>,
    ): List<String> {
        val lines = mutableListOf<String>()
        lines += "conn = ref.connection()"
        if (hasParams) lines += "params = params"

        // Use the same canonicalized parameter names that query execution functions expect
        val chosenParamNames = adapterNameResolver.chooseAdapterParamNames(statementAdapters)

        statementAdapters.forEach { config ->
            val (providerProp, adapterName) = resolveAdapterProvider(
                config,
                namespace,
                adaptersByNamespace
            )
            val canonicalParamName = chosenParamNames[config] ?: config.adapterFunctionName
            lines += "$canonicalParamName = ref.$providerProp.$adapterName"
        }
        return lines
    }

    /**
     * Collects output adapter configurations for EXECUTE statements with RETURNING clause.
     * This is similar to how SELECT statements collect output adapters.
     */
    private fun collectOutputAdaptersForExecuteReturning(statement: AnnotatedExecuteStatement): List<AdapterConfig.ParamConfig> {
        if (!statement.hasReturningClause()) {
            return emptyList()
        }

        // Find the table definition
        val tableStatement = tableLookup[statement.src.table] ?: return emptyList()

        // Get RETURNING columns
        val returningColumns = statement.src.returningColumns

        // Determine which columns to include
        val columnsToInclude = if (returningColumns.contains("*")) {
            tableStatement.columns
        } else {
            val returningSet = CaseInsensitiveSet().apply { addAll(returningColumns) }
            tableStatement.columns.filter { column -> returningSet.containsIgnoreCase(column.src.name) }
        }

        // Create adapter configurations for columns that need them
        val configs = mutableListOf<AdapterConfig.ParamConfig>()
        val processedAdapters = mutableSetOf<String>()

        columnsToInclude.forEach { column ->
            if (column.annotations.containsKey(AnnotationConstants.ADAPTER)) {
                val propertyName = statement.annotations.propertyNameGenerator.convertToPropertyName(column.src.name)
                val adapterFunctionName = adapterConfig.getOutputAdapterFunctionName(propertyName)

                // Skip if already processed
                if (adapterFunctionName in processedAdapters) {
                    return@forEach
                }
                processedAdapters.add(adapterFunctionName)

                // Create adapter configuration for output (result conversion)
                val baseType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(column.src.dataType)
                val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
                val propertyNullable = column.isNullable()
                val sqlNullable = column.isSqlNullable()
                val targetType = SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
                    baseType,
                    propertyType,
                    propertyNullable,
                    packageName
                )

                val inputType = baseType.copy(nullable = sqlNullable)
                val outputType = targetType.copy(nullable = propertyNullable)

                val config = AdapterConfig.ParamConfig(
                    paramName = column.src.name,
                    adapterFunctionName = adapterFunctionName,
                    inputType = inputType,
                    outputType = outputType,
                    isNullable = propertyNullable,
                    providerNamespace = null,
                    kind = AdapterConfig.AdapterKind.RESULT_FIELD,
                )
                configs.add(config)
            }
        }

        return configs
    }

    /**
     * Finds the primary key column for a table.
     * Returns null if no primary key is found or if there are multiple primary keys.
     */
    private fun findPrimaryKeyColumn(table: AnnotatedCreateTableStatement): String? {
        val primaryKeyColumns = table.columns.filter { it.src.primaryKey }
        return if (primaryKeyColumns.size == 1) {
            primaryKeyColumns.first().src.name
        } else {
            null // No primary key or composite primary key
        }
    }

    /** Generates an adapter wrapper class for a specific namespace. */
    private fun generateAdapterClass(namespace: String, adapters: List<UniqueAdapter>): TypeSpec {
        val adapterClassName = adapterClassNameFor(namespace)
        val classBuilder = TypeSpec.Companion.classBuilder(adapterClassName)
            .addModifiers(KModifier.PUBLIC, KModifier.DATA)

        // Add constructor with adapter parameters
        val constructorBuilder = FunSpec.Companion.constructorBuilder()

        adapters.forEach { adapter ->
            val paramSpec = adapter.toParameterSpec()
            constructorBuilder.addParameter(paramSpec)

            // Add as property
            val propertySpec =
                PropertySpec.Companion.builder(adapter.functionName, adapter.toParameterSpec().type)
                    .initializer(adapter.functionName)
                    .build()
            classBuilder.addProperty(propertySpec)
        }

        classBuilder.primaryConstructor(constructorBuilder.build())
        return classBuilder.build()
    }

    /** Generates a router class for a specific namespace. */
    private fun generateRouterClass(
        namespace: String,
        statements: List<AnnotatedStatement>,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): TypeSpec {
        val routerClassName = routerClassNameFor(namespace)
        val classBuilder = TypeSpec.Companion.classBuilder(routerClassName)
            .addModifiers(KModifier.PUBLIC)

        // Add constructor with database reference
        val constructorBuilder = FunSpec.Companion.constructorBuilder()
            .addParameter("ref", ClassName("", databaseClassName))
        classBuilder.primaryConstructor(constructorBuilder.build())

        // Add ref property
        val refProperty = PropertySpec.Companion.builder("ref", ClassName("", databaseClassName))
            .initializer("ref")
            .addModifiers(KModifier.PRIVATE)
            .build()
        classBuilder.addProperty(refProperty)

        // Generate methods for each statement
        statements.forEach { statement ->
            when (statement) {
                is AnnotatedSelectStatement -> {
                    // Generate SelectRunners object for SELECT statements
                    classBuilder.addProperty(
                        generateSelectRunnersProperty(
                            statement,
                            namespace,
                            adaptersByNamespace
                        )
                    )
                }

                is AnnotatedExecuteStatement -> {
                    // Generate ExecuteRunners or ExecuteReturningRunners object based on RETURNING clause
                    if (statement.hasReturningClause()) {
                        classBuilder.addProperty(
                            generateExecuteReturningRunnersProperty(
                                statement,
                                namespace,
                                adaptersByNamespace
                            )
                        )
                    } else {
                        classBuilder.addProperty(
                            generateExecuteRunnersProperty(
                                statement,
                                namespace,
                                adaptersByNamespace
                            )
                        )
                    }
                }

                else -> {
                    // CREATE TABLE/VIEW statements don't need router methods
                }
            }
        }

        return classBuilder.build()
    }

    /** Generates an ExecuteRunners property for INSERT/UPDATE/DELETE statements. */
    private fun generateExecuteRunnersProperty(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.lowercaseFirst()

        // Check if statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        val hasParams = namedParameters.isNotEmpty()

        // Create the property type
        val propertyType = if (hasParams) {
            // Function type: (Params) -> ExecuteRunners
            val paramsType = ClassName(packageName, queryNamespaceName(namespace))
                .nestedClass(className)
                .nestedClass("Params")
            val executeRunnersType = ClassName("dev.goquick.sqlitenow.core", "ExecuteRunners")
            LambdaTypeName.Companion.get(parameters = arrayOf(paramsType), returnType = executeRunnersType)
        } else {
            // Direct ExecuteRunners type
            ClassName("dev.goquick.sqlitenow.core", "ExecuteRunners")
        }

        // Generate the object expression implementing ExecuteRunners
        val objectExpression = generateExecuteRunnersObjectExpression(
            statement, namespace, className,
            hasParams, adaptersByNamespace
        )

        return PropertySpec.Companion.builder(propertyName, propertyType)
            .initializer(objectExpression)
            .build()
    }

    /** Generates an ExecuteReturningRunners property for INSERT statements with RETURNING clause. */
    private fun generateExecuteReturningRunnersProperty(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.lowercaseFirst()

        // Check if statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        val hasParams = namedParameters.isNotEmpty()

        // Get the result type for RETURNING clause
        val resultType = SharedResultTypeUtils.createResultTypeNameForExecute(packageName, namespace, statement)

        // Create the property type
        val propertyType = if (hasParams) {
            // Function type: (Params) -> ExecuteReturningRunners<ResultType>
            val paramsType = ClassName(packageName, queryNamespaceName(namespace))
                .nestedClass(className)
                .nestedClass("Params")
            val executeReturningRunnersType = ClassName(
                "dev.goquick.sqlitenow.core",
                "ExecuteReturningRunners"
            )
                .parameterizedBy(resultType)
            LambdaTypeName.Companion.get(parameters = arrayOf(paramsType), returnType = executeReturningRunnersType)
        } else {
            // Direct ExecuteReturningRunners<ResultType> type
            ClassName("dev.goquick.sqlitenow.core", "ExecuteReturningRunners")
                .parameterizedBy(resultType)
        }

        // Generate the object expression implementing ExecuteReturningRunners
        val objectExpression = generateExecuteReturningRunnersObjectExpression(
            statement, namespace, className,
            hasParams, adaptersByNamespace
        )

        return PropertySpec.Companion.builder(propertyName, propertyType)
            .initializer(objectExpression)
            .build()
    }

    /** Generates a SelectRunners property for SELECT statements. */
    private fun generateSelectRunnersProperty(
        statement: AnnotatedSelectStatement,
        namespace: String,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): PropertySpec {
        val className = statement.getDataClassName()
        val propertyName = className.lowercaseFirst()

        // Determine result type (handles shared results)
        val resultType = resolvePublicResultType(namespace, statement)

        // Check if statement has parameters
        val namedParameters = StatementUtils.getNamedParameters(statement)
        val hasParams = namedParameters.isNotEmpty()

        // Create the property type
        val propertyType = if (hasParams) {
            // Function type: (Params) -> SelectRunners<ResultType>
            val paramsType = ClassName(packageName, queryNamespaceName(namespace))
                .nestedClass(className)
                .nestedClass("Params")
            val selectRunnersType = ClassName("dev.goquick.sqlitenow.core", "SelectRunners")
                .parameterizedBy(resultType)
            LambdaTypeName.Companion.get(parameters = arrayOf(paramsType), returnType = selectRunnersType)
        } else {
            // Direct SelectRunners<ResultType> type
            ClassName("dev.goquick.sqlitenow.core", "SelectRunners")
                .parameterizedBy(resultType)
        }

        // Generate the object expression implementing SelectRunners
        val objectExpression = generateSelectRunnersObjectExpression(
            statement,
            namespace,
            className,
            hasParams,
            adaptersByNamespace
        )

        return PropertySpec.Companion.builder(propertyName, propertyType)
            .initializer(objectExpression)
            .build()
    }

    /** Generates the object expression that implements SelectRunners interface. */
    private fun generateSelectRunnersObjectExpression(
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        hasParams: Boolean,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val paramLines = buildCommonParamsLines(
            hasParams = hasParams,
            statementAdapters = statementAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        val b = IndentedCodeBuilder()
        val resultTypeName = resolvePublicResultTypeString(namespace, statement)
        if (hasParams) {
            b.line("{ params ->")
        }
        b.line("object : SelectRunners<$resultTypeName> {")
        // asList
        b.indent(by = 2) {
            b.line("override suspend fun asList() = $capitalizedNamespace.$className.executeAsList(")
            b.indent(by = 2) {
                paramLines.forEachIndexed { idx, line ->
                    val suffix = if (idx < paramLines.lastIndex) "," else ""
                    b.line("$line$suffix")
                }
            }
            b.line(")")
            b.line("")

            // Single-row methods for non-collection queries
            if (!statement.hasCollectionMapping()) {
                b.line("override suspend fun asOne() = $capitalizedNamespace.$className.executeAsOne(")
                b.indent(by = 2) {
                    paramLines.forEachIndexed { idx, line ->
                        val suffix = if (idx < paramLines.lastIndex) "," else ""
                        b.line("$line$suffix")
                    }
                }
                b.line(")")
                b.line("")

                b.line("override suspend fun asOneOrNull() = $capitalizedNamespace.$className.executeAsOneOrNull(")
                b.indent(by = 2) {
                    paramLines.forEachIndexed { idx, line ->
                        val suffix = if (idx < paramLines.lastIndex) "," else ""
                        b.line("$line$suffix")
                    }
                }
                b.line(")")
                b.line("")
            } else {
                // For collection mapping queries, single-row methods don't make sense
                b.line("override suspend fun asOne(): $resultTypeName = throw UnsupportedOperationException(\"asOne() is not supported for collection mapping queries. Use asList() instead.\")")
                b.line("")
                b.line("override suspend fun asOneOrNull(): $resultTypeName? = throw UnsupportedOperationException(\"asOneOrNull() is not supported for collection mapping queries. Use asList() instead.\")")
                b.line("")
            }

            // asFlow
            b.line("override fun asFlow() = ref.createReactiveQueryFlow(")
            b.indent(by = 2) {
                b.line("affectedTables = $capitalizedNamespace.$className.affectedTables,")
                b.line("queryExecutor = {")
                b.indent(by = 2) {
                    b.line("$capitalizedNamespace.$className.executeAsList(")
                    b.indent(by = 2) {
                        paramLines.forEachIndexed { idx, line ->
                            val suffix = if (idx < paramLines.lastIndex) "," else ""
                            b.line("$line$suffix")
                        }
                    }
                    b.line(")")
                }
                b.line("}")
            }
            b.line(")")
        }
        b.line("}")

        if (hasParams) {
            b.line("}")
        }

        return b.build()
    }

    /** Generates the object expression that implements ExecuteRunners interface. */
    private fun generateExecuteRunnersObjectExpression(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        hasParams: Boolean,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val statementAdapters = adapterConfig.collectAllParamConfigs(statement, namespace)
        val paramLines = buildCommonParamsLines(
            hasParams = hasParams,
            statementAdapters = statementAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        val b = IndentedCodeBuilder()
        if (hasParams) {
            b.line("{ params ->")
        }
        b.line("object : ExecuteRunners {")
        b.indent(by = 2) {
            b.line("override suspend fun execute() {")
            b.indent(by = 2) {
                b.line("$capitalizedNamespace.$className.execute(")
                b.indent(by = 2) {
                    paramLines.forEachIndexed { idx, line ->
                        val suffix = if (idx < paramLines.lastIndex) "," else ""
                        b.line("$line$suffix")
                    }
                }
                b.line(")")
                b.line("// Notify listeners that tables have changed")
                b.line("ref.notifyTablesChanged($capitalizedNamespace.$className.affectedTables)")
            }
            b.line("}")
        }
        b.line("}")

        if (hasParams) {
            b.line("}")
        }

        return b.build()
    }

    /** Generates the object expression that implements ExecuteReturningRunners interface. */
    private fun generateExecuteReturningRunnersObjectExpression(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        className: String,
        hasParams: Boolean,
        adaptersByNamespace: Map<String, List<UniqueAdapter>>
    ): String {
        val capitalizedNamespace = queryNamespaceName(namespace)

        // For RETURNING queries, we need both input and output adapters
        val inputAdapters = adapterConfig.collectAllParamConfigs(statement, namespace) // Input adapters for parameter binding
        val outputAdapters = collectOutputAdaptersForExecuteReturning(statement) // Output adapters for result conversion
        val allAdapters = inputAdapters + outputAdapters

        val paramLines = buildCommonParamsLines(
            hasParams = hasParams,
            statementAdapters = allAdapters,
            namespace = namespace,
            adaptersByNamespace = adaptersByNamespace,
        )

        val b = IndentedCodeBuilder()
        if (hasParams) {
            b.line("{ params ->")
        }
        val resultTypeString = SharedResultTypeUtils.createResultTypeStringForExecute(namespace, statement)
        b.line("object : ExecuteReturningRunners<${resultTypeString}> {")
        b.indent(by = 2) {
            // executeReturningList method
            b.line("override suspend fun executeReturningList(): List<${resultTypeString}> {")
            b.indent(by = 2) {
                b.line("val result = $capitalizedNamespace.$className.executeReturningList(")
                b.indent(by = 2) {
                    paramLines.forEachIndexed { idx, line ->
                        val suffix = if (idx < paramLines.lastIndex) "," else ""
                        b.line("$line$suffix")
                    }
                }
                b.line(")")
                b.line("// Notify listeners that tables have changed")
                b.line("ref.notifyTablesChanged($capitalizedNamespace.$className.affectedTables)")
                b.line("return result")
            }
            b.line("}")
            b.line("")

            // executeReturningOne method
            b.line("override suspend fun executeReturningOne(): $resultTypeString {")
            b.indent(by = 2) {
                b.line("val result = $capitalizedNamespace.$className.executeReturningOne(")
                b.indent(by = 2) {
                    paramLines.forEachIndexed { idx, line ->
                        val suffix = if (idx < paramLines.lastIndex) "," else ""
                        b.line("$line$suffix")
                    }
                }
                b.line(")")
                b.line("// Notify listeners that tables have changed")
                b.line("ref.notifyTablesChanged($capitalizedNamespace.$className.affectedTables)")
                b.line("return result")
            }
            b.line("}")
            b.line("")

            // executeReturningOneOrNull method
            b.line("override suspend fun executeReturningOneOrNull(): ${resultTypeString}? {")
            b.indent(by = 2) {
                b.line("val result = $capitalizedNamespace.$className.executeReturningOneOrNull(")
                b.indent(by = 2) {
                    paramLines.forEachIndexed { idx, line ->
                        val suffix = if (idx < paramLines.lastIndex) "," else ""
                        b.line("$line$suffix")
                    }
                }
                b.line(")")
                b.line("// Notify listeners that tables have changed")
                b.line("ref.notifyTablesChanged($capitalizedNamespace.$className.affectedTables)")
                b.line("return result")
            }
            b.line("}")
        }
        b.line("}")

        if (hasParams) {
            b.line("}")
        }

        return b.build()
    }

    private fun resolvePublicResultType(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): TypeName {
        return resolveMapToType(statement) ?: SharedResultTypeUtils.createResultTypeName(packageName, namespace, statement)
    }

    private fun resolveMapToType(statement: AnnotatedSelectStatement): TypeName? {
        val target = statement.annotations.mapTo ?: return null
        return SqliteTypeToKotlinCodeConverter.parseCustomType(target, packageName)
    }

    private fun resolvePublicResultTypeString(
        namespace: String,
        statement: AnnotatedSelectStatement,
    ): String {
        val override = statement.annotations.mapTo?.trim()
        if (!override.isNullOrEmpty()) return override
        return SharedResultTypeUtils.createResultTypeString(namespace, statement)
    }
}
