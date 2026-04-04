package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.generator.query.GetterCallFactory
import dev.goquick.sqlitenow.gradle.generator.query.ParameterBinding
import dev.goquick.sqlitenow.gradle.generator.query.QueryBindEmitter
import dev.goquick.sqlitenow.gradle.generator.query.QueryExecuteEmitter
import dev.goquick.sqlitenow.gradle.generator.query.QueryFunctionScaffolder
import dev.goquick.sqlitenow.gradle.generator.query.QueryReadEmitter
import dev.goquick.sqlitenow.gradle.generator.query.ResultMappingHelper
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import java.io.File
import java.nio.file.Path
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class QueryEmitterCoverageTest {

    @Test
    @DisplayName("QueryReadEmitter keeps mapTo adapters on regular reads but excludes them from joined reads")
    fun queryReadEmitterSeparatesJoinedAndRegularAdapters() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("id", "INTEGER"),
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val statement = annotatedSelectStatement(
            name = "SelectSummary",
            src = selectStatement(
                fields = listOf(
                    fieldSource("id", "p", originalColumnName = "id", dataType = "INTEGER"),
                    fieldSource("birth_date", "p", originalColumnName = "birth_date"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(
                regularField("id", "p", originalColumnName = "id", dataType = "INTEGER"),
                regularField("birth_date", "p", originalColumnName = "birth_date"),
            ),
            queryResult = "PersonSummaryRow",
            mapTo = "fixture.model.PersonSummary",
        )
        val ctx = generatorContext(createTableStatements = listOf(personTable), selectStatements = listOf(statement))
        val emitter = newQueryReadEmitter(ctx)

        val regular = emitter.generateReadStatementResultFunction("person", statement).toString()
        val joined = emitter.generateReadJoinedStatementResultFunction("person", statement).toString()

        assertTrue(regular.contains("personSummaryRowMapper"))
        assertTrue(regular.contains("sqlValueToBirthDate"))
        assertFalse(joined.contains("personSummaryRowMapper"))
        assertTrue(joined.contains("sqlValueToBirthDate"))
    }

    @Test
    @DisplayName("QueryExecuteEmitter uses collection branches for executeAsList, executeAsOne, and executeAsOneOrNull")
    fun queryExecuteEmitterCoversCollectionBranches() {
        val statement = annotatedSelectStatement(
            name = "SelectWithAddresses",
            src = selectStatement(
                fields = listOf(
                    fieldSource("person_id", "p", originalColumnName = "id", dataType = "INTEGER"),
                    fieldSource("address__id", "a", originalColumnName = "id", dataType = "INTEGER", isNullable = true),
                    fieldSource("address__city", "a", originalColumnName = "city", isNullable = true),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person", "a" to "address"),
                joinConditions = listOf(
                    dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement.JoinCondition(
                        leftTable = "p",
                        leftColumn = "id",
                        rightTable = "a",
                        rightColumn = "person_id",
                    )
                ),
            ),
            fields = listOf(
                regularField("person_id", "p", originalColumnName = "id", dataType = "INTEGER"),
                regularField("address__id", "a", originalColumnName = "id", dataType = "INTEGER", isNullable = true),
                regularField("address__city", "a", originalColumnName = "city", isNullable = true),
                dynamicField(
                    fieldName = "addresses",
                    mappingType = "collection",
                    propertyType = "List<PersonAddressRow>",
                    sourceTable = "a",
                    aliasPrefix = "address__",
                    collectionKey = "address__id",
                ),
            ),
            queryResult = "PersonWithAddressesRow",
            collectionKey = "person_id",
        )
        val ctx = generatorContext(selectStatements = listOf(statement))
        val emitter = newQueryExecuteEmitter(ctx) { builder, _, _, _, _, _ ->
            builder.line("collectionResults()")
        }

        val asList = emitter.generateSelectQueryFunction("person", statement, "executeAsList").toString()
        val asOne = emitter.generateSelectQueryFunction("person", statement, "executeAsOne").toString()
        val asOneOrNull = emitter.generateSelectQueryFunction("person", statement, "executeAsOneOrNull").toString()

        assertTrue(asList.contains("collectionResults()"))
        assertTrue(asOne.contains("val results = run {"))
        assertTrue(asOne.contains("collectionResults()"))
        assertTrue(asOne.contains("results.isEmpty() -> throw IllegalStateException"))
        assertTrue(asOneOrNull.contains("val results = run {"))
        assertTrue(asOneOrNull.contains("results.isEmpty() -> null"))
    }

    @Test
    @DisplayName("QueryExecuteEmitter uses queryResult names for RETURNING statements and wires result adapters")
    fun queryExecuteEmitterUsesReturningResultNames() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("id", "INTEGER"),
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val statement = annotatedInsertStatement(
            name = "AddReturning",
            table = "person",
            namedParameters = listOf("birthDate"),
            parameterToColumnNames = mapOf("birthDate" to "birth_date"),
            returningColumns = listOf("id", "birth_date"),
            annotations = statementAnnotations(queryResult = "CreatedPersonRow"),
            sql = "INSERT INTO person (birth_date) VALUES (?) RETURNING id, birth_date",
        )
        val ctx = generatorContext(createTableStatements = listOf(personTable))
        val emitter = newQueryExecuteEmitter(ctx)

        val rendered = emitter.generateExecuteQueryFunction("person", statement, "executeReturningOne").toString()

        assertTrue(rendered.contains("executeReturningOne("))
        assertTrue(rendered.contains("fixture.db.PersonQuery.AddReturning"))
        assertTrue(rendered.contains("sqlValueToBirthDate"))
        assertTrue(rendered.contains(": fixture.db.CreatedPersonRow"))
    }

    @Test
    @DisplayName("QueryFunctionScaffolder adds shared receiver and parameter structure deterministically")
    fun queryFunctionScaffolderBuildsSharedFunctionStructure() {
        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val statement = annotatedSelectStatement(
            name = "SelectByBirthDate",
            src = selectStatementWithParameters(
                fields = listOf(fieldSource("birth_date", "p", originalColumnName = "birth_date")),
                namedParameters = listOf("birthDateStart"),
                namedParametersToColumns = linkedMapOf(
                    "birthDateStart" to dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn.Default("birth_date"),
                ),
                fromTable = "person",
                tableAliases = mapOf("p" to "person"),
            ),
            fields = listOf(regularField("birth_date", "p", originalColumnName = "birth_date")),
        )
        val ctx = generatorContext(createTableStatements = listOf(personTable), selectStatements = listOf(statement))
        val adapterEmitter = AdapterParameterEmitter(ctx)
        val scaffolder = QueryFunctionScaffolder("fixture.db", { "PersonQuery" }, adapterEmitter)

        val bindFunction = com.squareup.kotlinpoet.FunSpec.builder("bind")
        scaffolder.setupStatementFunctionStructure(
            fnBld = bindFunction,
            statement = statement,
            namespace = "person",
            className = statement.getDataClassName(),
            includeParamsParameter = true,
            adapterType = QueryFunctionScaffolder.AdapterType.PARAMETER_BINDING,
        )
        val rendered = bindFunction.build().toString()

        assertTrue(rendered.contains("PersonQuery.SelectByBirthDate.bind("))
        assertTrue(rendered.contains("statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement"))
        assertTrue(rendered.contains("params: fixture.db.PersonQuery.SelectByBirthDate.Params"))
        assertTrue(rendered.contains("birthDateToSqlValue"))
    }

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("QueryBindEmitter skips params for no-parameter statements and emits debug bind logging when needed")
    fun queryBindEmitterHandlesNoParamsAndDebugLogging() {
        val queriesDir = tempDir.resolve("queries").toFile().apply {
            resolve("person").mkdirs()
        }
        val outputDir = tempDir.resolve("output").toFile().apply { mkdirs() }
        File(queriesDir, "person/selectAll.sql").writeText(
            """
            -- @@{name=SelectAll}
            SELECT birth_date FROM person;
            """.trimIndent()
        )
        File(queriesDir, "person/selectByBirthDate.sql").writeText(
            """
            -- @@{name=SelectByBirthDate}
            SELECT birth_date
            FROM person
            WHERE birth_date = :birthDate;
            """.trimIndent()
        )

        val personTable = annotatedCreateTable(
            tableName = "person",
            columns = listOf(
                annotatedTableColumn("birth_date", "TEXT", notNull = false, adapter = true),
            ),
        )
        val connection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        connection.createStatement().execute("CREATE TABLE person (birth_date TEXT)")

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = connection,
            queriesDir = queriesDir,
            createTableStatements = listOf(personTable),
            packageName = "fixture.db",
            outputDir = outputDir,
        )
        val ctx = GeneratorContext(
            packageName = "fixture.db",
            outputDir = outputDir,
            createTableStatements = listOf(personTable),
            createViewStatements = emptyList(),
            nsWithStatements = dataStructGenerator.nsWithStatements,
        )
        val scaffolder = QueryFunctionScaffolder("fixture.db", { "PersonQuery" }, AdapterParameterEmitter(ctx))
        val parameterBinding = ParameterBinding(
            columnLookup = ctx.columnLookup,
            typeMapping = ctx.typeMapping,
            dataStructCodeGenerator = dataStructGenerator,
            debug = true,
        )
        val emitter = QueryBindEmitter(parameterBinding, scaffolder, debug = true)
        val statements = dataStructGenerator.nsWithStatements.getValue("person")
            .filterIsInstance<dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement>()
        val selectAll = statements.first { it.src.namedParameters.isEmpty() }
        val selectByBirthDate = statements.first { it.src.namedParameters.isNotEmpty() }

        val noParams = emitter.generateBindStatementParamsFunction("person", selectAll).toString()
        val debugBind = emitter.generateBindStatementParamsFunction("person", selectByBirthDate).toString()

        assertFalse(noParams.contains("params:"))
        assertFalse(noParams.contains("__paramsLog"))
        assertTrue(debugBind.contains("val __paramsLog = mutableListOf<String>()"))
        assertTrue(debugBind.contains("sqliteNowLogger.d"))
        assertTrue(debugBind.contains("sqliteNowPreview"))
    }

    private fun newQueryReadEmitter(ctx: GeneratorContext): QueryReadEmitter {
        val adapterEmitter = AdapterParameterEmitter(ctx)
        val scaffolder = QueryFunctionScaffolder("fixture.db", { value -> value.replaceFirstChar(Char::uppercaseChar) + "Query" }, adapterEmitter)
        val getterFactory = GetterCallFactory(
            adapterConfig = ctx.adapterConfig,
            adapterNameResolver = ctx.adapterNameResolver,
            selectFieldGenerator = ctx.selectFieldGenerator,
            typeMapping = ctx.typeMapping,
        )
        val mappingHelper = ResultMappingHelper(ctx, ctx.selectFieldGenerator, ctx.adapterConfig)
        return QueryReadEmitter(
            packageName = "fixture.db",
            queryNamespaceName = { value -> value.replaceFirstChar(Char::uppercaseChar) + "Query" },
            scaffolder = scaffolder,
            adapterParameterEmitter = adapterEmitter,
            adapterConfig = ctx.adapterConfig,
            selectFieldGenerator = ctx.selectFieldGenerator,
            typeMapping = ctx.typeMapping,
            resultMappingHelper = mappingHelper,
            generateGetterCallWithPrefixes = getterFactory::buildGetterCall,
            generateDynamicFieldMappingFromJoined = { _, _ -> "dynamicValue()" },
            createSelectLikeFieldsFromExecuteReturning = { emptyList() },
            findMainTableAlias = ctx::findMainTableAlias,
        )
    }

    private fun newQueryExecuteEmitter(
        ctx: GeneratorContext,
        collectionBuilder: (
            IndentedCodeBuilder,
            dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement,
            String,
            String,
            String,
            String?,
        ) -> Unit = { builder, _, _, _, _, _ -> builder.line("collectionResults()") },
    ): QueryExecuteEmitter {
        val adapterEmitter = AdapterParameterEmitter(ctx)
        val scaffolder = QueryFunctionScaffolder("fixture.db", { value -> value.replaceFirstChar(Char::uppercaseChar) + "Query" }, adapterEmitter)
        return QueryExecuteEmitter(
            packageName = "fixture.db",
            debug = false,
            scaffolder = scaffolder,
            adapterParameterEmitter = adapterEmitter,
            queryNamespaceName = { value -> value.replaceFirstChar(Char::uppercaseChar) + "Query" },
            collectionMappingBuilder = collectionBuilder,
        )
    }
}
