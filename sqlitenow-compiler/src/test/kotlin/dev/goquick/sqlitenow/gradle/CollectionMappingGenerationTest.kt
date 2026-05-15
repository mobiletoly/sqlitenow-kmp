package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertTrue

class CollectionMappingGenerationTest {
    @TestFactory
    fun collectionMappingScenarios(): List<DynamicTest> {
        return listOf(
            CodegenCase(
                name = "generates grouping and distinct for collection mapping",
                prefix = "coll-map-int-",
                schemas = listOf(
                    FixtureFile(
                        path = "person.sql",
                        text = """
                            CREATE TABLE person (
                              id BLOB PRIMARY KEY NOT NULL,
                              doc_id TEXT NOT NULL UNIQUE,
                              name TEXT NOT NULL
                            ) WITHOUT ROWID;
                        """.trimIndent(),
                    ),
                    FixtureFile(
                        path = "address.sql",
                        text = """
                            CREATE TABLE address (
                              address_id TEXT PRIMARY KEY NOT NULL,
                              person_doc_id TEXT NOT NULL,
                              city TEXT NOT NULL
                            );
                        """.trimIndent(),
                    ),
                ),
                queries = listOf(
                    QueryFile(
                        namespace = "address",
                        path = "selectAll.sql",
                        text = """
                            -- @@{ queryResult=Row }
                            SELECT address_id, person_doc_id, city FROM address;
                        """.trimIndent(),
                    ),
                    QueryFile(
                        namespace = "person",
                        path = "selectWithAddresses.sql",
                        text = """
                            /* @@{ queryResult=RowWithAddresses, collectionKey=doc_id } */
                            SELECT
                              p.*,
                              a.address_id AS addr_address_id,
                              a.person_doc_id AS addr_person_doc_id,
                              a.city AS addr_city

                            /* @@{ dynamicField=addresses,
                                   mappingType=collection,
                                   propertyType=List<AddressQuery.SharedResult.Row>,
                                   sourceTable=a,
                                   collectionKey=addr_address_id,
                                   aliasPrefix=addr_ } */

                            FROM person p LEFT JOIN address a ON p.doc_id = a.person_doc_id;
                        """.trimIndent(),
                    ),
                ),
                generatedFileNamePart = "PersonQuery_SelectWithAddresses",
                verifyGeneratedText = { text ->
                    assertHasGroupBy(text)
                    assertTrue(text.contains(".distinctBy"), "Should distinct collection items by unique key")
                    assertTrue(text.contains("addresses ="), "Should map collection items from joined rows")
                },
            ),
            CodegenCase(
                name = "includes per-row dynamic field alongside collection infrastructure",
                prefix = "perrow-map-int-",
                dbName = "TestDb2",
                packageName = "dev.test2",
                schemas = listOf(
                    FixtureFile(
                        path = "person.sql",
                        text = """
                            CREATE TABLE person (
                              id BLOB PRIMARY KEY NOT NULL,
                              doc_id TEXT NOT NULL UNIQUE
                            ) WITHOUT ROWID;
                        """.trimIndent(),
                    ),
                    FixtureFile(
                        path = "detail.sql",
                        text = """
                            CREATE TABLE detail (
                              doc_id TEXT PRIMARY KEY NOT NULL,
                              info TEXT NOT NULL
                            );
                        """.trimIndent(),
                    ),
                ),
                queries = listOf(
                    QueryFile(
                        namespace = "detail",
                        path = "selectAll.sql",
                        text = """
                            -- @@{ queryResult=Row }
                            SELECT doc_id, info FROM detail;
                        """.trimIndent(),
                    ),
                    QueryFile(
                        namespace = "person",
                        path = "selectWithDetailAndList.sql",
                        text = """
                            /* @@{ queryResult=RowWithDetail, collectionKey=doc_id } */
                            SELECT
                              p.*, d.doc_id AS det_doc_id, d.info AS det_info

                            /* @@{ dynamicField=primaryDetail,
                                   mappingType=perRow,
                                   propertyType=DetailQuery.SharedResult.Row,
                                   sourceTable=d,
                                   aliasPrefix=det_ } */

                            FROM person p LEFT JOIN detail d ON p.doc_id = d.doc_id;
                        """.trimIndent(),
                    ),
                ),
                generatedFileNamePart = "PersonQuery_SelectWithDetailAndList",
                verifyGeneratedText = { text ->
                    assertTrue(
                        text.contains("primaryDetail ="),
                        "Per-row dynamic field should be constructed in mapped result",
                    )
                },
            ),
            CodegenCase(
                name = "guards per-row dynamic field when collections are present",
                prefix = "perrow-guard-",
                dbName = "GuardDb",
                packageName = "dev.guard",
                schemas = listOf(
                    FixtureFile(
                        path = "person.sql",
                        text = """
                            CREATE TABLE person (
                              id BLOB PRIMARY KEY NOT NULL,
                              doc_id TEXT NOT NULL UNIQUE,
                              name TEXT NOT NULL
                            ) WITHOUT ROWID;
                        """.trimIndent(),
                    ),
                    FixtureFile(
                        path = "address.sql",
                        text = """
                            CREATE TABLE address (
                              address_id TEXT PRIMARY KEY NOT NULL,
                              person_doc_id TEXT NOT NULL,
                              city TEXT NOT NULL
                            );
                        """.trimIndent(),
                    ),
                    FixtureFile(
                        path = "detail.sql",
                        text = """
                            CREATE TABLE detail (
                              doc_id TEXT PRIMARY KEY NOT NULL,
                              info TEXT NOT NULL
                            );
                        """.trimIndent(),
                    ),
                ),
                queries = listOf(
                    QueryFile(
                        namespace = "address",
                        path = "selectAll.sql",
                        text = """
                            -- @@{ queryResult=Row }
                            SELECT address_id, person_doc_id, city FROM address;
                        """.trimIndent(),
                    ),
                    QueryFile(
                        namespace = "detail",
                        path = "selectAll.sql",
                        text = """
                            -- @@{ queryResult=Row }
                            SELECT doc_id, info FROM detail;
                        """.trimIndent(),
                    ),
                    QueryFile(
                        namespace = "person",
                        path = "selectWithAddressesAndDetail.sql",
                        text = """
                            /* @@{ queryResult=RowWithData, collectionKey=doc_id } */
                            SELECT
                              p.*,
                              a.address_id AS addr_address_id,
                              a.person_doc_id AS addr_person_doc_id,
                              a.city AS addr_city,
                              d.doc_id AS det_doc_id,
                              d.info AS det_info

                            /* @@{ dynamicField=addresses,
                                   mappingType=collection,
                                   propertyType=List<AddressQuery.SharedResult.Row>,
                                   sourceTable=a,
                                   collectionKey=addr_address_id,
                                   aliasPrefix=addr_ } */

                            /* @@{ dynamicField=primaryDetail,
                                   mappingType=perRow,
                                   propertyType=DetailQuery.SharedResult.Row,
                                   sourceTable=d,
                                   aliasPrefix=det_ } */

                            FROM person p
                            LEFT JOIN address a ON p.doc_id = a.person_doc_id
                            LEFT JOIN detail d ON p.doc_id = d.doc_id;
                        """.trimIndent(),
                    ),
                ),
                generatedFileNamePart = "PersonQuery_SelectWithAddressesAndDetail",
                verifyGeneratedText = { text ->
                    assertTrue(
                        text.contains("primaryDetail = rowsForEntity.firstOrNull"),
                        "Per-row mapping inside collection context should access rowsForEntity with a firstOrNull guard",
                    )
                },
            ),
            CodegenCase(
                name = "generates correct distinctBy path for custom dynamic field names",
                prefix = "custom-field-names-",
                schemas = listOf(
                    FixtureFile(
                        path = "product.sql",
                        text = """
                            CREATE TABLE product (
                              id BLOB PRIMARY KEY NOT NULL,
                              doc_id TEXT NOT NULL UNIQUE,
                              name TEXT NOT NULL
                            ) WITHOUT ROWID;
                        """.trimIndent(),
                    ),
                    FixtureFile(
                        path = "review.sql",
                        text = """
                            CREATE TABLE review (
                              review_id TEXT PRIMARY KEY NOT NULL,
                              product_doc_id TEXT NOT NULL,
                              rating INTEGER NOT NULL,
                              comment TEXT NOT NULL
                            );
                        """.trimIndent(),
                    ),
                ),
                queries = listOf(
                    QueryFile(
                        namespace = "review",
                        path = "selectAll.sql",
                        text = """
                            -- @@{ queryResult=DetailedRow }
                            SELECT
                              r.review_id,
                              r.product_doc_id,
                              r.rating,
                              r.comment,
                              p.name AS product_name

                            /* @@{ dynamicField=productInfo,
                                   mappingType=entity,
                                   propertyType=ProductQuery.SharedResult.BasicRow,
                                   sourceTable=p,
                                   aliasPrefix=product_ } */

                            FROM review r
                            LEFT JOIN product p ON r.product_doc_id = p.doc_id;
                        """.trimIndent(),
                    ),
                    QueryFile(
                        namespace = "product",
                        path = "selectBasic.sql",
                        text = """
                            -- @@{ queryResult=BasicRow }
                            SELECT doc_id, name FROM product;
                        """.trimIndent(),
                    ),
                    QueryFile(
                        namespace = "product",
                        path = "selectWithReviews.sql",
                        text = """
                            /* @@{ queryResult=ProductWithReviewsRow, collectionKey=doc_id } */
                            SELECT
                              p.*,
                              r.review_id AS rev_review_id,
                              r.product_doc_id AS rev_product_doc_id,
                              r.rating AS rev_rating,
                              r.comment AS rev_comment,
                              p2.name AS rev_product_name

                            /* @@{ dynamicField=customerReviews,
                                   mappingType=collection,
                                   propertyType=List<ReviewQuery.SharedResult.DetailedRow>,
                                   sourceTable=r,
                                   collectionKey=rev_review_id,
                                   aliasPrefix=rev_ } */

                            FROM product p
                            LEFT JOIN review r ON p.doc_id = r.product_doc_id
                            LEFT JOIN product p2 ON r.product_doc_id = p2.doc_id;
                        """.trimIndent(),
                    ),
                ),
                generatedFileNamePart = "ProductQuery_SelectWithReviews",
                verifyGeneratedText = { text ->
                    assertTrue(text.contains(".distinctBy"), "Should generate distinctBy for collection deduplication")
                    assertTrue(text.contains("customerReviews ="), "Should map collection items to 'customerReviews' field")
                    assertHasGroupBy(text)
                },
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.name) {
                runCase(case)
            }
        }
    }

    private fun runCase(case: CodegenCase) {
        val fixture = CodegenFixture.create(
            prefix = case.prefix,
            dbName = case.dbName,
            packageName = case.packageName,
        )
        case.schemas.forEach { fixture.writeSchema(it.path, it.text) }
        case.queries.forEach { fixture.writeQuery(it.namespace, it.path, it.text) }

        fixture.generate()

        case.verifyGeneratedText(fixture.generatedTextContaining(case.generatedFileNamePart))
    }

    private fun assertHasGroupBy(text: String) {
        val hasGroupBy = text.contains("joinedRows.groupBy") ||
            text.contains(".groupBy { row ->") ||
            text.contains(".groupBy { it")
        assertTrue(hasGroupBy, "Should group joined rows")
    }

    private data class CodegenCase(
        val name: String,
        val prefix: String,
        val dbName: String = "TestDb",
        val packageName: String = "dev.test",
        val schemas: List<FixtureFile>,
        val queries: List<QueryFile>,
        val generatedFileNamePart: String,
        val verifyGeneratedText: (String) -> Unit,
    )

    private data class FixtureFile(
        val path: String,
        val text: String,
    )

    private data class QueryFile(
        val namespace: String,
        val path: String,
        val text: String,
    )
}
