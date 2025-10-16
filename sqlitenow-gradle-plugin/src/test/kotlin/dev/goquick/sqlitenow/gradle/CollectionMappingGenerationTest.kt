package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.io.path.createTempDirectory
import kotlin.test.assertTrue

class CollectionMappingGenerationTest {
    @Test
    fun generates_grouping_and_distinct_for_collection_mapping() {
        val root = createTempDirectory(prefix = "coll-map-int-").toFile()
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queriesDir = File(root, "queries").apply { mkdirs() }

        // Schema: person + address
        File(schemaDir, "person.sql").writeText(
            """
            CREATE TABLE person (
              id BLOB PRIMARY KEY NOT NULL,
              doc_id TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )
        File(schemaDir, "address.sql").writeText(
            """
            CREATE TABLE address (
              address_id TEXT PRIMARY KEY NOT NULL,
              person_doc_id TEXT NOT NULL,
              city TEXT NOT NULL
            );
            """.trimIndent()
        )

        // Provide shared result for Address
        val addrDir = File(queriesDir, "address").apply { mkdirs() }
        File(addrDir, "selectAll.sql").writeText(
            """
            -- @@{ queryResult=Row }
            SELECT address_id, person_doc_id, city FROM address;
            """.trimIndent()
        )

        // Person query with collection mapping from alias a (aliasPrefix=addr_)
        val personDir = File(queriesDir, "person").apply { mkdirs() }
        File(personDir, "selectWithAddresses.sql").writeText(
            """
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
            """.trimIndent()
        )

        val outDir = File(root, "out").apply { mkdirs() }
        generateDatabaseFiles(
            dbName = "TestDb",
            sqlDir = root,
            packageName = "dev.test",
            outDir = outDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        val genFile = outDir.walkTopDown().first { it.name.contains("PersonQuery_SelectWithAddresses") && it.extension == "kt" }
        val text = genFile.readText()

        // Verify grouping and distinct are present
        val hasGroupBy = text.contains("joinedRows.groupBy") || text.contains(".groupBy { row ->")
        assertTrue(hasGroupBy, "Should group joined rows")
        assertTrue(text.contains(".distinctBy"), "Should distinct collection items by unique key")
        // Basic sanity: file contains mapping of collection items
        assertTrue(text.contains("addresses ="), "Should map collection items from joined rows")
    }

    @Test
    fun per_row_dynamic_field_is_included_alongside_collection() {
        val root = createTempDirectory(prefix = "perrow-map-int-").toFile()
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queriesDir = File(root, "queries").apply { mkdirs() }

        File(schemaDir, "person.sql").writeText(
            """
            CREATE TABLE person (
              id BLOB PRIMARY KEY NOT NULL,
              doc_id TEXT NOT NULL UNIQUE
            ) WITHOUT ROWID;
            """.trimIndent()
        )
        File(schemaDir, "detail.sql").writeText(
            """
            CREATE TABLE detail (
              doc_id TEXT PRIMARY KEY NOT NULL,
              info TEXT NOT NULL
            );
            """.trimIndent()
        )

        val detDir = File(queriesDir, "detail").apply { mkdirs() }
        File(detDir, "selectAll.sql").writeText(
            """
            -- @@{ queryResult=Row }
            SELECT doc_id, info FROM detail;
            """.trimIndent()
        )

        val personDir = File(queriesDir, "person").apply { mkdirs() }
        File(personDir, "selectWithDetailAndList.sql").writeText(
            """
            /* @@{ queryResult=RowWithDetail, collectionKey=doc_id } */
            SELECT
              p.*, d.doc_id AS det_doc_id, d.info AS det_info

            /* @@{ dynamicField=primaryDetail,
                   mappingType=perRow,
                   propertyType=DetailQuery.SharedResult.Row,
                   sourceTable=d,
                   aliasPrefix=det_ } */

            FROM person p LEFT JOIN detail d ON p.doc_id = d.doc_id;
            """.trimIndent()
        )

        val outDir = File(root, "out").apply { mkdirs() }
        generateDatabaseFiles("TestDb2", root, "dev.test2", outDir, null, false)

        val genFile = outDir.walkTopDown().first { it.name.contains("PersonQuery_SelectWithDetailAndList") && it.extension == "kt" }
        val text = genFile.readText()
        assertTrue(text.contains("primaryDetail ="), "Per-row dynamic field should be constructed in mapped result")
    }

    @Test
    fun per_row_dynamic_field_is_guarded_when_collections_present() {
        val root = createTempDirectory(prefix = "perrow-guard-").toFile()
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queriesDir = File(root, "queries").apply { mkdirs() }

        File(schemaDir, "person.sql").writeText(
            """
            CREATE TABLE person (
              id BLOB PRIMARY KEY NOT NULL,
              doc_id TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )
        File(schemaDir, "address.sql").writeText(
            """
            CREATE TABLE address (
              address_id TEXT PRIMARY KEY NOT NULL,
              person_doc_id TEXT NOT NULL,
              city TEXT NOT NULL
            );
            """.trimIndent()
        )
        File(schemaDir, "detail.sql").writeText(
            """
            CREATE TABLE detail (
              doc_id TEXT PRIMARY KEY NOT NULL,
              info TEXT NOT NULL
            );
            """.trimIndent()
        )

        val addressQueries = File(queriesDir, "address").apply { mkdirs() }
        File(addressQueries, "selectAll.sql").writeText(
            """
            -- @@{ queryResult=Row }
            SELECT address_id, person_doc_id, city FROM address;
            """.trimIndent()
        )

        val detailQueries = File(queriesDir, "detail").apply { mkdirs() }
        File(detailQueries, "selectAll.sql").writeText(
            """
            -- @@{ queryResult=Row }
            SELECT doc_id, info FROM detail;
            """.trimIndent()
        )

        val personQueries = File(queriesDir, "person").apply { mkdirs() }
        File(personQueries, "selectWithAddressesAndDetail.sql").writeText(
            """
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
            """.trimIndent()
        )

        val outDir = File(root, "out").apply { mkdirs() }
        generateDatabaseFiles(
            dbName = "GuardDb",
            sqlDir = root,
            packageName = "dev.guard",
            outDir = outDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        val genFile = outDir.walkTopDown().first {
            it.name.contains("PersonQuery_SelectWithAddressesAndDetail") && it.extension == "kt"
        }
        val text = genFile.readText()
        assertTrue(
            text.contains("primaryDetail = rowsForEntity.firstOrNull"),
            "Per-row mapping inside collection context should access rowsForEntity with a firstOrNull guard",
        )
    }

    @Test
    fun generates_correct_distinctBy_path_for_custom_dynamic_field_names() {
        val root = createTempDirectory(prefix = "custom-field-names-").toFile()
        val schemaDir = File(root, "schema").apply { mkdirs() }
        val queriesDir = File(root, "queries").apply { mkdirs() }

        // Schema: product + review
        File(schemaDir, "product.sql").writeText(
            """
            CREATE TABLE product (
              id BLOB PRIMARY KEY NOT NULL,
              doc_id TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL
            ) WITHOUT ROWID;
            """.trimIndent()
        )
        File(schemaDir, "review.sql").writeText(
            """
            CREATE TABLE review (
              review_id TEXT PRIMARY KEY NOT NULL,
              product_doc_id TEXT NOT NULL,
              rating INTEGER NOT NULL,
              comment TEXT NOT NULL
            );
            """.trimIndent()
        )

        // Review query for shared result with dynamic field
        val reviewDir = File(queriesDir, "review").apply { mkdirs() }
        File(reviewDir, "selectAll.sql").writeText(
            """
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
            """.trimIndent()
        )

        // Product basic query for the entity mapping
        val productDir = File(queriesDir, "product").apply { mkdirs() }
        File(productDir, "selectBasic.sql").writeText(
            """
            -- @@{ queryResult=BasicRow }
            SELECT doc_id, name FROM product;
            """.trimIndent()
        )

        // Product query with collection mapping using custom dynamic field name "customerReviews"
        File(productDir, "selectWithReviews.sql").writeText(
            """
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
            """.trimIndent()
        )

        val outDir = File(root, "out").apply { mkdirs() }
        generateDatabaseFiles(
            dbName = "TestDb",
            sqlDir = root,
            packageName = "dev.test",
            outDir = outDir,
            schemaDatabaseFile = null,
            debug = false,
        )

        val genFile = outDir.walkTopDown().first { it.name.contains("ProductQuery_SelectWithReviews") && it.extension == "kt" }
        val text = genFile.readText()

        // Verify basic collection mapping functionality
        assertTrue(text.contains(".distinctBy"), "Should generate distinctBy for collection deduplication")
        assertTrue(text.contains("customerReviews ="), "Should map collection items to 'customerReviews' field")
        val hasNestedGroupBy = text.contains(".groupBy { row ->") || text.contains(".groupBy { it")
        assertTrue(hasNestedGroupBy, "Should group joined rows")
    }
}
