/*
 * Copyright 2025 Anatoliy Pochkin
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
package dev.goquick.sqlitenow.gradle.sqlite

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SqlTranslatorTest {

    @Test
    @DisplayName("Test translating SQL without dollar signs")
    fun testTranslateSqlWithoutDollarSigns() {
        val sql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """.trimIndent()

        val result = translateSqliteStatementToKotlin(sql)

        // The result should not have the $$ prefix
        assertEquals(
            "|CREATE TABLE test_table (\n" +
            "|id INTEGER PRIMARY KEY,\n" +
            "|name TEXT NOT NULL,\n" +
            "|created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
            "|);",
            result
        )
    }

    @Test
    @DisplayName("Test translating SQL with dollar signs")
    fun testTranslateSqlWithDollarSigns() {
        val sql = """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                price REAL DEFAULT 0.0,
                currency TEXT DEFAULT '$',
                description TEXT
            );
        """.trimIndent()

        val result = translateSqliteStatementToKotlin(sql)

        // The result should not have the $$ prefix since we've rolled back that implementation
        assertEquals(
            "|CREATE TABLE test_table (\n" +
            "|id INTEGER PRIMARY KEY,\n" +
            "|price REAL DEFAULT 0.0,\n" +
            "|currency TEXT DEFAULT '$',\n" +
            "|description TEXT\n" +
            "|);",
            result
        )
    }

    @Test
    @DisplayName("Test translating SQL with multiple dollar signs")
    fun testTranslateSqlWithMultipleDollarSigns() {
        val sql = """
            INSERT INTO products (name, price, currency, discount_code)
            VALUES ('Product 1', 19.99, '$', 'SAVE$10');
        """.trimIndent()

        val result = translateSqliteStatementToKotlin(sql)

        // The result should not have the $$ prefix since we've rolled back that implementation
        assertEquals(
            "|INSERT INTO products (name, price, currency, discount_code)\n" +
            "|VALUES ('Product 1', 19.99, '$', 'SAVE$10');",
            result
        )
    }
}
