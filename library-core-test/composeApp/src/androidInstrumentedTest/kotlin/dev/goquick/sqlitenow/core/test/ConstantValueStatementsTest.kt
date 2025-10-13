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
package dev.goquick.sqlitenow.core.test

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.test.db.AddressType
import dev.goquick.sqlitenow.core.test.db.LibraryTestDatabase
import dev.goquick.sqlitenow.core.test.db.PersonAddressQuery
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Integration coverage ensuring constant literals across INSERT/UPDATE/DELETE/SELECT
 * statements are accepted and mapped correctly by the generated runtime.
 */
@RunWith(AndroidJUnit4::class)
class ConstantValueStatementsTest {

    private lateinit var database: LibraryTestDatabase

    @Before
    fun setup() {
        database = TestDatabaseHelper.createDatabase()
    }

    @After
    fun teardown() {
        runBlocking {
            database.close()
        }
    }

    @Test
    fun testConstantAssignments() {
        runBlocking {
            database.open()

            val person = database.person.add.one(
                PersonQuery.Add.Params(
                    email = "constant@example.com",
                    firstName = "Constant",
                    lastName = "Assignments",
                    phone = "+1000000000",
                    birthDate = LocalDate(1995, 1, 1)
                )
            )

            val insertedAddress = database.personAddress.addWithConstants.one(
                PersonAddressQuery.AddWithConstants.Params(
                    personId = person.id,
                    addressType = AddressType.HOME,
                    street = "123 Constant Ave",
                    city = "Literal City"
                )
            )

            assertEquals(person.id, insertedAddress.personId)
            assertEquals(AddressType.HOME, insertedAddress.addressType)
            assertEquals("US", insertedAddress.country)
            assertTrue(insertedAddress.isPrimary)
            assertNull("State should be null on insert", insertedAddress.state)
            assertNotNull("Created at should be populated from CURRENT_TIMESTAMP", insertedAddress.createdAt)

            val updatedAddress = database.personAddress.updateConstantFlags.one(
                PersonAddressQuery.UpdateConstantFlags.Params(
                    state = "CA",
                    addressId = insertedAddress.id
                )
            )

            assertEquals("CA", updatedAddress.state)
            assertEquals("CA", updatedAddress.country)
            assertFalse(updatedAddress.isPrimary)
            assertNotNull("Created at should be refreshed on update", updatedAddress.createdAt)

            val selectResults = database.personAddress.selectWithConstantColumns(
                PersonAddressQuery.SelectWithConstantColumns.Params(
                    personId = person.id
                )
            ).asList()

            assertEquals(1, selectResults.size)
            val selectRow = selectResults.first()
            assertEquals(updatedAddress.id, selectRow.id)
            assertFalse(selectRow.isPrimary)
            val constantInt = selectRow.constantInt
            assertNotNull(constantInt)
            assertEquals(42, constantInt!!)
            val constantReal = selectRow.constantReal
            assertNotNull(constantReal)
            assertEquals(3.14, constantReal!!, 0.0001)
            assertNotNull(selectRow.constantTimestamp)
            assertNull(selectRow.constantNull)

            database.personAddress.deleteSecondaryByPerson(
                PersonAddressQuery.DeleteSecondaryByPerson.Params(
                    personId = person.id
                )
            )

            val remaining = database.personAddress.selectWithConstantColumns(
                PersonAddressQuery.SelectWithConstantColumns.Params(
                    personId = person.id
                )
            ).asList()
            assertTrue(remaining.isEmpty())
        }
    }
}
