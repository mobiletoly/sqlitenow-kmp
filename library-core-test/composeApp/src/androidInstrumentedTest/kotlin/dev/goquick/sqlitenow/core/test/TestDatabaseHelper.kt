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

/**
 * Helper object to create LibraryTestDatabase instances with all required adapters.
 * This ensures consistent database setup across all test files.
 */
object TestDatabaseHelper {
    
    /**
     * Creates a LibraryTestDatabase instance with all required adapters configured.
     * Uses the working configuration from BasicCollectionTest as the reference.
     */
    fun createDatabase(dbName: String = ":memory:", debug: Boolean = true) =
        createLibraryTestDatabase(dbName = dbName, debug = debug)
}
