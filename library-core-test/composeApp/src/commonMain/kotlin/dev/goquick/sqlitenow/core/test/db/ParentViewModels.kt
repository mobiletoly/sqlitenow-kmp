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
package dev.goquick.sqlitenow.core.test.db

/**
 * Lightweight data holders referenced by generated SQLiteNow code for the parent_with_children view.
 * Defining them in shared code keeps generated files simple while giving instrumentation tests
 * strongly-typed access to the mapped fields.
 */
data class ParentMainDoc(
    val id: Long,
    val docId: String,
    val categoryId: Long,
)

data class ParentCategoryDoc(
    val id: Long,
    val docId: String,
    val title: String,
)

data class ParentChildDoc(
    val id: Long,
    val parentDocId: String,
    val title: String,
)
