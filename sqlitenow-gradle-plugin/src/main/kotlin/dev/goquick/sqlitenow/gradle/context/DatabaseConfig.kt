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
package dev.goquick.sqlitenow.gradle.context

import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property
import javax.inject.Inject

open class DatabaseConfig @Inject constructor(
    /** The container will pass the database name here. */
    val name: String,
    objects: ObjectFactory
) {
    /** The package into which code will be generated */
    val packageName: Property<String> = objects.property(String::class.java)
        .convention("")
    /** Path to a file where to store the schema database */
    val schemaDatabaseFile: RegularFileProperty = objects.fileProperty()
    /**
     * Set to true, if you want to enable troubleshooting features, e.g. in case of
     * SQL exceptions you will see the full stack trace. Might slightly affect performance.
     */
    val debug: Property<Boolean> = objects.property(Boolean::class.java)
        .convention(false)
}