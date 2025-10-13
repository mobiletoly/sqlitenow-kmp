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
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.DatabaseConfig
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.model.ObjectFactory
import javax.inject.Inject

open class SqliteNowExtension @Inject constructor(
    objects: ObjectFactory
) {
    /** Container for multiple DatabaseConfig entries */
    val databases: NamedDomainObjectContainer<DatabaseConfig> =
        objects.domainObjectContainer(DatabaseConfig::class.java) { name ->
            DatabaseConfig(name, objects)
        }  // DSL container created via ObjectFactory  [oai_citation_attribution:8‡Gradle Documentation](https://docs.gradle.org/current/kotlin-dsl/gradle/org.gradle.api/-named-domain-object-container/index.html?utm_source=chatgpt.com)
}
