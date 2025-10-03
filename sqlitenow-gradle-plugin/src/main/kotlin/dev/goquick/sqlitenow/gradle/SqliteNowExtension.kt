package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.DatabaseConfig
import javax.inject.Inject
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

open class SqliteNowExtension @Inject constructor(
    objects: ObjectFactory
) {
    /** Container for multiple DatabaseConfig entries */
    val databases: NamedDomainObjectContainer<DatabaseConfig> =
        objects.domainObjectContainer(DatabaseConfig::class.java) { name ->
            DatabaseConfig(name, objects)
        }  // DSL container created via ObjectFactory  [oai_citation_attribution:8‡Gradle Documentation](https://docs.gradle.org/current/kotlin-dsl/gradle/org.gradle.api/-named-domain-object-container/index.html?utm_source=chatgpt.com)
}
