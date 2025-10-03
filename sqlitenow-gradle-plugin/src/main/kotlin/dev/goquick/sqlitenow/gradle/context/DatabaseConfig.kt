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