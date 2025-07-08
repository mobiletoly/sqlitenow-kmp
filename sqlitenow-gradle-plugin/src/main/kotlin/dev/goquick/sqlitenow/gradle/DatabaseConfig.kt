package dev.goquick.sqlitenow.gradle

import javax.inject.Inject
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

open class DatabaseConfig @Inject constructor(
    /** The container will pass the database name here. */
    val name: String,
    objects: ObjectFactory
) {
    /** The package into which code will be generated */
    val packageName: Property<String> = objects.property(String::class.java)
        .convention("")
    val schemaDatabaseFile: RegularFileProperty = objects.fileProperty()
    val debug: Property<Boolean> = objects.property(Boolean::class.java)
        .convention(false)
}
