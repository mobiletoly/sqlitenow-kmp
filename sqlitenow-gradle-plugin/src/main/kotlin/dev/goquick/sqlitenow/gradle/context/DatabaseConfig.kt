/*
 * Copyright 2025 Toly Pochkin
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

import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS
import org.gradle.api.Action
import org.gradle.api.file.Directory
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFile
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import java.io.File
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

    /** Enables generation of oversqlite bridge helpers for this database. */
    val oversqlite: Property<Boolean> = objects.property(Boolean::class.java)
        .convention(false)

    /** Configures plugin-owned local Swift package generation for this database. */
    val swiftPackage: SwiftPackageConfig = SwiftPackageConfig(name, objects)

    fun swiftPackage(action: Action<in SwiftPackageConfig>) {
        swiftPackage.enabled.set(true)
        action.execute(swiftPackage)
    }
}

open class SwiftPackageConfig(
    databaseName: String,
    objects: ObjectFactory,
) {
    /** Enables local Swift package tasks for this database. */
    val enabled: Property<Boolean> = objects.property(Boolean::class.java)
        .convention(false)

    /** SwiftPM package and product name. */
    val packageName: Property<String> = objects.property(String::class.java)
        .convention("${databaseName}SQLiteNow")

    /** SwiftPM source target name containing generated database source. */
    val swiftTargetName: Property<String> = objects.property(String::class.java)
        .convention(packageName)

    /** Generated SwiftPM package output directory. */
    val outputDirectory: DirectoryProperty = objects.directoryProperty()

    /** Compiler-generated Swift product source directory. */
    val generatedSourceDirectory: DirectoryProperty = objects.directoryProperty()

    /** Explicit Swift runtime mode. Defaults to [DatabaseConfig.oversqlite] when unset. */
    val runtime: Property<String> = objects.property(String::class.java)

    /** Runtime artifact copied or referenced by the generated package. */
    val runtimeArtifact: SwiftPackageRuntimeArtifactConfig = SwiftPackageRuntimeArtifactConfig(objects)

    /** Runtime XCFramework copied into the generated package. Prefer [runtimeArtifact]. */
    val runtimeXcframework: DirectoryProperty = objects.directoryProperty()

    /** Optional task path that produces [runtimeXcframework]. */
    val runtimeTaskPath: Property<String> = objects.property(String::class.java)

    /** Runtime binary target/module imported by generated Swift source. */
    val runtimeModuleName: Property<String> = objects.property(String::class.java)

    /** Apple target families the package is expected to support. */
    val requestedAppleTargets: ListProperty<String> = objects.listProperty(String::class.java)
        .convention(DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS)

    /** Minimum iOS platform version emitted in Package.swift. */
    val minimumIos: Property<String> = objects.property(String::class.java)
        .convention(DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS)

    /** Minimum macOS platform version emitted in Package.swift. */
    val minimumMacos: Property<String> = objects.property(String::class.java)
        .convention(DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS)

    /** Runtime framework linkage mode recorded in package metadata. */
    val frameworkMode: Property<String> = objects.property(String::class.java)
        .convention("dynamic")

    /** Regex patterns that must not appear in generated Swift package source. */
    val forbiddenTokenPatterns: ListProperty<String> = objects.listProperty(String::class.java)
        .convention(
            listOf(
                "Kotlin",
                "Ktor",
                "StateFlow",
                "Flow<",
                "Result<",
                "Throwable",
                "KotlinByteArray",
                "Coroutine",
                "\\bKt\\b",
            )
        )
}

open class SwiftPackageRuntimeArtifactConfig(
    objects: ObjectFactory,
) {
    val kind: Property<String> = objects.property(String::class.java)
    val localXcframeworkDirectory: DirectoryProperty = objects.directoryProperty()
    val localZipFile: RegularFileProperty = objects.fileProperty()
    val remoteZipUrl: Property<String> = objects.property(String::class.java)
    val checksum: Property<String> = objects.property(String::class.java)
    val sqliteNowVersion: Property<String> = objects.property(String::class.java)

    fun localXcframework(directory: Provider<Directory>) {
        kind.set("localXcframework")
        localXcframeworkDirectory.set(directory)
    }

    fun localXcframework(directory: Directory) {
        kind.set("localXcframework")
        localXcframeworkDirectory.set(directory)
    }

    fun localXcframework(directory: File) {
        kind.set("localXcframework")
        localXcframeworkDirectory.set(directory)
    }

    fun localZip(file: Provider<RegularFile>) {
        kind.set("localZip")
        localZipFile.set(file)
    }

    fun localZip(file: RegularFile) {
        kind.set("localZip")
        localZipFile.set(file)
    }

    fun localZip(file: File) {
        kind.set("localZip")
        localZipFile.set(file)
    }

    fun remoteZip(url: String) {
        kind.set("remoteZip")
        remoteZipUrl.set(url)
    }
}
