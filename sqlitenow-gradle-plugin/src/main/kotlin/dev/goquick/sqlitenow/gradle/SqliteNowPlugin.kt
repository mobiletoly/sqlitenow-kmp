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
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.DatabaseConfig
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME
import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageRuntimeArtifactKind
import dev.goquick.sqlitenow.gradle.swift.findExecutableOnPath
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.component.ComponentIdentifier
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.component.ProjectComponentIdentifier
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.ExtensionAware
import org.gradle.api.tasks.Exec
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.dsl.KotlinMultiplatformExtension
import java.util.concurrent.atomic.AtomicBoolean

class SqliteNowPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val ext = project.extensions.create(
            "sqliteNow",
            SqliteNowExtension::class.java,
            project.objects
        )
        (ext as ExtensionAware).extensions.add("databases", ext.databases)

        // ➀ React when the MPP plugin is applied (during configuration)
        project.pluginManager.withPlugin("org.jetbrains.kotlin.multiplatform") {
            val mppExt = project.extensions
                .getByType(KotlinMultiplatformExtension::class.java)

            configureWasmResourceBundling(project)

            // For each db entry in commonMain, register task and wire srcDir
            ext.databases.all { db ->
                val genTask = project.tasks.register(
                    "generate${db.name}",
                    GenerateDatabaseFilesTask::class.java
                ) { task ->
                    task.dbName.set(db.name)
                    task.outputDir.set(project.layout.buildDirectory.dir("generated/sqlitenow/code/${db.name}"))
                    task.packageName.set(db.packageName)
                    task.schemaDatabaseFile.set(db.schemaDatabaseFile)
                    task.debug.set(db.debug)
                    task.oversqlite.set(db.oversqlite)
                    task.oversqliteRuntimePresent.set(
                        project.provider { hasOversqliteRuntimeDependency(project) }
                    )
                }

                // Add generated dir to commonMain immediately
                mppExt.sourceSets
                    .getByName("commonMain")
                    .kotlin
                    .srcDir(genTask.map { it.outputDir })
            }

            project.afterEvaluate {
                val swiftPackageDatabases = ext.databases.filter { it.swiftPackage.enabled.get() }
                swiftPackageDatabases.forEach { db ->
                    registerSwiftPackageTasks(
                        project = project,
                        db = db,
                        genericAliases = swiftPackageDatabases.size == 1,
                    )
                }
            }
        }
    }
}

private fun registerSwiftPackageTasks(
    project: Project,
    db: DatabaseConfig,
    genericAliases: Boolean,
) {
    val runtimeMode = db.swiftPackage.runtime.orElse(db.oversqlite.map { if (it) "sync" else "core" })
    val runtimeModuleName = db.swiftPackage.runtimeModuleName.orElse(
        runtimeMode.map { mode ->
            if (mode == "sync") DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME else DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME
        }
    )
    val packageName = db.swiftPackage.packageName
    val swiftTargetName = db.swiftPackage.swiftTargetName
    val generatedSourceDir = db.swiftPackage.generatedSourceDirectory.orElse(
        project.layout.buildDirectory.dir(swiftTargetName.map { "generated/sqlitenow/swift-product/$it" })
    )
    val generatedMetadataFile = project.layout.buildDirectory.file(
        swiftTargetName.map { "generated/sqlitenow/swift-product-metadata/$it/metadata.json" }
    )
    val outputDir = db.swiftPackage.outputDirectory.orElse(
        project.layout.buildDirectory.dir(packageName.map { "swift-package/$it" })
    )
    val sqliteNowVersion = project.providers.gradleProperty("sqlitenow.version")
        .orElse(project.provider { project.rootProject.version.toString() })
    val sqlFileTree = project.fileTree(project.layout.projectDirectory.dir("src/commonMain/sql/${db.name}")) { tree ->
        tree.include("**/*.sql")
    }

    val swiftProductSourceTaskName = "generate${db.name}DebugSwiftProductSource"
    project.tasks.register(swiftProductSourceTaskName, GenerateDatabaseFilesTask::class.java) { task ->
        task.group = "build"
        task.description = "Generates ${db.name} Swift product source for the local Swift package."
        task.dbName.set(db.name)
        task.outputDir.set(project.layout.buildDirectory.dir("generated/sqlitenow/swift-product-kotlin-stub/${db.name}"))
        task.packageName.set(db.packageName)
        task.debug.set(db.debug)
        task.oversqlite.set(db.oversqlite)
        task.oversqliteRuntimePresent.set(
            project.provider { hasOversqliteRuntimeDependency(project) }
        )
        task.swiftProductOutputDir.set(generatedSourceDir)
        task.swiftProductModuleName.set(swiftTargetName)
        task.swiftProductRuntimeModuleName.set(runtimeModuleName)
        task.swiftProductRuntimeMode.set(runtimeMode)
        task.swiftProductMetadataFile.set(generatedMetadataFile)
    }

    val generateSwiftTaskName = "generate${db.name}DebugSwiftProduct"
    project.tasks.register(generateSwiftTaskName, VerifySwiftProductSourceTask::class.java) { task ->
        task.group = "build"
        task.description = "Verifies the compiler-generated ${db.name} Swift product source."
        task.dependsOn(swiftProductSourceTaskName)
        task.databaseName.set(db.name)
        task.generatedSwiftSourceDirectory.set(generatedSourceDir)
    }

    val packageTaskName = "package${db.name}DebugSwiftPackage"
    project.tasks.register(packageTaskName, PackageSwiftPackageTask::class.java) { task ->
        task.group = "build"
        task.description = "Builds the local ${db.name} SwiftPM package over the reusable runtime framework."
        task.dependsOn(generateSwiftTaskName)
        db.swiftPackage.runtimeTaskPath.orNull
            ?.takeIf { it.isNotBlank() }
            ?.let { task.dependsOn(it) }
        task.databaseName.set(db.name)
        task.swiftPackageName.set(packageName)
        task.swiftTargetName.set(swiftTargetName)
        task.runtimeMode.set(runtimeMode)
        task.runtimeModuleName.set(runtimeModuleName)
        task.frameworkMode.set(db.swiftPackage.frameworkMode)
        task.forbiddenTokenPatterns.set(db.swiftPackage.forbiddenTokenPatterns)
        task.requestedAppleTargets.set(db.swiftPackage.requestedAppleTargets)
        task.minimumIos.set(db.swiftPackage.minimumIos)
        task.minimumMacos.set(db.swiftPackage.minimumMacos)
        task.sqliteNowVersion.set(sqliteNowVersion)
        task.generatedBy.set(
            project.provider {
                val publicTaskName = if (genericAliases) "packageDebugSwiftPackage" else packageTaskName
                "./gradlew ${taskPath(project, publicTaskName)}"
            }
        )
        task.generatedSwiftSourceDirectory.set(generatedSourceDir)
        task.swiftProductMetadataFile.set(generatedMetadataFile)
        configureRuntimeArtifactInputs(db, task)
        task.sqlFiles.from(sqlFileTree)
        task.swiftPackageOutputDirectory.set(outputDir)
    }

    val validateTaskName = "validate${db.name}DebugSwiftPackageManifest"
    project.tasks.register(validateTaskName, ValidateSwiftPackageManifestTask::class.java) { task ->
        task.group = "verification"
        task.description = "Validates ${db.name} local Swift package manifest and cache metadata."
        task.dependsOn(packageTaskName)
        task.databaseName.set(db.name)
        task.swiftPackageName.set(packageName)
        task.swiftTargetName.set(swiftTargetName)
        task.runtimeMode.set(runtimeMode)
        task.runtimeModuleName.set(runtimeModuleName)
        task.frameworkMode.set(db.swiftPackage.frameworkMode)
        task.forbiddenTokenPatterns.set(db.swiftPackage.forbiddenTokenPatterns)
        task.requestedAppleTargets.set(db.swiftPackage.requestedAppleTargets)
        task.minimumIos.set(db.swiftPackage.minimumIos)
        task.minimumMacos.set(db.swiftPackage.minimumMacos)
        task.sqliteNowVersion.set(sqliteNowVersion)
        task.manifestFile.set(outputDir.map { it.file(".sqlitenow/package-manifest.json") })
        task.generatedSwiftSourceDirectory.set(generatedSourceDir)
        task.swiftProductMetadataFile.set(generatedMetadataFile)
        configureRuntimeArtifactInputs(db, task)
        task.sqlFiles.from(sqlFileTree)
    }

    val leakTaskName = "check${db.name}DebugSwiftPackageLeaks"
    project.tasks.register(leakTaskName, CheckSwiftPackageLeaksTask::class.java) { task ->
        task.group = "verification"
        task.description = "Checks that ${db.name} generated Swift package source does not expose Kotlin bridge tokens."
        task.dependsOn(packageTaskName)
        task.sourceDirectory.set(outputDir.map { it.dir("Sources") })
        task.forbiddenTokenPatterns.set(db.swiftPackage.forbiddenTokenPatterns)
    }

    val buildTaskName = "build${db.name}DebugSwiftPackage"
    project.tasks.register(buildTaskName, Exec::class.java) { task ->
        task.group = "verification"
        task.description = "Builds the generated ${db.name} local Swift package with SwiftPM."
        task.dependsOn(packageTaskName)
        task.workingDir = project.rootDir
        task.inputs.file(outputDir.map { it.file("Package.swift") })
        task.inputs.dir(outputDir.map { it.dir("Sources") })
        task.inputs.dir(outputDir.map { it.dir("Binaries") })
        task.outputs.upToDateWhen { false }
        task.doFirst {
            task.executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
            task.args("build", "--package-path", outputDir.get().asFile.absolutePath)
        }
    }

    if (genericAliases) {
        registerSwiftPackageAlias(project, "generateDebugSwiftProduct", generateSwiftTaskName, "build")
        registerSwiftPackageAlias(project, "packageDebugSwiftPackage", packageTaskName, "build")
        registerSwiftPackageAlias(project, "validateDebugSwiftPackageManifest", validateTaskName, "verification")
        registerSwiftPackageAlias(project, "checkDebugSwiftPackageLeaks", leakTaskName, "verification")
        registerSwiftPackageAlias(project, "buildDebugSwiftPackage", buildTaskName, "verification")
    }
}

private fun configureRuntimeArtifactInputs(
    db: DatabaseConfig,
    task: PackageSwiftPackageTask,
) {
    val kind = configuredRuntimeArtifactKind(db)
    task.runtimeArtifactKind.set(kind)
    configureRuntimeArtifactFileInputs(
        db = db,
        kind = kind,
        setRuntimeXcframeworkDirectory = { task.runtimeXcframeworkDirectory.set(it) },
        setRuntimeZipFile = { task.runtimeZipFile.set(it) },
    )
    task.runtimeArtifactUrl.set(db.swiftPackage.runtimeArtifact.remoteZipUrl)
    task.runtimeArtifactChecksum.set(db.swiftPackage.runtimeArtifact.checksum)
    task.runtimeArtifactVersion.set(db.swiftPackage.runtimeArtifact.sqliteNowVersion)
}

private fun configureRuntimeArtifactInputs(
    db: DatabaseConfig,
    task: ValidateSwiftPackageManifestTask,
) {
    val kind = configuredRuntimeArtifactKind(db)
    task.runtimeArtifactKind.set(kind)
    configureRuntimeArtifactFileInputs(
        db = db,
        kind = kind,
        setRuntimeXcframeworkDirectory = { task.runtimeXcframeworkDirectory.set(it) },
        setRuntimeZipFile = { task.runtimeZipFile.set(it) },
    )
    task.runtimeArtifactUrl.set(db.swiftPackage.runtimeArtifact.remoteZipUrl)
    task.runtimeArtifactChecksum.set(db.swiftPackage.runtimeArtifact.checksum)
    task.runtimeArtifactVersion.set(db.swiftPackage.runtimeArtifact.sqliteNowVersion)
}

private fun configureRuntimeArtifactFileInputs(
    db: DatabaseConfig,
    kind: String,
    setRuntimeXcframeworkDirectory: (org.gradle.api.file.DirectoryProperty) -> Unit,
    setRuntimeZipFile: (org.gradle.api.file.RegularFileProperty) -> Unit,
) {
    when (kind) {
        SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK.id -> {
            val hasNewXcframework = db.swiftPackage.runtimeArtifact.localXcframeworkDirectory.isPresent
            val hasLegacyXcframework = db.swiftPackage.runtimeXcframework.isPresent
            require(!(hasNewXcframework && hasLegacyXcframework)) {
                "Configure either swiftPackage.runtimeArtifact.localXcframework(...) or " +
                    "swiftPackage.runtimeXcframework, not both."
            }
            if (hasNewXcframework) {
                setRuntimeXcframeworkDirectory(db.swiftPackage.runtimeArtifact.localXcframeworkDirectory)
            } else {
                setRuntimeXcframeworkDirectory(db.swiftPackage.runtimeXcframework)
            }
        }

        SwiftPackageRuntimeArtifactKind.LOCAL_ZIP.id -> {
            setRuntimeZipFile(db.swiftPackage.runtimeArtifact.localZipFile)
        }

        SwiftPackageRuntimeArtifactKind.REMOTE_ZIP.id -> Unit
    }
}

private fun configuredRuntimeArtifactKind(db: DatabaseConfig): String {
    val configuredKind = db.swiftPackage.runtimeArtifact.kind.orNull?.trim()?.takeIf { it.isNotEmpty() }
    if (configuredKind != null) {
        require(!db.swiftPackage.runtimeXcframework.isPresent) {
            "Configure either swiftPackage.runtimeArtifact or swiftPackage.runtimeXcframework, not both."
        }
        SwiftPackageRuntimeArtifactKind.fromId(configuredKind)
        return configuredKind
    }
    return SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK.id
}

private fun registerSwiftPackageAlias(
    project: Project,
    aliasName: String,
    targetTaskName: String,
    groupName: String,
) {
    project.tasks.register(aliasName) { task ->
        task.group = groupName
        task.description = "Alias for $targetTaskName."
        task.dependsOn(targetTaskName)
    }
}

private fun taskPath(project: Project, taskName: String): String =
    if (project.path == ":") ":$taskName" else "${project.path}:$taskName"

private fun hasOversqliteRuntimeDependency(project: Project): Boolean {
    return project.configurations.any { configuration ->
        configuration.allDependencies.any { dependency ->
            when (dependency) {
                is ProjectDependency -> dependency.path == ":library-oversqlite"
                else -> dependency.group == "dev.goquick.sqlitenow" && dependency.name == "oversqlite"
            }
        }
    }
}

private fun configureWasmResourceBundling(project: Project) {
    val configured = AtomicBoolean(false)
    val preferredName = "wasmJsMainCompileClasspath"
    val fallbackName = "wasmJsCompileClasspath"
    project.configurations.matching { it.name == preferredName || it.name == fallbackName }.all { wasmClasspath ->
        if (configured.get()) return@all
        if (wasmClasspath.name == fallbackName &&
            project.configurations.findByName(preferredName) != null
        ) {
            return@all
        }
        configured.set(true)
        project.tasks.withType(ProcessResources::class.java).configureEach { task ->
            if (task.name != "wasmJsProcessResources") return@configureEach
            task.dependsOn(wasmClasspath)
            task.from({ resolveSqliteNowWasmKlibs(project, wasmClasspath).map { project.zipTree(it) } }) { spec ->
                spec.include(
                    "sqlitenow-sqljs.js",
                    "sqlitenow-indexeddb.js",
                    "sql-wasm.wasm"
                )
            }
        }
    }
}

private fun resolveSqliteNowWasmKlibs(project: Project, classpath: Configuration): Set<java.io.File> {
    val artifacts = classpath.incoming.artifactView { }.artifacts
    val klibs = artifacts.artifacts.mapNotNull { artifact ->
        val file = artifact.file
        if (file.extension != "klib") {
            return@mapNotNull null
        }
        if (isSqliteNowComponent(project, artifact.id.componentIdentifier)) {
            return@mapNotNull file
        }
        null
    }.toSet()

    if (klibs.isNotEmpty()) {
        return klibs
    }

    return artifacts.artifacts.mapNotNull { artifact ->
        val file = artifact.file
        if (file.extension == "klib" && file.name.contains("sqlitenow", ignoreCase = true)) {
            file
        } else {
            null
        }
    }.toSet()
}

private fun isSqliteNowComponent(project: Project, id: ComponentIdentifier): Boolean {
    return when (id) {
        is ModuleComponentIdentifier -> id.group == "dev.goquick.sqlitenow"
        is ProjectComponentIdentifier -> {
            val targetProject = project.rootProject.findProject(id.projectPath) ?: return false
            targetProject.group.toString() == "dev.goquick.sqlitenow"
        }
        else -> false
    }
}
