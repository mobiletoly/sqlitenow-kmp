@file:OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)

import groovy.json.JsonSlurper
import java.security.MessageDigest
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.testing.Test
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.androidKotlinMultiplatformLibrary.get().pluginId)
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id(libs.plugins.mavenPublish.get().pluginId)
}

group = providers.gradleProperty("sqlitenow.group").get()
version = providers.gradleProperty("sqlitenow.version").get()

val isPublishingToMavenLocal =
    gradle.startParameter.taskNames.any { it.contains("publishToMavenLocal", ignoreCase = true) }
val hasSigningCredentials =
    providers.environmentVariable("SIGNING_KEY").isPresent ||
        providers.environmentVariable("SIGNING_KEY_ID").isPresent ||
        providers.environmentVariable("SIGNING_PASSWORD").isPresent ||
        providers.gradleProperty("signingInMemoryKey").isPresent ||
        providers.gradleProperty("signing.keyId").isPresent ||
        providers.gradleProperty("signing.password").isPresent ||
        providers.gradleProperty("signing.secretKeyRingFile").isPresent ||
        providers.gradleProperty("signing.gnupg.keyName").isPresent

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()
    jvm()

    android {
        namespace = "dev.goquick.sqlitenow.core"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        withDeviceTestBuilder {
            sourceSetTreeName = "test"
        }

        @OptIn(ExperimentalKotlinGradlePluginApi::class)
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
        }
    }

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
        freeCompilerArgs.addAll("-Xexpect-actual-classes")
    }

    js {
        browser()
        nodejs {
            testTask {
                useMocha {
                    timeout = "180s"
                }
            }
        }
        binaries.library()
    }

    wasmJs {
        browser()
        binaries.library()
    }

    iosArm64()
    iosSimulatorArm64()
    macosArm64()
    linuxArm64()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            api(libs.sqlite.core)
            api(libs.sqlite.async)
            api(libs.kotlinx.coroutines.core)
            api(libs.kotlinx.datetime)
            api(libs.kotlinx.serialization.json)
            api(libs.kermit)
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }

        val bundledDriverTest by creating {
            dependsOn(commonTest.get())
            dependencies {
                implementation(libs.sqlite.bundled)
            }
        }

        val bundledDriverMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.sqlite.bundled)
            }
        }

        val nativeMain by getting {
            dependsOn(bundledDriverMain)
        }

        val nativeTest by getting {
            dependsOn(bundledDriverTest)
        }

        val jvmAndroidMain by creating {
            dependsOn(bundledDriverMain)
        }

        val jvmMain by getting {
            dependsOn(jvmAndroidMain)
        }

        val jvmTest by getting {
            dependsOn(bundledDriverTest)
        }

        val androidMain by getting {
            dependsOn(jvmAndroidMain)
        }

        val androidDeviceTest by getting {
            dependsOn(bundledDriverTest)
        }

        getByName("androidDeviceTest").dependencies {
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.rules)
            implementation(libs.androidx.junit)
            implementation(libs.kotlinx.coroutines.test)
        }

        val webTest by getting {
            kotlin.srcDir("src/legacySqlJsWebTest/kotlin")
            dependencies {
                implementation(npm("@sqlite.org/sqlite-wasm", "3.53.0-build1"))
                implementation(devNpm("copy-webpack-plugin", "11.0.0"))
                implementation(npm("sql.js", "1.13.0"))
            }
        }

        val jsTest by getting {
            kotlin.srcDir("src/legacySqlJsJsTest/kotlin")
        }

        val wasmJsTest by getting {
            kotlin.srcDir("src/legacySqlJsWasmJsTest/kotlin")
            resources.srcDir("src/legacySqlJsWasmJsTest/resources")
        }

        jsMain.dependencies {
            implementation(libs.kotlinx.browser)
        }

        val wasmJsMain by getting {
            languageSettings {
                optIn("kotlin.js.ExperimentalWasmJsInterop")
            }
            dependencies {
                implementation(libs.kotlinx.coroutines.core.wasm.js)
            }
        }
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

val sqlJsFixtureModuleDir = rootProject.layout.buildDirectory.dir("js/node_modules/sql.js/dist")
val wasmSqlJsFixtureResourceDir = layout.buildDirectory.dir("generated/test-fixtures/sqljs/wasm")
val sqliteWasmModuleDir =
    rootProject.layout.buildDirectory.dir("js/node_modules/@sqlite.org/sqlite-wasm/dist")
val sqliteWorkerResourceDir = layout.buildDirectory.dir("generated/sqlite-worker/resources")
val sqliteWorkerResourceNamespace = "sqlitenow-worker-v1"
val sqliteWorkerRuntimeNamespace = "$sqliteWorkerResourceNamespace/sqlite-3.53.0-build1"
val sqliteWorkerAuthoredResourceDir = layout.projectDirectory.dir("src/webMain/resources")

val cleanSqliteWorkerGeneratedProductionOutputs by tasks.registering(Delete::class) {
    delete(
        layout.buildDirectory.dir("dist"),
        rootProject.layout.buildDirectory.dir("js/packages/sqlitenow-kmp-library-core"),
        rootProject.layout.buildDirectory.dir("wasm/packages/sqlitenow-kmp-library-core"),
    )
}

listOf(
    "jsBrowserProductionLibraryDistribution",
    "wasmJsBrowserProductionLibraryDistribution",
).forEach { taskName ->
    tasks.named(taskName) {
        dependsOn(cleanSqliteWorkerGeneratedProductionOutputs)
    }
}

val copySqlJsFixtureWasmForWasmTests by tasks.registering(Copy::class) {
    dependsOn(rootProject.tasks.named("kotlinNpmInstall"))
    from(sqlJsFixtureModuleDir.map { it.file("sql-wasm.wasm") })
    into(wasmSqlJsFixtureResourceDir)
}

val copySqliteWorkerPublicationAssets by tasks.registering(Copy::class) {
    dependsOn(rootProject.tasks.named("kotlinNpmInstall"))
    doFirst {
        project.delete(sqliteWorkerResourceDir)
    }
    into(sqliteWorkerResourceDir)
    from(sqliteWasmModuleDir) {
        include(
            "index.mjs",
            "node.mjs",
            "sqlite3.wasm",
            "sqlite3-opfs-async-proxy.js",
        )
        into("$sqliteWorkerRuntimeNamespace/vendor")
    }
    from(rootProject.file("LICENSE")) {
        into("$sqliteWorkerResourceNamespace/licenses")
        rename { "sqlite-wasm-Apache-2.0.txt" }
    }
}

listOf(
    "jsProcessResources",
    "wasmJsProcessResources",
).forEach { taskName ->
    tasks.named<ProcessResources>(taskName) {
        dependsOn(copySqliteWorkerPublicationAssets)
        doFirst {
            project.delete(
                destinationDir.resolve("sqlitenow-sqlite-worker-client.mjs"),
                destinationDir.resolve("sqlitenow-sqlite-worker.mjs"),
                destinationDir.resolve("sqlitenow-sqlite-worker"),
                destinationDir.resolve("sqlitenow-sqljs.js"),
                destinationDir.resolve("sqlitenow-indexeddb.js"),
                destinationDir.resolve("sql-wasm.wasm"),
            )
        }
        from(sqliteWorkerAuthoredResourceDir) {
            include("$sqliteWorkerResourceNamespace/**")
        }
        from(sqliteWorkerResourceDir)
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
}

tasks.named<ProcessResources>("wasmJsTestProcessResources") {
    dependsOn(copySqlJsFixtureWasmForWasmTests)
    from(wasmSqlJsFixtureResourceDir)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val verifyNoDefaultSqlJsWebpackResolution by tasks.registering {
    group = "verification"
    description = "Requires default Core webpack configuration to be unable to resolve SQL.js."

    doLast {
        val permittedFixtureConfig =
            project.file("webpack.config.d/legacy-sqljs-fixture.js")
        val fixtureSource = permittedFixtureConfig.readText()
        check("sqlitenow-kmp-library-core-test" in fixtureSource) {
            "The SQL.js fixture webpack fragment must be explicitly test-package scoped."
        }
        check("resolve.alias" !in fixtureSource) {
            "The SQL.js fixture webpack fragment must not add a default module alias."
        }
        val forbiddenConfigs = project.file("webpack.config.d")
            .walkTopDown()
            .filter { it.isFile }
            .filter { it != permittedFixtureConfig }
            .filter { config ->
                val source = config.readText()
                "sql.js" in source || "sql-wasm.wasm" in source
            }
            .map { it.relativeTo(projectDir).invariantSeparatorsPath }
            .toList()
        check(forbiddenConfigs.isEmpty()) {
            "Default Core webpack configuration still resolves SQL.js: " +
                forbiddenConfigs.sorted()
        }
    }
}

val verifySqliteWorkerPublicationAssets by tasks.registering {
    dependsOn(
        verifyNoDefaultSqlJsWebpackResolution,
        "jsBrowserProductionLibraryDistribution",
        "wasmJsBrowserProductionLibraryDistribution",
        "jsPublicPackageJson",
        "jsTestPublicPackageJson",
        "wasmJsPublicPackageJson",
        "wasmJsTestPublicPackageJson",
        "jsJar",
        "wasmJsJar",
        "jsProcessResources",
        "wasmJsProcessResources",
    )

    doLast {
        fun packageJson(taskName: String): String {
            val files = tasks.named(taskName).get().outputs.files.asFileTree
                .matching { include("**/package.json") }
                .files
            check(files.size == 1) {
                "$taskName must produce exactly one package.json, found ${files.size}."
            }
            return files.single().readText()
        }

        listOf("jsPublicPackageJson", "wasmJsPublicPackageJson").forEach { taskName ->
            check("@sqlite.org/sqlite-wasm" !in packageJson(taskName)) {
                "$taskName must not require npm SQLite WASM at consumer runtime."
            }
            check("\"sql.js\"" !in packageJson(taskName)) {
                "$taskName must not expose SQL.js in public package metadata."
            }
        }
        listOf("jsTestPublicPackageJson", "wasmJsTestPublicPackageJson").forEach { taskName ->
            check("@sqlite.org/sqlite-wasm" in packageJson(taskName)) {
                "$taskName must retain the internal worker test dependency."
            }
            check("\"sql.js\"" in packageJson(taskName)) {
                "$taskName must retain isolated SQL.js fixture generation."
            }
        }

        fun sha256(file: java.io.File): String {
            val digest = MessageDigest.getInstance("SHA-256")
            file.inputStream().buffered().use { input ->
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                }
            }
            return digest.digest().joinToString("") { "%02x".format(it) }
        }

        fun verifyManifest(root: java.io.File, owner: String) {
            val namespaceRoot = root.resolve(sqliteWorkerResourceNamespace)
            val manifestFile = namespaceRoot.resolve("asset-manifest.json")
            check(manifestFile.isFile) {
                "$owner is missing $sqliteWorkerResourceNamespace/asset-manifest.json."
            }
            @Suppress("UNCHECKED_CAST")
            val manifest = JsonSlurper().parse(manifestFile) as Map<String, Any?>
            check(manifest["schema"] == "sqlitenow-worker-asset-manifest-v1")
            check(manifest["protocol"] == "sqlitenow-sqlite-worker-v1")
            check(manifest["resourceNamespace"] == sqliteWorkerResourceNamespace)
            check(manifest["cacheVersion"] == "sqlite-3.53.0-build1")
            val defaultWorkerModule = manifest["defaultWorkerModule"] as String
            check(defaultWorkerModule == "sqlite-3.53.0-build1/worker.mjs")

            @Suppress("UNCHECKED_CAST")
            val assets = manifest["assets"] as List<Map<String, Any?>>
            val expectedPaths = assets.map { asset ->
                val path = asset.getValue("path") as String
                check(!path.startsWith("/") && ".." !in path.split('/')) {
                    "$owner manifest contains a non-relative asset path: $path"
                }
                val file = namespaceRoot.resolve(path)
                check(file.isFile) { "$owner is missing manifest asset $path." }
                check(file.length() == (asset.getValue("size") as Number).toLong()) {
                    "$owner asset size differs from the authored manifest: $path"
                }
                check(sha256(file) == asset.getValue("sha256")) {
                    "$owner asset checksum differs from the authored manifest: $path"
                }
                path
            }
            check(expectedPaths.distinct().size == expectedPaths.size) {
                "$owner manifest contains duplicate asset paths."
            }
            val actualPaths = namespaceRoot.walkTopDown()
                .filter { it.isFile }
                .map { it.relativeTo(namespaceRoot).invariantSeparatorsPath }
                .filter { it != "asset-manifest.json" }
                .toSet()
            check(actualPaths == expectedPaths.toSet()) {
                "$owner asset inventory differs from the authored manifest. " +
                    "Expected ${expectedPaths.toSet()}, found $actualPaths."
            }

            val client = namespaceRoot.resolve("client.mjs").readText()
            check("const SOURCE_CLIENT_MODULE_URL = import.meta.url;" in client)
            check("new urlConstructor(DEFAULT_WORKER_MODULE_URL, clientModuleUrl)" in client)
            check("""/* webpackIgnore: true */ "./sqlitenow-worker-v1/client.mjs"""" in client)
            check("""new globalThis.Worker(workerModuleUrl, { type: "module" })""" in client)
            check("sql.js" !in client && "sql-wasm.wasm" !in client)
            check("new URL(\"/sqlitenow" !in client)
            val forbiddenWorkerScheme = Regex("""["'](?:blob|data):""")
            check(!forbiddenWorkerScheme.containsMatchIn(client))

            val worker = namespaceRoot.resolve(defaultWorkerModule).readText()
            check(""""./vendor/node.mjs"""" in worker)
            check(""""./vendor/index.mjs"""" in worker)
            check("oo1.OpfsDb" in worker)
            check("sqlitenow-worker-v1-" in worker)
            check("sql.js" !in worker && "sql-wasm.wasm" !in worker)
            check(!forbiddenWorkerScheme.containsMatchIn(worker))
            listOf(
                "Web Crypto",
                "Origin Private File System",
                "Web Locks",
                "SQLite OPFS VFS",
                "no snapshot or in-memory browser fallback was started",
            ).forEach { requiredFailureContract ->
                check(requiredFailureContract in worker) {
                    "$owner worker is missing capability failure contract: " +
                        requiredFailureContract
                }
            }
            check(worker.indexOf("requireDirectOpfs();") < worker.indexOf("databaseIdentity(request.fileName)")) {
                "$owner worker must reject missing capabilities before deriving or opening storage."
            }
        }

        val legacyWorkerResources = listOf(
            "sqlitenow-sqlite-worker-client.mjs",
            "sqlitenow-sqlite-worker.mjs",
            "sqlitenow-sqlite-worker",
        )
        val forbiddenSqlJsResources = listOf(
            "sqlitenow-sqljs.js",
            "sqlitenow-indexeddb.js",
            "sql-wasm.wasm",
        )
        val forbiddenProductionResolutionTokens = listOf(
            "'sql.js",
            "\"sql.js",
            "require('sql.js",
            "require(\"sql.js",
            "from 'sql.js",
            "from \"sql.js",
            "SqlJsSQLiteConnection",
            "sqlitenow-sqljs",
            "sqlitenow-indexeddb",
            "sql-wasm.wasm",
        )
        val forbiddenKlibContentTokens = listOf(
            "sql.js",
            "SqlJsSQLiteConnection",
            "SqlJsDatabase",
            "loadSqlJsModule",
            "sqlitenow-sqljs",
            "sqlitenow-indexeddb",
        )

        fun verifyGeneratedProductionTree(root: java.io.File, owner: String) {
            check(root.isDirectory) { "$owner production output is missing: $root" }
            val files = root.walkTopDown()
                .filter { it.isFile }
                .toList()
            val forbiddenFiles = files
                .filter { it.name in forbiddenSqlJsResources }
                .map { it.relativeTo(root).invariantSeparatorsPath }
            check(forbiddenFiles.isEmpty()) {
                "$owner contains forbidden SQL.js compatibility assets: ${forbiddenFiles.sorted()}"
            }
            val forbiddenMatches = files
                .filter { it.extension in setOf("json", "js", "mjs", "cjs") }
                .mapNotNull { file ->
                    val text = file.readText()
                    forbiddenProductionResolutionTokens
                        .firstOrNull { token -> token in text }
                        ?.let { token ->
                            "${file.relativeTo(root).invariantSeparatorsPath}: $token"
                        }
                }
            check(forbiddenMatches.isEmpty()) {
                "$owner still resolves SQL.js or legacy assets: ${forbiddenMatches.sorted()}"
            }
        }

        fun verifyKlibSqlJsAbsence(archive: java.io.File, owner: String) {
            val archiveTree = zipTree(archive)
            val forbiddenEntries = archiveTree.matching {
                forbiddenSqlJsResources.forEach(::include)
            }.files
            check(forbiddenEntries.isEmpty()) {
                "$owner contains forbidden SQL.js compatibility assets: " +
                    forbiddenEntries.map { it.name }.sorted()
            }

            val forbiddenContent = archiveTree.files.mapNotNull { entry ->
                val content = String(entry.readBytes(), Charsets.ISO_8859_1)
                forbiddenKlibContentTokens
                    .firstOrNull { token -> token in content }
                    ?.let { token -> "${entry.name}: $token" }
            }
            check(forbiddenContent.isEmpty()) {
                "$owner contains forbidden SQL.js runtime or dependency content: " +
                    forbiddenContent.sorted()
            }
        }

        listOf("js", "wasmJs").forEach { target ->
            val mainRoot = layout.buildDirectory.dir("processedResources/$target/main").get().asFile
            verifyManifest(mainRoot, "$target main resources")
            check(legacyWorkerResources.none { mainRoot.resolve(it).exists() }) {
                "$target main resources contain obsolete unversioned worker assets."
            }
            check(forbiddenSqlJsResources.none { mainRoot.resolve(it).exists() }) {
                "$target main resources contain forbidden SQL.js compatibility assets."
            }
        }

        listOf("jsJar", "wasmJsJar").forEach { taskName ->
            val archives = tasks.named(taskName).get().outputs.files.files
                .filter { it.extension == "klib" }
            check(archives.size == 1) {
                "$taskName must produce exactly one klib, found ${archives.size}."
            }
            val verifiedRoot = layout.buildDirectory.dir("verifiedWorkerKlibs/$taskName").get().asFile
            project.delete(verifiedRoot)
            project.copy {
                from(zipTree(archives.single()))
                include("$sqliteWorkerResourceNamespace/**")
                into(verifiedRoot)
            }
            verifyManifest(
                verifiedRoot,
                "$taskName klib",
            )
            verifyKlibSqlJsAbsence(archives.single(), "$taskName klib")
        }

        val retainedKlibs = layout.buildDirectory.dir("libs").get().asFileTree
            .matching { include("*.klib") }
            .files
        check(retainedKlibs.isNotEmpty()) {
            "Core publication build/libs must contain retained klibs for residue verification."
        }
        retainedKlibs.forEach { archive ->
            verifyKlibSqlJsAbsence(archive, archive.name)
        }

        verifyGeneratedProductionTree(
            layout.buildDirectory.dir("dist").get().asFile,
            "Core generated distributions",
        )
        listOf("js", "wasm").forEach { target ->
            verifyGeneratedProductionTree(
                rootProject.layout.buildDirectory
                    .dir("$target/packages/sqlitenow-kmp-library-core")
                    .get()
                    .asFile,
                "Core generated $target package",
            )
        }

        val forbiddenProductionImports = listOf(
            "sql.js",
            "sqlitenow-sqljs",
            "sqlitenow-indexeddb",
            "sql-wasm.wasm",
        )
        val productionSourceRoots = listOf(
            layout.projectDirectory.dir("src/jsMain").asFile,
            layout.projectDirectory.dir("src/wasmJsMain").asFile,
            layout.projectDirectory.dir("src/webMain").asFile,
        )
        val forbiddenProductionMatches = productionSourceRoots
            .flatMap { root ->
                if (!root.exists()) {
                    emptyList()
                } else {
                    root.walkTopDown()
                        .filter { it.isFile }
                        .filter { file ->
                            forbiddenProductionImports.any { token ->
                                token in file.readText()
                            }
                        }
                        .map { it.relativeTo(projectDir).invariantSeparatorsPath }
                        .toList()
                }
            }
        check(forbiddenProductionMatches.isEmpty()) {
            "Production web sources still resolve SQL.js or legacy assets: " +
                forbiddenProductionMatches.sorted()
        }
    }
}

tasks.named("check") {
    dependsOn(verifySqliteWorkerPublicationAssets)
}

mavenPublishing {
    publishToMavenCentral()

    if (!isPublishingToMavenLocal && hasSigningCredentials) {
        signAllPublications()
    }

    coordinates(group.toString(), "core", version.toString())

    pom {
        name = "SQLiteNow Core Multiplatform Library"
        description = "SQLiteNow core multiplatform runtime"
        inceptionYear = "2025"
        url = "https://github.com/mobiletoly/sqlitenow-kmp/"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "mobiletoly"
                name = "Toly Pochkin"
                url = "https://github.com/mobiletoly"
            }
        }
        scm {
            url = "https://github.com/mobiletoly/sqlitenow-kmp"
            connection = "scm:git:git://github.com/mobiletoly/sqlitenow-kmp.git"
            developerConnection = "scm:git:git://github.com/mobiletoly/sqlitenow-kmp.git"
        }
    }
}
