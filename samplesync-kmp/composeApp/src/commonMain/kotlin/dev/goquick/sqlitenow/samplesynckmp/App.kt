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
package dev.goquick.sqlitenow.samplesynckmp

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.AlertDialog
import androidx.compose.material.Button
import androidx.compose.material.ButtonDefaults
import androidx.compose.material.Card
import androidx.compose.material.CircularProgressIndicator
import androidx.compose.material.MaterialTheme
import androidx.compose.material.OutlinedTextField
import androidx.compose.material.Text
import androidx.compose.material.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.common.PlatformType
import dev.goquick.sqlitenow.common.platform
import dev.goquick.sqlitenow.common.resolveDatabasePath
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromRfc3339String
import dev.goquick.sqlitenow.core.util.jsonDecodeListFromSqlite
import dev.goquick.sqlitenow.core.util.jsonEncodeToSqlite
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toRfc3339String
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.ConnectBindingConflictException
import dev.goquick.sqlitenow.oversqlite.ConnectLocalStateConflictException
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.Resolver
import dev.goquick.sqlitenow.oversqlite.SyncReport
import dev.goquick.sqlitenow.oversqlite.SyncThenDetachResult
import dev.goquick.sqlitenow.samplesynckmp.db.AddressType
import dev.goquick.sqlitenow.samplesynckmp.db.CommentQuery
import dev.goquick.sqlitenow.samplesynckmp.db.CommentRow
import dev.goquick.sqlitenow.samplesynckmp.db.NowSampleSyncDatabase
import dev.goquick.sqlitenow.samplesynckmp.db.PersonAddressQuery
import dev.goquick.sqlitenow.samplesynckmp.db.PersonQuery
import dev.goquick.sqlitenow.samplesynckmp.db.PersonRow
import dev.goquick.sqlitenow.samplesynckmp.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.samplesynckmp.model.PersonNote
import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.serialization.kotlinx.json.json
import io.ktor.http.HttpHeaders
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.datetime.LocalDate
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.jetbrains.compose.ui.tooling.preview.Preview
import kotlin.random.Random
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

private const val periodicSyncEnabled = false
private const val periodicSyncIntervalMs = 10_000L
private const val sampleAttachMaxAttempts = 3

private val firstNames = listOf(
    // Traditional English names
    "John",
    "Jane",
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank",
    "Grace",
    "Henry",
    "William",
    "Mary",
    "James",
    "Patricia",
    "Robert",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "Richard",
    "Barbara",
    "Joseph",
    "Susan",
    "Thomas",
    "Jessica",
    "Christopher",
    "Sarah",
    "Daniel",
    "Karen",
    "Paul",
    "Nancy",
    "Mark",
    "Lisa",
    "Donald",
    "Betty",
    "George",
    "Helen",
    "Kenneth",
    "Sandra",
    "Steven",
    "Donna",
    "Edward",
    "Carol",
    "Brian",
    "Ruth",
    "Ronald",
    "Sharon",
    "Anthony",
    "Michelle",
    "Kevin",
    "Laura",
    "Jason",
    "Sarah",
    "Matthew",
    "Kimberly",
    "Gary",
    "Deborah",
    "Timothy",
    "Dorothy",
    "Jose",
    "Amy",
)

private val lastNames = listOf(
    // Common American surnames
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
    "Gomez",
    "Phillips",
    "Evans",
    "Turner",
    "Diaz",
    "Parker",
    "Cruz",
    "Edwards",
    "Collins",
    "Reyes",
    "Stewart",
    "Morris",
)

private val domains = listOf("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com")

val db = NowSampleSyncDatabase(
    resolveDatabasePath(dbName = "test05.db", appName = "SampleSync"),
    personAdapters = NowSampleSyncDatabase.PersonAdapters(
        birthDateToSqlValue = {
            it?.toSqliteDate()
        },
        sqlValueToBirthDate = {
            it?.let { LocalDate.fromSqliteDate(it) }
        },
        sqlValueToUpdatedAt = { Instant.fromRfc3339String(it) },
    ),
    commentAdapters = NowSampleSyncDatabase.CommentAdapters(
        createdAtToSqlValue = { ts -> ts.toRfc3339String() },
        tagsToSqlValue = { tags -> tags?.jsonEncodeToSqlite() },
        sqlValueToCreatedAt = { Instant.fromRfc3339String(it) },
        sqlValueToTags = { it?.jsonDecodeListFromSqlite() ?: emptyList() },
    ),
    personAddressAdapters = NowSampleSyncDatabase.PersonAddressAdapters(
        addressTypeToSqlValue = { it.value },
        sqlValueToAddressType = { AddressType.from(it) },
    ),
    migration = VersionBasedDatabaseMigrations()
)

private val updatedAtWinsResolver = Resolver { conflict ->
    val serverUpdatedAt = conflict.serverRow.updatedAtOrNull()
    val localUpdatedAt = conflict.localPayload.updatedAtOrNull()
    if (serverUpdatedAt != null && localUpdatedAt != null && localUpdatedAt > serverUpdatedAt) {
        MergeResult.KeepLocal
    } else {
        MergeResult.AcceptServer
    }
}

private fun JsonElement?.updatedAtOrNull(): Instant? {
    val value = this
        ?.jsonObject
        ?.get("updated_at")
        ?.jsonPrimitive
        ?.contentOrNull
        ?: return null
    return runCatching { Instant.fromRfc3339String(value) }.getOrNull()
}

private data class SampleSyncSession(
    val client: OversqliteClient,
    val httpClient: HttpClient,
)

private suspend fun attachUntilConnected(
    client: OversqliteClient,
    user: String,
): AttachResult.Connected {
    repeat(sampleAttachMaxAttempts) { attempt ->
        when (val attach = client.attach(user).getOrThrow()) {
            is AttachResult.Connected -> return attach
            is AttachResult.RetryLater -> {
                if (attempt == sampleAttachMaxAttempts - 1) {
                    throw IllegalStateException(
                        "Attach kept asking to retry later for user=$user " +
                            "after $sampleAttachMaxAttempts attempts"
                    )
                }
                appLog.i {
                    "Attach asked to retry later for user=$user " +
                        "in ${attach.retryAfterSeconds}s (attempt ${attempt + 1}/$sampleAttachMaxAttempts)"
                }
                delay(attach.retryAfterSeconds.coerceAtLeast(1) * 1_000)
            }
        }
    }
    error("attachUntilConnected exhausted attempts unexpectedly")
}

private fun buildSyncReportMessage(report: SyncReport): String = buildString {
    append("Push: ")
    append(report.pushOutcome)
    append('\n')
    append("Pull: ")
    append(report.remoteOutcome)
    append('\n')
    append("Pending rows: ")
    append(report.status.pending.pendingRowCount)
    report.restore?.let { restore ->
        append('\n')
        append("Restored snapshot: bundle=")
        append(restore.bundleSeq)
        append(", rows=")
        append(restore.rowCount)
    }
}

private fun buildDetachReportMessage(result: SyncThenDetachResult): String = buildString {
    if (result.isSuccess()) {
        append("Signed out successfully after ")
        append(result.syncRounds)
        append(" sync round(s).")
    } else {
        append("Sign out stayed attached after ")
        append(result.syncRounds)
        append(" sync round(s). ")
        append(result.remainingPendingRowCount)
        append(" pending row(s) still remain.")
    }
}

private fun clearSavedAuth() {
    AuthPrefs.remove(AuthKeys.Token)
    AuthPrefs.remove(AuthKeys.Username)
}

private fun loadOrCreateSourceId(): String {
    val existing = AuthPrefs.get(AuthKeys.SourceId)
    if (!existing.isNullOrEmpty()) {
        return existing
    }
    val created = generateSourceId()
    AuthPrefs.set(AuthKeys.SourceId, created)
    return created
}

private suspend fun setupSyncClient(
    baseUrl: String,
    user: String,
    sourceId: String,
    token: String,
    resourceScope: CoroutineScope,
    onSuccess: (SampleSyncSession) -> Unit,
    onError: (Exception) -> Unit
) {
    var httpClient: HttpClient? = null
    var client: OversqliteClient? = null
    try {
        ensureSampleSyncServer(baseUrl)

        // Build long-lived session resources from their own scope so later sync requests do not
        // inherit the completed sign-in coroutine job.
        withContext(resourceScope.coroutineContext) {
            httpClient = createAuthenticatedHttpClient(
                baseUrl = baseUrl,
                token = token,
            )

            client = db.newOversqliteClient(
                schema = "business",
                httpClient = httpClient,
                resolver = updatedAtWinsResolver,
                verboseLogs = true,
            )
        }

        val sessionHttpClient = checkNotNull(httpClient)
        val sessionClient = checkNotNull(client)

        sessionClient.open().getOrThrow()
        val attach = try {
            attachUntilConnected(sessionClient, user)
        } catch (e: ConnectBindingConflictException) {
            throw IllegalStateException(
                "Local sync state is still attached to ${e.attachedUserId}. " +
                    "Sign back into that user or clear the sample database before switching accounts.",
                e,
            )
        } catch (e: ConnectLocalStateConflictException) {
            throw IllegalStateException(
                "Local sync recovery is pending for a different durable scope. " +
                    "Sign back into that user or clear the sample database before switching accounts.",
                e,
            )
        }
        appLog.i { "Attach complete for user=$user outcome=${attach.outcome}" }
        logLocalSyncState("after attach")

        val initialSync = sessionClient.sync().getOrThrow()
        appLog.i {
            "Initial sync complete for user=$user " +
                "push=${initialSync.pushOutcome} pull=${initialSync.remoteOutcome}"
        }
        logLocalSyncState("after initial sync")

        onSuccess(SampleSyncSession(client = sessionClient, httpClient = sessionHttpClient))
    } catch (e: Exception) {
        client?.close()
        httpClient?.close()
        onError(e)
    }
}

@OptIn(FlowPreview::class)
@Composable
@Preview
fun App() {
    val coroutineScope = rememberCoroutineScope()
    val sessionResourceScope = remember { CoroutineScope(SupervisorJob() + Dispatchers.Default) }
    var persons by remember {
        mutableStateOf<List<PersonRow>>(emptyList())
    }
    var isLoading by remember { mutableStateOf(true) }
    var errorMessage by remember { mutableStateOf<String?>(null) }
    var syncSession by remember { mutableStateOf<SampleSyncSession?>(null) }
    var lifecycleReady by remember { mutableStateOf(false) }
    var signedIn by remember { mutableStateOf(false) }
    var skippedSignin by remember { mutableStateOf(false) }
    var showSigninDialog by remember { mutableStateOf(true) }
    var username by remember { mutableStateOf("") }
    var password by remember { mutableStateOf("") }
    var signingIn by remember { mutableStateOf(false) }
    var signingOut by remember { mutableStateOf(false) }
    var sourceId by remember { mutableStateOf("") }
    var reportMessage by remember { mutableStateOf<String?>(null) }
    var isDatabaseOpen by remember { mutableStateOf(false) }
    val syncTrigger = remember { MutableSharedFlow<Unit>(extraBufferCapacity = 1) }
    val commentsRefreshTrigger = remember { MutableSharedFlow<Unit>(extraBufferCapacity = 1) }
    fun requestAutoSync() {
        if (periodicSyncEnabled) {
            syncTrigger.tryEmit(Unit)
        }
    }

    val baseUrl = if (platform() == PlatformType.ANDROID) {
        "http://10.0.2.2:8080"
    } else {
        "http://127.0.0.1:8080"
    }

    DisposableEffect(syncSession) {
        val session = syncSession
        onDispose {
            session?.client?.close()
            session?.httpClient?.close()
        }
    }

    DisposableEffect(Unit) {
        onDispose {
            sessionResourceScope.cancel()
        }
    }

    LaunchedEffect(Unit) {

        // TODO .open() is here just for demo purposes. In your app you should open it in some other place
        db.open()
        db.connection().execSQL("PRAGMA foreign_keys = ON;")
        isDatabaseOpen = true

        // Listen for real-time changes from Person/SelectAll query
        db.person
            .selectAll(
                PersonQuery.SelectAll.Params(
                    limit = -1,
                    offset = 0
                )
            )
            .asFlow()
            .map { personList ->
                personList.map { person ->
                    person.copy(myLastName = person.myLastName.uppercase())
                }
            }
            .collect {
                // This code runs on Dispatchers.Main (UI)
                persons = it
                isLoading = false
            }
    }

    // Restore persisted auth, then resume signed-in session if token exists.
    LaunchedEffect(isDatabaseOpen) {
        if (!isDatabaseOpen) {
            return@LaunchedEffect
        }
        val persistedSourceId = loadOrCreateSourceId()
        sourceId = persistedSourceId

        val savedUser = AuthPrefs.get(AuthKeys.Username)
        if (!savedUser.isNullOrBlank()) {
            try {
                val token = fetchJwt(
                    baseUrl = baseUrl,
                    user = savedUser,
                    sourceId = persistedSourceId,
                    password = "demo",
                )
                AuthPrefs.set(AuthKeys.Token, token)
                setupSyncClient(
                    baseUrl = baseUrl,
                    user = savedUser,
                    sourceId = persistedSourceId,
                    token = token,
                    resourceScope = sessionResourceScope,
                    onSuccess = { session ->
                        syncSession = session
                        lifecycleReady = true
                        signedIn = true
                        skippedSignin = false
                        showSigninDialog = false
                        username = savedUser
                        appLog.i { "Restored session for user=$savedUser source=$persistedSourceId" }
                    },
                    onError = { error ->
                        appLog.e(error) { "Failed to restore session; showing sign-in" }
                        syncSession = null
                        signedIn = false
                        lifecycleReady = false
                        showSigninDialog = true
                    }
                )
            } catch (e: Exception) {
                // Fallback to signed-out state if token invalid
                appLog.e(e) { "Failed to restore session; showing sign-in" }
                signedIn = false
                lifecycleReady = false
                showSigninDialog = true
            }
        }
    }
    // Sync worker: runs on demand when triggered, with debounce/coalescing
    LaunchedEffect(syncSession) {
        val client = syncSession?.client ?: return@LaunchedEffect
        appLog.i { "Sync worker active" }
        syncTrigger
            .debounce(700)
            .collectLatest {
                try {
                    ensureSampleSyncServer(baseUrl)
                    logLocalSyncState("before sync")
                    client.sync()
                        .onFailure {
                            appLog.e(it) { "sync failed" }
                            errorMessage = "Sync failed: ${it.message ?: "unknown"}"
                            return@collectLatest
                        }
                    logLocalSyncState("after sync")
                    appLog.i { "Sync cycle complete" }
                } catch (e: Exception) {
                    appLog.e(e) { "Sync cycle failed" }
                    errorMessage = e.message ?: "Sync cycle failed"
                }
            }
    }

    // Periodic nudge (e.g., every 60s) only when signed in
    LaunchedEffect(signedIn, lifecycleReady) {
        if (signedIn && lifecycleReady) {
            if (!periodicSyncEnabled) {
                appLog.i { "Periodic sync disabled" }
                return@LaunchedEffect
            }
            appLog.i { "Scheduling periodic sync every ${periodicSyncIntervalMs / 1000}s" }
            while (isActive) {
                delay(periodicSyncIntervalMs)
                syncTrigger.tryEmit(Unit)
            }
        }
    }

    MaterialTheme {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            // Header
            HeaderSection(persons.size)

            // Secondary header actions
            SignInSection(
                signedIn = signedIn,
                skippedSignin = skippedSignin,
                username = username,
                signingOut = signingOut,
                onSignInClick = { showSigninDialog = true },
                onSignOutClick = {
                    if (signingOut) {
                        return@SignInSection
                    }
                    signingOut = true
                    coroutineScope.launch {
                        try {
                            val client =
                                syncSession?.client ?: run { errorMessage = "Not signed in"; return@launch }
                            ensureSampleSyncServer(baseUrl)
                            val result = client.syncThenDetach().getOrThrow()
                            reportMessage = buildDetachReportMessage(result)
                            if (result.isSuccess()) {
                                clearSavedAuth()
                                syncSession = null
                                lifecycleReady = false
                                signedIn = false
                                skippedSignin = true
                                showSigninDialog = false
                                username = ""
                                password = ""
                                errorMessage = null
                            } else {
                                errorMessage =
                                    "Sign out blocked: ${result.remainingPendingRowCount} pending row(s) still need sync."
                            }
                        } catch (e: Exception) {
                            appLog.e(e) { "Sign out failed" }
                            errorMessage = "Sign out failed: ${e.message}"
                        } finally {
                            signingOut = false
                        }
                    }
                }
            )

            // Actions row: Add Person + Sync
            ActionButtonsRow(
                signedIn = signedIn,
                lifecycleReady = lifecycleReady,
                onAddPerson = {
                    coroutineScope.launch {
                        addRandomPerson { error ->
                            errorMessage = error;
                            reportMessage = null
                        }
                        requestAutoSync()
                    }
                },
                onSync = {
                    coroutineScope.launch {
                        val client =
                            syncSession?.client ?: run { errorMessage = "Not signed in"; return@launch }
                        try {
                            ensureSampleSyncServer(baseUrl)
                            logLocalSyncState("manual sync before sync")
                            val report = client.sync().getOrThrow()
                            logLocalSyncState("manual sync after sync")
                            reportMessage = buildSyncReportMessage(report)
                            errorMessage = null
                        } catch (e: Exception) {
                            appLog.e(e) { "Manual sync failed" }
                            errorMessage = "Sync failed: ${e.message}"
                            reportMessage = null
                        }
                    }
                }
            )

            // Sync controls removed; app syncs automatically once signed in

            // Loading State
            if (isLoading) {
                Box(
                    modifier = Modifier.fillMaxWidth(),
                    contentAlignment = Alignment.Center
                ) {
                    CircularProgressIndicator()
                }
            }

            // Persons List
            if (persons.isEmpty() && !isLoading) {
                EmptyPersonsPlaceholder()
            } else {
                PersonsList(
                    persons = persons,
                    commentsRefreshTrigger = commentsRefreshTrigger,
                    onRandomize = { p ->
                        coroutineScope.launch {
                            randomizePerson(p) { error ->
                                errorMessage = error;
                                reportMessage = null
                            }
                            logLocalSyncState("after Rnd click", p.id)
                            requestAutoSync()
                        }
                    },
                    onDelete = { p ->
                        coroutineScope.launch {
                            deletePerson(p.id) { error ->
                                errorMessage = error;
                                reportMessage = null
                            }
                            requestAutoSync()
                        }
                    },
                    onAddAddress = { pid ->
                        coroutineScope.launch {
                            addRandomAddress(pid) { error ->
                                errorMessage = error;
                                reportMessage = null
                            }
                            requestAutoSync()
                        }
                    },
                    onAddComment = { pid ->
                        coroutineScope.launch {
                            addRandomComment(pid) { error ->
                                errorMessage = error;
                                reportMessage = null
                            }
                            requestAutoSync()
                            commentsRefreshTrigger.tryEmit(Unit)
                        }
                    }
                )
            }
        }

        // Report Dialog (success)
        reportMessage?.let { msg ->
            appLog.i { "Showing report: $msg" }
            AlertDialog(
                onDismissRequest = { reportMessage = null },
                title = {
                    Text(
                        "Sync Report",
                        fontWeight = FontWeight.Bold,
                        color = MaterialTheme.colors.primary
                    )
                },
                text = { Text(msg, fontSize = 14.sp) },
                confirmButton = {
                    TextButton(onClick = { reportMessage = null }) {
                        Text("OK")
                    }
                }
            )
        }

        // Error / Report Dialog
        ErrorDialog(message = errorMessage, onDismiss = { errorMessage = null })

        // Sign-in dialog (shown on app start). Allows skip or sign in.
        SignInDialog(
            show = showSigninDialog && !signedIn,
            username = username,
            password = password,
            signingIn = signingIn,
            onUsernameChange = { username = it },
            onPasswordChange = { password = it },
            onConfirm = {
                if (!signingIn) {
                    signingIn = true
                    coroutineScope.launch {
                        try {
                            val displayUser = username.ifBlank { "user-sample" }
                            val installSourceId = loadOrCreateSourceId()
                            sourceId = installSourceId
                            appLog.i { "Signing in user='${displayUser}' source=$installSourceId" }
                            val token = fetchJwt(
                                baseUrl,
                                user = displayUser,
                                sourceId = installSourceId,
                                password = password.ifBlank { "demo" })

                            // Save token first so the HttpClient can use it
                            AuthPrefs.set(AuthKeys.Token, token)

                            val finalUser = username.ifBlank { "user-sample" }
                            setupSyncClient(
                                baseUrl = baseUrl,
                                user = finalUser,
                                sourceId = installSourceId,
                                token = token,
                                resourceScope = sessionResourceScope,
                                onSuccess = { session ->
                                    syncSession = session
                                    lifecycleReady = true
                                    signedIn = true
                                    skippedSignin = false
                                    showSigninDialog = false
                                    AuthPrefs.set(AuthKeys.Username, finalUser)
                                    AuthPrefs.set(AuthKeys.SourceId, installSourceId)
                                },
                                onError = { error ->
                                    clearSavedAuth()
                                    appLog.e(error) { "Sign-in failed" }
                                    errorMessage = "Sign-in failed: ${error.message}"
                                }
                            )
                        } catch (e: Exception) {
                            clearSavedAuth()
                            appLog.e(e) { "Sign-in failed" }
                            errorMessage = "Sign-in failed: ${e.message}"
                        } finally {
                            signingIn = false
                        }
                    }
                }
            },
            onSkip = { skippedSignin = true; showSigninDialog = false }
        )
    }
}

@Composable
fun PersonCard(
    person: PersonRow,
    onRandomize: (PersonRow) -> Unit,
    onDelete: (PersonRow) -> Unit,
    onAddAddress: (ByteArray) -> Unit = {},
    onAddComment: (ByteArray) -> Unit = {},
    commentsRefreshTrigger: MutableSharedFlow<Unit>? = null,
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        elevation = 2.dp,
        shape = RoundedCornerShape(8.dp)
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(10.dp)
        ) {
            // Action buttons (horizontal, on top)
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(12.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "Rnd",
                    color = MaterialTheme.colors.error,
                    fontSize = 14.sp,
                    modifier = Modifier
                        .background(
                            MaterialTheme.colors.error.copy(alpha = 0.06f),
                            RoundedCornerShape(4.dp)
                        )
                        .padding(horizontal = 6.dp, vertical = 2.dp)
                        .clickable { onRandomize(person) }
                )
                Text(
                    text = "Addr",
                    color = MaterialTheme.colors.primary,
                    fontSize = 14.sp,
                    modifier = Modifier
                        .background(
                            MaterialTheme.colors.primary.copy(alpha = 0.06f),
                            RoundedCornerShape(4.dp)
                        )
                        .padding(horizontal = 6.dp, vertical = 2.dp)
                        .clickable { onAddAddress(person.id) }
                )
                Text(
                    text = "Cmnt",
                    color = MaterialTheme.colors.primary,
                    fontSize = 14.sp,
                    modifier = Modifier
                        .background(
                            MaterialTheme.colors.primary.copy(alpha = 0.06f),
                            RoundedCornerShape(4.dp)
                        )
                        .padding(horizontal = 6.dp, vertical = 2.dp)
                        .clickable { onAddComment(person.id) }
                )
                Text(
                    text = "Del",
                    color = MaterialTheme.colors.error,
                    fontSize = 14.sp,
                    modifier = Modifier
                        .background(
                            MaterialTheme.colors.error.copy(alpha = 0.06f),
                            RoundedCornerShape(4.dp)
                        )
                        .padding(horizontal = 6.dp, vertical = 2.dp)
                        .clickable { onDelete(person) }
                )
            }

            // Person Info below
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(top = 8.dp)
            ) {
                Text(
                    text = "${person.myFirstName} ${person.myLastName}",
                    fontSize = 18.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colors.onSurface
                )

                // Comments list (if any) - now reactive to database changes and refresh triggers
                var comments by remember(person.id.toList()) {
                    mutableStateOf<List<CommentRow>>(
                        emptyList()
                    )
                }

                // Function to refresh comments
                val refreshComments = suspend {
                    val commentList = db.comment
                        .selectAll(CommentQuery.SelectAll.Params(personId = person.id))
                        .asList()
                    comments = commentList
                }

                // Initial load and reactive updates
                LaunchedEffect(person.id.toList()) {
                    // Start with reactive flow
                    launch {
                        db.comment
                            .selectAll(CommentQuery.SelectAll.Params(personId = person.id))
                            .asFlow()
                            .collect { commentList ->
                                comments = commentList
                            }
                    }

                    // Also listen for manual refresh triggers
                    commentsRefreshTrigger?.let { trigger ->
                        launch {
                            trigger.collect {
                                refreshComments()
                            }
                        }
                    }
                }
                if (comments.isNotEmpty()) {
                    Column(
                        modifier = Modifier.padding(top = 6.dp),
                        verticalArrangement = Arrangement.spacedBy(2.dp)
                    ) {
                        Text(
                            text = "Comments:",
                            fontSize = 12.sp,
                            fontWeight = FontWeight.Medium,
                            color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f)
                        )
                        comments.take(3).forEach { c ->
                            Text(
                                text = "• ${c.comment}",
                                fontSize = 12.sp,
                                color = MaterialTheme.colors.onSurface.copy(alpha = 0.8f),
                                maxLines = 2,
                                overflow = TextOverflow.Ellipsis
                            )
                        }
                    }
                }


                Text(
                    text = person.email,
                    fontSize = 14.sp,
                    color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )

                Text(
                    text = person.phone ?: "",
                    fontSize = 12.sp,
                    color = MaterialTheme.colors.onSurface.copy(alpha = 0.5f)
                )

                if (person.birthDate != null) {
                    Text(
                        text = "Born: ${person.birthDate}",
                        fontSize = 12.sp,
                        color = MaterialTheme.colors.secondary
                    )
                }

                // TODO
//                if (person.notes != null) {
//                    Text(
//                        text = "📝 ${person.notes.title}",
//                        fontSize = 12.sp,
//                        color = MaterialTheme.colors.onSurface.copy(alpha = 0.6f),
//                        maxLines = 1,
//                        overflow = TextOverflow.Ellipsis
//                    )
//                }
            }
        }
    }
}

/**
 * Creates a simple authenticated HttpClient for the sample.
 *
 * The sample uses the same direct Authorization-header pattern as the real-server smoke tests.
 * That path is reliable across repeated attach/sync cycles and avoids the extra auth-plugin
 * request pipeline that was cancelling later sample sync requests before they reached the server.
 */
private fun createAuthenticatedHttpClient(
    baseUrl: String,
    token: String,
): HttpClient {
    return HttpClient {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
        defaultRequest {
            url(baseUrl)
            header(HttpHeaders.Authorization, "Bearer $token")
        }
    }
}

private fun generateSourceId(): String {
    val alphabet = "abcdef0123456789"
    val sb = StringBuilder()
    repeat(8) {
        val idx = Random.nextInt(alphabet.length)
        sb.append(alphabet[idx])
    }
    return "source-" + sb.toString()
}

// Helper function to add a random person
suspend fun addRandomPerson(onError: (String) -> Unit) {

    val firstName = firstNames.random()
    val lastName = lastNames.random()
    val email = "${firstName.lowercase()}.${lastName.lowercase()}@${domains.random()}"
    val phone = if (Random.nextBoolean()) "555-${Random.nextInt(100, 999)}-${
        Random.nextInt(
            1000,
            9999
        )
    }" else null
    val birthDate = if (Random.nextBoolean()) {
        LocalDate(
            year = Random.nextInt(1950, 2005),
            month = Random.nextInt(1, 13),
            day = Random.nextInt(1, 29)
        )
    } else null

    val ssn: Long? = if (Random.nextBoolean()) null else Random.nextLong(100_000_000, 999_999_999)

    val notes = if (Random.nextBoolean()) {
        PersonNote(
            title = "Random Note",
            content = "This is a randomly generated note for $firstName $lastName"
        )
    } else null

    val score = if (Random.nextBoolean()) {
        Random.nextDouble(0.0, 10.0)
    } else null

    val isActive = Random.nextBoolean()

    try {
        db.person.add(
            PersonQuery.Add.Params(
                firstName = firstName,
                lastName = lastName,
                email = email,
                phone = phone ?: "",        // because we marked "phone" as not-null
                birthDate = birthDate,
                ssn = ssn,
                score = score,
                isActive = isActive,
                notes = "", // TODO
            )
        )
    } catch (e: SqliteException) {
        appLog.e(e) { "Failed to add person (SQLite)" }
        // Check if duplicate
        if (e.message?.contains("UNIQUE constraint failed") == true) {
            onError("Email already exists. Please try again with a different email.")
            return
        }
        onError("Failed to add person: ${e.message}")
    } catch (e: Exception) {
        appLog.e(e) { "Failed to add person" }
        onError("Unexpected error: ${e.message}")
    }
}

// Helper function to delete a person
suspend fun deletePerson(personId: ByteArray, onError: (String) -> Unit = {}) {
    try {
        db.person.deleteById(
            PersonQuery.DeleteById.Params(
                id = personId
            )
        )
    } catch (e: SqliteException) {
        appLog.e(e) { "Failed to delete person (SQLite)" }
        onError("Failed to delete person: ${e.message}")
    } catch (e: Exception) {
        appLog.e(e) { "Failed to delete person" }
        onError("Unexpected error: ${e.message}")
    }
}

suspend fun randomizePerson(person: PersonRow, onError: (String) -> Unit = {}) {
    try {
        val firstName = firstNames.random()
        val lastName = lastNames.random()
        val email = person.email
        val phone = person.phone
        val birthDate = person.birthDate
        val notes = person.notes
        appLog.i {
            "Rnd updating person id=${person.id.toHexLower()} " +
                "from='${person.myFirstName} ${person.myLastName}' to='$firstName $lastName'"
        }
        db.transaction {
            db.person.updateById(
                PersonQuery.UpdateById.Params(
                    id = person.id,
                    firstName = firstName,
                    lastName = lastName,
                    email = email,
                    phone = phone,
                    birthDate = birthDate,
                    ssn = person.ssn,
                    score = person.score,
                    isActive = person.isActive,
                    notes = notes
                )
            )
        }
        logLocalSyncState("after randomizePerson", person.id)
    } catch (e: SqliteException) {
        appLog.e(e) { "Failed to update person (SQLite)" }
        onError("Failed to update person: ${e.message}")
    } catch (e: Exception) {
        appLog.e(e) { "Failed to update person" }
        onError("Unexpected error: ${e.message}")
    }
}

private suspend fun logLocalSyncState(label: String, personId: ByteArray? = null) {
    val connection = db.connection()
    connection.withContextAndTrace {
        val dirtyRows = mutableListOf<String>()
        connection.prepare(
            """
            SELECT schema_name, table_name, key_json, op, base_row_version, dirty_ordinal
            FROM _sync_dirty_rows
            ORDER BY dirty_ordinal, table_name, key_json
            LIMIT 10
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                dirtyRows += buildString {
                    append("schema=")
                    append(st.getText(0))
                    append(", table=")
                    append(st.getText(1))
                    append(", key=")
                    append(st.getText(2))
                    append(", op=")
                    append(st.getText(3))
                    append(", baseRowVersion=")
                    append(st.getLong(4))
                    append(", dirtyOrdinal=")
                    append(st.getLong(5))
                }
            }
        }

        val rowStates = mutableListOf<String>()
        connection.prepare(
            """
            SELECT schema_name, table_name, key_json, row_version, deleted
            FROM _sync_row_state
            ORDER BY table_name, key_json
            LIMIT 10
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                rowStates += buildString {
                    append("schema=")
                    append(st.getText(0))
                    append(", table=")
                    append(st.getText(1))
                    append(", key=")
                    append(st.getText(2))
                    append(", rowVersion=")
                    append(st.getLong(3))
                    append(", deleted=")
                    append(st.getLong(4) == 1L)
                }
            }
        }

        val syncState = connection.prepare(
            """
            SELECT
              (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1),
              (SELECT source_id FROM _sync_source_state WHERE source_id = (
                SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1
              )),
              (SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (
                SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1
              )),
              (SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1),
              (SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1),
              (SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1)
            """.trimIndent()
        ).use { st ->
            if (!st.step()) {
                "<missing>"
            } else {
                buildString {
                    append("attachment.currentSourceId=")
                    append(st.getText(0))
                    append(", source.sourceId=")
                    append(st.getText(1))
                    append(", source.nextSourceBundleId=")
                    append(st.getLong(2))
                    append(", attachment.lastBundleSeqSeen=")
                    append(st.getLong(3))
                    append(", attachment.rebuildRequired=")
                    append(st.getLong(4) == 1L)
                    append(", apply.applyMode=")
                    append(st.getLong(5) == 1L)
                }
            }
        }

        val personSummary = personId?.let { id ->
            connection.prepare(
                """
                SELECT lower(hex(id)), first_name, last_name, email
                FROM person
                WHERE id = ?
                """.trimIndent()
            ).use { st ->
                st.bindBlob(1, id)
                if (!st.step()) {
                    "person=${id.toHexLower()} missing"
                } else {
                    "person=${st.getText(0)} name='${st.getText(1)} ${st.getText(2)}' email='${st.getText(3)}'"
                }
            }
        } ?: "person=<not requested>"

        appLog.i {
            buildString {
                append("Local sync state [")
                append(label)
                append("] syncState=")
                append(syncState)
                append("; dirtyRows=")
                append(if (dirtyRows.isEmpty()) "<none>" else dirtyRows.joinToString(" | "))
                append("; rowState=")
                append(if (rowStates.isEmpty()) "<none>" else rowStates.joinToString(" | "))
                append("; ")
                append(personSummary)
            }
        }
    }
}

private fun ByteArray.toHexLower(): String = buildString(size * 2) {
    for (byte in this@toHexLower) {
        val value = byte.toInt() and 0xff
        append("0123456789abcdef"[value ushr 4])
        append("0123456789abcdef"[value and 0x0f])
    }
}

@OptIn(ExperimentalUuidApi::class)
private fun generateUuid(): String = Uuid.random().toString()

suspend fun addRandomAddress(personId: ByteArray, onError: (String) -> Unit = {}) {
    try {
        val streets = listOf("Main St", "Oak Ave", "Pine Rd", "Maple Blvd")
        val cities = listOf("Springfield", "Riverdale", "Fairview", "Greenville")
        val state = listOf("CA", "NY", "TX", "WA").random()
        val addressType = if (Random.nextBoolean()) AddressType.HOME else AddressType.WORK
        val street = "${Random.nextInt(10, 9999)} ${streets.random()}"
        val city = cities.random()
        val postal = "${Random.nextInt(10000, 99999)}"
        val country = "US"
        val isPrimary = Random.nextBoolean()
        db.personAddress.add(
            PersonAddressQuery.Add.Params(
                personId = personId,
                addressType = addressType,
                street = street,
                city = city,
                state = state,
                postalCode = postal,
                country = country,
                isPrimary = isPrimary,
            )
        )
    } catch (e: Exception) {
        appLog.e(e) { "Failed to add address" }
        onError("Failed to add address: ${e.message}")
    }
}

suspend fun addRandomComment(personId: ByteArray, onError: (String) -> Unit = {}) {
    try {
        val comments =
            listOf("Great person!", "Met at the event.", "Loves Kotlin", "Follows up quickly")
        val created = Clock.System.now()
        db.comment.add(
            CommentQuery.Add.Params(
                id = generateUuid(),
                personId = personId,
                comment = comments.random(),
                createdAt = created,
                tags = emptyList(),
            )
        )
    } catch (e: Exception) {
        appLog.e(e) { "Failed to add comment" }
        onError("Failed to add comment: ${e.message}")
    }
}


@Composable
private fun HeaderSection(personCount: Int) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(
            text = "SQLiteNow Showcase",
            fontSize = 24.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colors.primary
        )
    }
}

@Composable
private fun SignInSection(
    signedIn: Boolean,
    skippedSignin: Boolean,
    username: String,
    signingOut: Boolean,
    onSignInClick: () -> Unit,
    onSignOutClick: () -> Unit,
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        if (signedIn) {
            Row(
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "Signed in as ${username.ifBlank { "(anonymous)" }}",
                    fontSize = 14.sp,
                    color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f)
                )
                TextButton(
                    onClick = onSignOutClick,
                    enabled = !signingOut
                ) {
                    Text(if (signingOut) "Signing Out..." else "Sign Out")
                }
            }
        } else if (skippedSignin) {
            Button(onClick = onSignInClick) { Text("Sign In") }
        } else {
            Text(
                text = "Not signed in",
                fontSize = 14.sp,
                color = MaterialTheme.colors.onSurface.copy(alpha = 0.6f)
            )
        }
    }
}

@Composable
private fun ActionButtonsRow(
    signedIn: Boolean,
    lifecycleReady: Boolean,
    onAddPerson: () -> Unit,
    onSync: () -> Unit,
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Button(
            onClick = onAddPerson,
            modifier = Modifier.weight(1f),
            colors = ButtonDefaults.buttonColors(backgroundColor = MaterialTheme.colors.primary)
        ) { Text("Add Person", fontSize = 16.sp) }

        Button(
            onClick = onSync,
            enabled = signedIn && lifecycleReady,
            modifier = Modifier.weight(1f),
            colors = ButtonDefaults.buttonColors(backgroundColor = MaterialTheme.colors.secondary)
        ) { Text("Sync", fontSize = 16.sp) }
    }
}

@Composable
private fun PersonsList(
    persons: List<PersonRow>,
    onRandomize: (PersonRow) -> Unit,
    onDelete: (PersonRow) -> Unit,
    onAddAddress: (ByteArray) -> Unit,
    onAddComment: (ByteArray) -> Unit,
    commentsRefreshTrigger: MutableSharedFlow<Unit>? = null,
) {
    Column(
        verticalArrangement = Arrangement.spacedBy(4.dp),
        modifier = Modifier.verticalScroll(rememberScrollState())
    ) {
        persons.forEach { person ->
            PersonCard(
                person = person,
                onRandomize = onRandomize,
                onDelete = onDelete,
                onAddAddress = onAddAddress,
                onAddComment = onAddComment,
                commentsRefreshTrigger = commentsRefreshTrigger,
            )
        }
    }
}

@Composable
private fun EmptyPersonsPlaceholder() {
    Card(
        modifier = Modifier.fillMaxWidth(),
        elevation = 2.dp,
        shape = RoundedCornerShape(8.dp)
    ) {
        Column(
            modifier = Modifier.padding(24.dp),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Text(
                text = "No persons yet",
                fontSize = 18.sp,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f)
            )
            Text(
                text = "Add your first person to get started",
                fontSize = 14.sp,
                color = MaterialTheme.colors.onSurface.copy(alpha = 0.5f)
            )
        }
    }
}

@Composable
private fun ErrorDialog(message: String?, onDismiss: () -> Unit) {
    message?.let { error ->
        AlertDialog(
            onDismissRequest = onDismiss,
            title = {
                val isError = error.contains("failed", ignoreCase = true) || error.contains(
                    "error",
                    ignoreCase = true
                )
                val titleText = if (isError) "Error" else "Sync Report"
                val titleColor =
                    if (isError) MaterialTheme.colors.error else MaterialTheme.colors.primary
                Text(text = titleText, fontWeight = FontWeight.Bold, color = titleColor)
            },
            text = { Text(text = error, fontSize = 14.sp) },
            confirmButton = { TextButton(onClick = onDismiss) { Text("OK") } },
            backgroundColor = MaterialTheme.colors.surface,
            contentColor = MaterialTheme.colors.onSurface
        )
    }
}

@Composable
private fun SignInDialog(
    show: Boolean,
    username: String,
    password: String,
    signingIn: Boolean,
    onUsernameChange: (String) -> Unit,
    onPasswordChange: (String) -> Unit,
    onConfirm: () -> Unit,
    onSkip: () -> Unit,
) {
    if (!show) return
    AlertDialog(
        onDismissRequest = { /* block dismiss */ },
        title = { Text(text = "Sign In", fontWeight = FontWeight.Bold) },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                OutlinedTextField(
                    value = username,
                    onValueChange = onUsernameChange,
                    label = { Text("Username") },
                    singleLine = true
                )
                OutlinedTextField(
                    value = password,
                    onValueChange = onPasswordChange,
                    label = { Text("Password") },
                    singleLine = true
                )
            }
        },
        confirmButton = {
            Button(
                onClick = onConfirm,
                enabled = !signingIn
            ) { Text(if (signingIn) "Signing In..." else "Sign In") }
        },
        dismissButton = { TextButton(onClick = onSkip) { Text("Skip") } }
    )
}
