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
import androidx.sqlite.SQLiteException
import dev.goquick.sqlitenow.common.resolveDatabasePath
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.jsonDecodeFromSqlite
import dev.goquick.sqlitenow.core.util.jsonEncodeToSqlite
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.samplesynckmp.db.AddressType
import dev.goquick.sqlitenow.samplesynckmp.db.CommentQuery
import dev.goquick.sqlitenow.samplesynckmp.db.NowSampleSyncDatabase
import dev.goquick.sqlitenow.samplesynckmp.db.PersonAddressQuery
import dev.goquick.sqlitenow.samplesynckmp.db.PersonQuery
import dev.goquick.sqlitenow.samplesynckmp.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.samplesynckmp.model.PersonNote
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import org.jetbrains.compose.ui.tooling.preview.Preview
import kotlin.random.Random
import kotlin.time.Clock
import kotlin.time.ExperimentalTime
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

typealias PersonEntity = PersonQuery.SharedResult.Row

typealias PersonAddressEntity = PersonAddressQuery.SharedResult.Row
typealias PersonWithAddressesEntity = PersonQuery.SharedResult.PersonWithAddressRow

private val firstNames = listOf(
    "John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve",
    "Frank", "Grace", "Henry"
)
private val lastNames = listOf(
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez"
)
private val domains = listOf("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com")
typealias CommentEntity = CommentQuery.SharedResult.Row

val db = NowSampleSyncDatabase(
    resolveDatabasePath("test05.db"),
    personAdapters = NowSampleSyncDatabase.PersonAdapters(
//        notesToSqlValue = { it?.let { PersonNote.serialize(it) } },
        birthDateToSqlValue = {
            it?.toSqliteDate()
        },
        sqlValueToBirthDate = {
            it?.let { LocalDate.fromSqliteDate(it) }
        },
        sqlValueToAddressType = {
            AddressType.from(it)
        },
        sqlValueToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
//        sqlValueToNotes = { it?.let { PersonNote.deserialize(it) } },
        sqlValueToTags = {
            it?.jsonDecodeFromSqlite() ?: emptyList()
        },
    ),
    commentAdapters = NowSampleSyncDatabase.CommentAdapters(
        sqlValueToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
        createdAtToSqlValue = {
            it.toSqliteTimestamp()
        },
        sqlValueToTags = { it?.jsonDecodeFromSqlite() ?: emptyList() },
        tagsToSqlValue = { it?.jsonEncodeToSqlite() }
    ),
    personAddressAdapters = NowSampleSyncDatabase.PersonAddressAdapters(
        addressTypeToSqlValue = { it.value },
        sqlValueToAddressType = { AddressType.from(it) },
        sqlValueToCreatedAt = {
            it.let { LocalDateTime.fromSqliteTimestamp(it) }
        },
    ),
    migration = VersionBasedDatabaseMigrations()
)

@OptIn(FlowPreview::class)
@Composable
@Preview
fun App() {
    val coroutineScope = rememberCoroutineScope()
    var persons by remember {
        mutableStateOf<List<PersonEntity>>(emptyList())
    }
    var isLoading by remember { mutableStateOf(true) }
    var errorMessage by remember { mutableStateOf<String?>(null) }
    var syncClient by remember { mutableStateOf<OversqliteClient?>(null) }
    var bootstrapDone by remember { mutableStateOf(false) }
    var signedIn by remember { mutableStateOf(false) }
    var skippedSignin by remember { mutableStateOf(false) }
    var showSigninDialog by remember { mutableStateOf(true) }
    var username by remember { mutableStateOf("") }
    var password by remember { mutableStateOf("") }
    var signingIn by remember { mutableStateOf(false) }
    var deviceId by remember { mutableStateOf("") }
    var reportMessage by remember { mutableStateOf<String?>(null) }
    val syncTrigger = remember { MutableSharedFlow<Unit>(extraBufferCapacity = 1) }
    val commentsRefreshTrigger = remember { MutableSharedFlow<Unit>(extraBufferCapacity = 1) }

    LaunchedEffect(Unit) {

        // TODO .open() is here just for demo purposes. In your app you should open it in some other place
        db.open()

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
            .flowOn(Dispatchers.IO)     // DB and mapper code above runs on Dispatchers.IO
            .collect {
                // This code runs on Dispatchers.Main (UI)
                persons = it
                isLoading = false
            }
    }

    // Load persisted auth and device id, and restore signed-in session if token exists.
    LaunchedEffect(Unit) {
        // Ensure device id exists
        val existingDevice = AuthPrefs.get(AuthKeys.DeviceId)
        val did = existingDevice ?: generateDeviceId().also { AuthPrefs.set(AuthKeys.DeviceId, it) }
        deviceId = did

        val savedToken = AuthPrefs.get(AuthKeys.Token)
        val savedUser = AuthPrefs.get(AuthKeys.Username)
        if (!savedToken.isNullOrBlank()) {
            try {
                val baseUrl = "http://10.0.2.2:8080"
                syncClient = db.newOversqliteClientWithToken(
                    schema = "business",
                    baseUrl = baseUrl,
                    token = savedToken,
                    resolver = ServerWinsResolver
                )
                val userForBootstrap = savedUser ?: "user-sample"
                syncClient?.bootstrap(userId = userForBootstrap, sourceId = did)
                bootstrapDone = true
                signedIn = true
                skippedSignin = false
                showSigninDialog = false
                username = userForBootstrap
                appLog.i { "Restored session for user=$userForBootstrap device=$did" }
                // One quick incremental sync to catch up
                val limit = 500
                var more = true
                while (more) {
                    val res = syncClient?.downloadOnce(limit = limit)
                    val applied = res?.getOrNull()?.first ?: 0
                    if (applied > 0) appLog.d { "restore: applied=$applied" }
                    more = applied == limit
                    if (applied == 0) break
                }
                // Nudge sync worker to run soon
                syncTrigger.tryEmit(Unit)
            } catch (e: Exception) {
                // Fallback to signed-out state if token invalid
                appLog.e(e) { "Failed to restore session; showing sign-in" }
                signedIn = false
                bootstrapDone = false
                showSigninDialog = true
            }
        }
    }
    // Sync worker: runs on demand when triggered, with debounce/coalescing
    LaunchedEffect(syncClient) {
        val client = syncClient ?: return@LaunchedEffect
        appLog.i { "Sync worker active" }
        syncTrigger
            .debounce(700)
            .collectLatest {
                try {
                    val up = client.uploadOnce()
                    up.onSuccess { summary ->
                        if (summary.total > 0) {
                            val reasons = if (summary.invalidReasons.isNotEmpty()) {
                                summary.invalidReasons.entries
                                    .sortedByDescending { it.value }
                                    .joinToString(
                                        limit = 3,
                                        separator = ", "
                                    ) { "${it.key}=${it.value}" }
                            } else "none"
                            appLog.i { "Upload: total=${summary.total} applied=${summary.applied} conflict=${summary.conflict} invalid=${summary.invalid} (reasons: ${reasons}) materialize_error=${summary.materializeError}" }
                            if (summary.conflict > 0 || summary.invalid > 0 || summary.materializeError > 0) {
                                // Prefer the first concrete error message if available
                                errorMessage = summary.firstErrorMessage ?: buildString {
                                    append("Upload issues: conflicts=${summary.conflict}, invalid=${summary.invalid}")
                                    if (summary.invalidReasons.isNotEmpty()) {
                                        append(" [")
                                        append(
                                            summary.invalidReasons.entries
                                                .sortedByDescending { it.value }
                                                .joinToString(
                                                    limit = 3,
                                                    separator = ", "
                                                ) { "${it.key}=${it.value}" })
                                        append("]")
                                    }
                                }
                            }
                        }
                    }.onFailure { appLog.e(it) { "uploadOnce failed" } }
                    val limit = 500
                    var more = true
                    var total = 0
                    while (more) {
                        val (applied, _) = client.downloadOnce(limit = limit)
                            .getOrNull() ?: (0 to 0L)
                        total += applied
                        more = applied == limit
                        if (applied == 0) break
                    }
                    if (total > 0) appLog.i { "Applied $total changes" }
                } catch (e: Exception) {
                    appLog.e(e) { "Sync cycle failed" }
                }
            }
    }

    // Periodic nudge (e.g., every 60s) only when signed in
    LaunchedEffect(signedIn, bootstrapDone) {
//        if (signedIn && bootstrapDone) {
//            appLog.i { "Scheduling periodic sync every 3s" }
//            while (signedIn) {
//                delay(3_000)
//                syncTrigger.tryEmit(Unit)
//            }
//        }
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
                onSignInClick = { showSigninDialog = true }
            )

            // Actions row: Add Person + Sync
            ActionButtonsRow(
                signedIn = signedIn,
                bootstrapDone = bootstrapDone,
                onAddPerson = {
                    coroutineScope.launch {
                        addRandomPerson { error -> errorMessage = error; reportMessage = null }
                        syncTrigger.tryEmit(Unit)
                    }
                },
                onSync = {
                    coroutineScope.launch {
                        val client =
                            syncClient ?: run { errorMessage = "Not signed in"; return@launch }
                        try {
                            val report = StringBuilder()
                            val up = client.uploadOnce()
                            up.onSuccess { summary ->
                                val reasons = if (summary.invalidReasons.isNotEmpty()) {
                                    summary.invalidReasons.entries.sortedByDescending { it.value }
                                        .joinToString(
                                            limit = 3,
                                            separator = ", "
                                        ) { "${it.key}=${it.value}" }
                                } else "none"
                                report.append("Upload: total=${summary.total} applied=${summary.applied} conflict=${summary.conflict} invalid=${summary.invalid} (reasons: ${reasons}) materialize_error=${summary.materializeError}")
                                if (summary.conflict > 0 || summary.invalid > 0 || summary.materializeError > 0) {
                                    summary.firstErrorMessage?.let { em ->
                                        report.append("\nFirst error: ").append(em)
                                    }
                                }
                            }.onFailure { err ->
                                appLog.e(err) { "Upload failed" }
                                report.append("Upload failed: ")
                                    .append(err.message ?: "unknown error")
                            }
                            val limit = 500
                            var more = true
                            var totalApplied = 0
                            while (more) {
                                val (applied, _) = client.downloadOnce(limit = limit).getOrNull()
                                    ?: (0 to 0L)
                                totalApplied += applied
                                more = applied == limit
                                if (applied == 0) break
                            }
                            if (report.isNotEmpty()) report.append('\n')
                            report.append("Download: applied=").append(totalApplied)
                            val conflictsOrErrors = up.getOrNull()
                                ?.let { it.conflict > 0 || it.invalid > 0 || it.materializeError > 0 }
                                ?: false
                            if (conflictsOrErrors) {
                                errorMessage = report.toString()
                                reportMessage = null
                            } else {
                                reportMessage = report.toString()
                                errorMessage = null
                            }
                        } catch (e: Exception) {
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
                                errorMessage = error; reportMessage = null
                            }
                            syncTrigger.tryEmit(Unit)
                        }
                    },
                    onDelete = { p ->
                        coroutineScope.launch {
                            deletePerson(p.id) { error ->
                                errorMessage = error; reportMessage = null
                            }
                            syncTrigger.tryEmit(Unit)
                        }
                    },
                    onAddAddress = { pid ->
                        coroutineScope.launch {
                            addRandomAddress(pid) { error ->
                                errorMessage = error; reportMessage = null
                            }
                            syncTrigger.tryEmit(Unit)
                        }
                    },
                    onAddComment = { pid ->
                        coroutineScope.launch {
                            addRandomComment(pid) { error ->
                                errorMessage = error; reportMessage = null
                            }
                            syncTrigger.tryEmit(Unit)
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
                confirmButton = { TextButton(onClick = { reportMessage = null }) { Text("OK") } }
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
                            val baseUrl = "http://10.0.2.2:8080"
                            val displayUser = username.ifBlank { "user-sample" }
                            appLog.i { "Signing in user='${displayUser}' device=${deviceId}" }
                            val token = fetchJwt(
                                baseUrl,
                                user = displayUser,
                                device = deviceId,
                                password = password.ifBlank { "demo" })
                            syncClient = db.newOversqliteClientWithToken(
                                schema = "business",
                                baseUrl = baseUrl,
                                token = token,
                                resolver = ServerWinsResolver
                            )
                            val finalUser = username.ifBlank { "user-sample" }
                            syncClient?.bootstrap(userId = finalUser, sourceId = deviceId)
                            bootstrapDone = true
                            signedIn = true
                            skippedSignin = false
                            showSigninDialog = false
                            AuthPrefs.set(AuthKeys.Username, finalUser)
                            AuthPrefs.set(AuthKeys.Token, token)
                            AuthPrefs.set(AuthKeys.DeviceId, deviceId)
                            appLog.i { "Sign-in success for user=$finalUser; starting hydrate" }
                            val hydrateRes = syncClient?.hydrate(limit = 1000, windowed = true)
                            if (hydrateRes?.isFailure == true) {
                                appLog.e(hydrateRes.exceptionOrNull()) { "Hydrate failed" }
                            } else {
                                appLog.i { "Hydrate complete" }
                            }
                        } catch (e: Exception) {
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
    person: PersonEntity,
    onRandomize: (PersonEntity) -> Unit,
    onDelete: (PersonEntity) -> Unit,
    onAddAddress: (String) -> Unit = {},
    onAddComment: (String) -> Unit = {},
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
                var comments by remember(person.id) { mutableStateOf<List<CommentEntity>>(emptyList()) }

                // Function to refresh comments
                val refreshComments = suspend {
                    val commentList = db.comment
                        .selectAll(CommentQuery.SelectAll.Params(personId = person.id))
                        .asList()
                    comments = commentList
                }

                // Initial load and reactive updates
                LaunchedEffect(person.id) {
                    // Start with reactive flow
                    launch {
                        db.comment
                            .selectAll(CommentQuery.SelectAll.Params(personId = person.id))
                            .asFlow()
                            .flowOn(Dispatchers.IO)
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

private fun generateDeviceId(): String {
    val alphabet = "abcdef0123456789"
    val sb = StringBuilder()
    repeat(8) {
        val idx = Random.nextInt(alphabet.length)
        sb.append(alphabet[idx])
    }
    return "device-" + sb.toString()
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
                id = generateUuid(),
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
        ).execute()
    } catch (e: SQLiteException) {
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
suspend fun deletePerson(personId: String, onError: (String) -> Unit = {}) {
    try {
        db.person.deleteByIds(
            PersonQuery.DeleteByIds.Params(
                ids = listOf(personId)
            )
        ).execute()
    } catch (e: SQLiteException) {
        appLog.e(e) { "Failed to delete person (SQLite)" }
        onError("Failed to delete person: ${e.message}")
    } catch (e: Exception) {
        appLog.e(e) { "Failed to delete person" }
        onError("Unexpected error: ${e.message}")
    }
}

suspend fun randomizePerson(person: PersonEntity, onError: (String) -> Unit = {}) {
    try {
        val firstName = firstNames.random()
        val lastName = lastNames.random()
        val email = person.email
        val phone = person.phone
        val birthDate = person.birthDate
        val notes = person.notes
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
            ).execute()
        }
    } catch (e: SQLiteException) {
        appLog.e(e) { "Failed to update person (SQLite)" }
        onError("Failed to update person: ${e.message}")
    } catch (e: Exception) {
        appLog.e(e) { "Failed to update person" }
        onError("Unexpected error: ${e.message}")
    }
}

@OptIn(ExperimentalUuidApi::class)
private fun generateUuid(): String = Uuid.random().toString()

suspend fun addRandomAddress(personId: String, onError: (String) -> Unit = {}) {
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
                id = generateUuid(),
                personId = personId,
                addressType = addressType,
                street = street,
                city = city,
                state = state,
                postalCode = postal,
                country = country,
                isPrimary = isPrimary,
            )
        ).execute()
    } catch (e: Exception) {
        appLog.e(e) { "Failed to add address" }
        onError("Failed to add address: ${e.message}")
    }
}

@OptIn(ExperimentalTime::class)
suspend fun addRandomComment(personId: String, onError: (String) -> Unit = {}) {
    try {
        val comments =
            listOf("Great person!", "Met at the event.", "Loves Kotlin", "Follows up quickly")
        val created = Clock.System.now().toLocalDateTime(TimeZone.UTC)
        db.comment.add(
            CommentQuery.Add.Params(
                id = generateUuid(),
                personId = personId,
                comment = comments.random(),
                createdAt = created,
                tags = emptyList(),
            )
        ).execute()
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
    onSignInClick: () -> Unit
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        if (signedIn) {
            Text(
                text = "Signed in as ${username.ifBlank { "(anonymous)" }}",
                fontSize = 14.sp,
                color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f)
            )
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
    bootstrapDone: Boolean,
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
            enabled = signedIn && bootstrapDone,
            modifier = Modifier.weight(1f),
            colors = ButtonDefaults.buttonColors(backgroundColor = MaterialTheme.colors.secondary)
        ) { Text("Sync", fontSize = 16.sp) }
    }
}

@Composable
private fun PersonsList(
    persons: List<PersonEntity>,
    onRandomize: (PersonEntity) -> Unit,
    onDelete: (PersonEntity) -> Unit,
    onAddAddress: (String) -> Unit,
    onAddComment: (String) -> Unit,
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
