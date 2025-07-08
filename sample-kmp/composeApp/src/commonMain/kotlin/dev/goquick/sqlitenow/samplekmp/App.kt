package dev.goquick.sqlitenow.samplekmp

import androidx.compose.foundation.background
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
import androidx.sqlite.SQLiteException
import dev.goquick.sqlitenow.core.resolveDatabasePath
import dev.goquick.sqlitenow.core.util.fromSqliteDate
import dev.goquick.sqlitenow.core.util.fromSqliteTimestamp
import dev.goquick.sqlitenow.core.util.jsonDecodeFromSqlite
import dev.goquick.sqlitenow.core.util.jsonEncodeToSqlite
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.samplekmp.db.AddressType
import dev.goquick.sqlitenow.samplekmp.db.NowSampleDatabase
import dev.goquick.sqlitenow.samplekmp.db.Person
import dev.goquick.sqlitenow.samplekmp.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.samplekmp.model.PersonNote
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.datetime.Clock
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import org.jetbrains.compose.ui.tooling.preview.Preview
import kotlin.random.Random

typealias PersonEntity = Person.SharedResult.Row

private val firstNames = listOf(
    "John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve",
    "Frank", "Grace", "Henry"
)
private val lastNames = listOf(
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez"
)
private val domains = listOf("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com")


val db = NowSampleDatabase(
    resolveDatabasePath("test04.db"),
    personAdapters = NowSampleDatabase.PersonAdapters(
        birthDateToSqlColumn = { it?.toSqliteDate() },
        notesToSqlColumn = { it?.let { PersonNote.serialize(it) } },
        sqlColumnToBirthDate = {
            it?.let { LocalDate.fromSqliteDate(it) }
        },
        sqlColumnToAddressType = {
            AddressType.from(it)
        },
        sqlColumnToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
        sqlColumnToNotes = { it?.let { PersonNote.deserialize(it) } }
    ),
    commentAdapters = NowSampleDatabase.CommentAdapters(
        sqlColumnToCreatedAt = {
            LocalDateTime.fromSqliteTimestamp(it)
        },
        createdAtToSqlColumn = {
            it.toSqliteTimestamp()
        },
        sqlColumnToTags = { it?.jsonDecodeFromSqlite() ?: emptyList() },
        tagsToSqlColumn = { it?.jsonEncodeToSqlite() }
    ),
    personAddressAdapters = NowSampleDatabase.PersonAddressAdapters(
        addressTypeToSqlColumn = { it.value },
        sqlColumnToAddressType = { AddressType.from(it) },
        sqlColumnToCreatedAt = { Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault()) },
    ),
    migration = VersionBasedDatabaseMigrations()
)

@Composable
@Preview
fun App() {
    val coroutineScope = rememberCoroutineScope()
    var persons by remember {
        mutableStateOf<List<PersonEntity>>(emptyList())
    }
    var isLoading by remember { mutableStateOf(true) }
    var errorMessage by remember { mutableStateOf<String?>(null) }

    DisposableEffect(Unit) {
        onDispose {
            // TODO .close() is here just for demo purposes. In your app you should close it in some other place
            @OptIn(DelicateCoroutinesApi::class)
            GlobalScope.launch {
                db.close()
            }
        }
    }

    LaunchedEffect(Unit) {

        // TODO .open() is here just for demo purposes. In your app you should open it in some other place
        db.open()

        // Listen for real-time changes from Person/SelectAll query
        db.person
            .selectAll(
                Person.SelectAll.Params(
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
    LaunchedEffect(Unit) {
        delay(2000L)
        db.person
            .selectAllByBirthdayRange(
                Person.SelectAllByBirthdayRange.Params(
                    startDate = LocalDate(1990, 1, 1),
                    endDate = LocalDate(2000, 1, 1)
                )
            )
            .asFlow()
            .flowOn(Dispatchers.Main)
            .collect {
                println("----> Persons born between 1990 and 2000: $it")
            }
    }

    MaterialTheme {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            // Header
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Column {
                    Text(
                        text = "SQLiteNow Showcase",
                        fontSize = 24.sp,
                        fontWeight = FontWeight.Bold,
                        color = MaterialTheme.colors.primary
                    )
                    Text(
                        text = "Real-time reactive database",
                        fontSize = 14.sp,
                        color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f)
                    )
                }

                Text(
                    text = "${persons.size} persons",
                    fontSize = 16.sp,
                    fontWeight = FontWeight.Medium,
                    color = MaterialTheme.colors.secondary
                )
            }

            // Add Person Button
            Button(
                onClick = {
                    coroutineScope.launch {
                        addRandomPerson { error ->
                            errorMessage = error
                        }
                    }
                },
                modifier = Modifier.fillMaxWidth(),
                colors = ButtonDefaults.buttonColors(
                    backgroundColor = MaterialTheme.colors.primary
                )
            ) {
                Text("Add Random Person", fontSize = 16.sp)
            }

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
                Card(
                    modifier = Modifier.fillMaxWidth(),
                    elevation = 4.dp,
                    shape = RoundedCornerShape(12.dp)
                ) {
                    Column(
                        modifier = Modifier.padding(32.dp),
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
            } else {
                Column(
                    verticalArrangement = Arrangement.spacedBy(8.dp),
                    modifier = Modifier
                        .verticalScroll(rememberScrollState())
                ) {
                    persons.forEach { person ->
                        PersonCard(
                            person = person,
                            onRandomize = {
                                coroutineScope.launch {
                                    randomizePerson(it) { error ->
                                        errorMessage = error
                                    }
                                }
                            },
                            onDelete = { personToDelete ->
                                coroutineScope.launch {
                                    deletePerson(personToDelete.id) { error ->
                                        errorMessage = error
                                    }
                                }
                            }
                        )
                    }
                }
            }
        }

        // Error Dialog
        errorMessage?.let { error ->
            AlertDialog(
                onDismissRequest = { errorMessage = null },
                title = {
                    Text(
                        text = "Database Error",
                        fontWeight = FontWeight.Bold,
                        color = MaterialTheme.colors.error
                    )
                },
                text = {
                    Text(
                        text = error,
                        fontSize = 14.sp
                    )
                },
                confirmButton = {
                    TextButton(
                        onClick = { errorMessage = null }
                    ) {
                        Text("OK")
                    }
                },
                backgroundColor = MaterialTheme.colors.surface,
                contentColor = MaterialTheme.colors.onSurface
            )
        }
    }
}

@Composable
fun PersonCard(
    person: PersonEntity,
    onRandomize: (PersonEntity) -> Unit,
    onDelete: (PersonEntity) -> Unit
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        elevation = 4.dp,
        shape = RoundedCornerShape(12.dp)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Person Info
            Column(
                modifier = Modifier.weight(1f)
            ) {
                Text(
                    text = "${person.myFirstName} ${person.myLastName}",
                    fontSize = 18.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colors.onSurface
                )

                Text(
                    text = person.email,
                    fontSize = 14.sp,
                    color = MaterialTheme.colors.onSurface.copy(alpha = 0.7f),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )

                if (person.phone != null) {
                    Text(
                        text = person.phone,
                        fontSize = 12.sp,
                        color = MaterialTheme.colors.onSurface.copy(alpha = 0.5f)
                    )
                }

                if (person.birthDate != null) {
                    Text(
                        text = "Born: ${person.birthDate}",
                        fontSize = 12.sp,
                        color = MaterialTheme.colors.secondary
                    )
                }

                if (person.notes != null) {
                    Text(
                        text = "📝 ${person.notes.title}",
                        fontSize = 12.sp,
                        color = MaterialTheme.colors.onSurface.copy(alpha = 0.6f),
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                }
            }

            // Randomize Button
            TextButton(
                onClick = { onRandomize(person) },
                modifier = Modifier
                    .background(
                        color = MaterialTheme.colors.error.copy(alpha = 0.1f),
                        shape = RoundedCornerShape(8.dp)
                    )
            ) {
                Text(
                    text = "R",
                    color = MaterialTheme.colors.error,
                    fontSize = 14.sp
                )
            }

            // Delete Button
            TextButton(
                onClick = { onDelete(person) },
                modifier = Modifier
                    .background(
                        color = MaterialTheme.colors.error.copy(alpha = 0.1f),
                        shape = RoundedCornerShape(8.dp)
                    )
            ) {
                Text(
                    text = "X",
                    color = MaterialTheme.colors.error,
                    fontSize = 14.sp
                )
            }
        }
    }
}

// Helper function to add a random person
suspend fun addRandomPerson(onError: (String) -> Unit) {

    val firstName = firstNames.random()
    val lastName = lastNames.random()
    val email = "${firstName.lowercase()}.${lastName.lowercase()}@${domains.random()}"
    val phone = if (Random.nextBoolean()) "555-${Random.nextInt(100, 999)}-${Random.nextInt(1000, 9999)}" else null
    val birthDate = if (Random.nextBoolean()) {
        LocalDate(
            year = Random.nextInt(1950, 2005),
            monthNumber = Random.nextInt(1, 13),
            dayOfMonth = Random.nextInt(1, 29)
        )
    } else null

    val notes = if (Random.nextBoolean()) {
        PersonNote(
            title = "Random Note",
            content = "This is a randomly generated note for $firstName $lastName"
        )
    } else null

    try {
        db.person.add(
            Person.Add.Params(
                firstName = firstName,
                lastName = lastName,
                email = email,
                phone = phone,
                birthDate = birthDate,
                notes = notes,
            )
        ).execute()
    } catch (e: SQLiteException) {
        e.printStackTrace()
        // Check if duplicate
        if (e.message?.contains("UNIQUE constraint failed") == true) {
            onError("Email already exists. Please try again with a different email.")
            return
        }
        onError("Failed to add person: ${e.message}")
    } catch (e: Exception) {
        e.printStackTrace()
        onError("Unexpected error: ${e.message}")
    }
}

// Helper function to delete a person
suspend fun deletePerson(personId: Long, onError: (String) -> Unit = {}) {
    try {
        db.person.deleteByIds(
            Person.DeleteByIds.Params(
                ids = listOf(personId)
            )
        ).execute()
    } catch (e: SQLiteException) {
        e.printStackTrace()
        onError("Failed to delete person: ${e.message}")
    } catch (e: Exception) {
        e.printStackTrace()
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
        db.person.updateById(
            Person.UpdateById.Params(
                id = person.id,
                firstName = firstName,
                lastName = lastName,
                email = email,
                phone = phone,
                birthDate = birthDate,
                notes = notes
            )
        ).execute()
    } catch (e: SQLiteException) {
        e.printStackTrace()
        onError("Failed to delete person: ${e.message}")
    } catch (e: Exception) {
        e.printStackTrace()
        onError("Unexpected error: ${e.message}")
    }
}
