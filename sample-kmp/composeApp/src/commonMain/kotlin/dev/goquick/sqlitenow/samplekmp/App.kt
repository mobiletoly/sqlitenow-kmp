package dev.goquick.sqlitenow.samplekmp

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
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
import dev.goquick.sqlitenow.core.util.jsonDecodeListFromSqlite
import dev.goquick.sqlitenow.core.util.jsonEncodeToSqlite
import dev.goquick.sqlitenow.core.util.toSqliteDate
import dev.goquick.sqlitenow.core.util.toSqliteTimestamp
import dev.goquick.sqlitenow.samplekmp.db.AddressType
import dev.goquick.sqlitenow.samplekmp.db.NowSampleDatabase
import dev.goquick.sqlitenow.samplekmp.db.PersonQuery
import dev.goquick.sqlitenow.samplekmp.db.PersonRow
import dev.goquick.sqlitenow.samplekmp.db.PersonWithAddressRow
import dev.goquick.sqlitenow.samplekmp.db.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.samplekmp.model.PersonNote
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import org.jetbrains.compose.ui.tooling.preview.Preview
import kotlin.random.Random

private val firstNames = listOf(
    // Traditional English names
    "John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "William", "Mary", "James", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
    "Richard", "Barbara", "Joseph", "Susan", "Thomas", "Jessica", "Christopher", "Sarah", "Daniel", "Karen",
    "Paul", "Nancy", "Mark", "Lisa", "Donald", "Betty", "George", "Helen", "Kenneth", "Sandra",
    "Steven", "Donna", "Edward", "Carol", "Brian", "Ruth", "Ronald", "Sharon", "Anthony", "Michelle",
    "Kevin", "Laura", "Jason", "Sarah", "Matthew", "Kimberly", "Gary", "Deborah", "Timothy", "Dorothy",
    "Jose", "Amy", "Larry", "Angela", "Jeffrey", "Ashley", "Frank", "Brenda", "Scott", "Emma",
    "Eric", "Olivia", "Stephen", "Cynthia", "Andrew", "Marie", "Raymond", "Janet", "Gregory", "Catherine",
    "Joshua", "Frances", "Jerry", "Christine", "Dennis", "Samantha", "Walter", "Debra", "Patrick", "Rachel",
    "Peter", "Carolyn", "Harold", "Janet", "Douglas", "Virginia", "Henry", "Maria", "Carl", "Heather",
    "Alexander", "Sophia", "Benjamin", "Isabella", "Lucas", "Charlotte", "Mason", "Amelia", "Ethan", "Mia",
    "Noah", "Harper", "Logan", "Evelyn", "Jacob", "Abigail", "Jackson", "Emily", "Aiden", "Elizabeth",
    "Sebastian", "Sofia", "Gabriel", "Avery", "Carter", "Ella", "Jayden", "Madison", "Luke", "Scarlett",
    "Anthony", "Victoria", "Isaac", "Aria", "Dylan", "Grace", "Wyatt", "Chloe", "Owen", "Camila",
    "Caleb", "Penelope", "Nathan", "Riley", "Ryan", "Layla", "Hunter", "Lillian", "Christian", "Nora",
    "Landon", "Zoey", "Adrian", "Mila", "Jonathan", "Aubrey", "Nolan", "Hannah", "Cameron", "Lily",
    "Connor", "Addison", "Santiago", "Eleanor", "Jeremiah", "Natalie", "Ezekiel", "Luna", "Angel", "Savannah",
    "Robert", "Brooklyn", "Axel", "Leah", "Colton", "Zoe", "Jordan", "Stella", "Dominic", "Hazel",
    "Austin", "Ellie", "Ian", "Paisley", "Adam", "Violet", "Eli", "Claire", "Jose", "Bella",
    "Jaxon", "Aurora", "Rowan", "Lucy", "Felix", "Anna", "Silas", "Samantha", "Miles", "Caroline"
)

private val lastNames = listOf(
    // Common American surnames
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
    "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
    "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper",
    "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez",
    "O'Connor", "MacDonald", "O'Brien", "Sullivan", "Kennedy", "Murphy", "O'Sullivan", "Walsh", "Ryan", "Byrne",
    "Schmidt", "Mueller", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Schulz", "Hoffmann",
    "Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci", "Marino", "Greco",
    "Singh", "Kumar", "Sharma", "Gupta", "Khan", "Ahmed", "Ali", "Hassan", "Hussein", "Rahman",
    "Chen", "Wang", "Li", "Zhang", "Liu", "Yang", "Huang", "Zhao", "Wu", "Zhou",
    "Tanaka", "Suzuki", "Takahashi", "Watanabe", "Ito", "Yamamoto", "Nakamura", "Kobayashi", "Kato", "Yoshida",
    "Johansson", "Andersson", "Karlsson", "Nilsson", "Eriksson", "Larsson", "Olsson", "Persson", "Svensson", "Gustafsson",
    "Petrov", "Ivanov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Volkov", "Sokolov", "Mikhailov", "Fedorov",
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira", "Lima", "Gomes",
    "Dubois", "Martin", "Bernard", "Moreau", "Laurent", "Simon", "Michel", "Lefebvre", "Leroy", "Roux"
)

private val domains = listOf("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com")

val db = NowSampleDatabase(
    resolveDatabasePath("test04.db"),
    personAdapters = NowSampleDatabase.PersonAdapters(
        notesToSqlValue = { it?.let { PersonNote.serialize(it) } },
        birthDateToSqlValue = {
            it?.toSqliteDate()
        },
        sqlValueToBirthDate = {
            it?.let { LocalDate.fromSqliteDate(it) }
        },
        sqlValueToNotes = { it?.let { PersonNote.deserialize(it) } },
        sqlValueToAddressType = { AddressType.from(it) },
    ),
    commentAdapters = NowSampleDatabase.CommentAdapters(
        createdAtToSqlValue = { ts -> ts.toSqliteTimestamp() },
        tagsToSqlValue = { tags -> tags?.jsonEncodeToSqlite() },
        sqlValueToCreatedAt = { LocalDateTime.fromSqliteTimestamp(it) },
        sqlValueToTags = { it?.jsonDecodeListFromSqlite() ?: emptyList() },
    ),
    personAddressAdapters = NowSampleDatabase.PersonAddressAdapters(
        addressTypeToSqlValue = { it.value },
    ),
    migration = VersionBasedDatabaseMigrations()
)

@Composable
@Preview
fun App() {
    val coroutineScope = rememberCoroutineScope()
    var persons by remember {
        mutableStateOf<List<PersonRow>>(emptyList())
    }
    var isLoading by remember { mutableStateOf(true) }
    var errorMessage by remember { mutableStateOf<String?>(null) }

    LaunchedEffect(Unit) {

        // TODO .open() is here just for demo purposes. In your app you should open it in some other place
        db.open()
        db.connection().execSQL("PRAGMA foreign_keys = ON;")

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
            .collectLatest {
                // This code runs on Dispatchers.Main (UI)
                persons = it
                isLoading = false
            }
    }

    LaunchedEffect(Unit) {
        delay(1000)
        db.person
            .selectAllWithAddresses(
                PersonQuery.SelectAllWithAddresses.Params(
                    limit = 20,
                    offset = 0
                )
            )
            .asFlow()
            .collectLatest { personWithAddressList: List<PersonWithAddressRow> ->
                for (person in personWithAddressList) {
                    println("----> Person: ${person.myFirstName} ${person.myLastName} - <${person.personPhone}> <${person.personBirthDate}>")
                    for (address in person.addresses) {
                        println("    ----> Address: $address")
                    }
                    for (comment in person.comments) {
                        println("    ----> Comment: $comment")
                    }
                }
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
    person: PersonRow,
    onRandomize: (PersonRow) -> Unit,
    onDelete: (PersonRow) -> Unit
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
            Column {
                TextButton(
                    onClick = { onRandomize(person) },
                    contentPadding = PaddingValues(start = 1.dp, top = 1.dp, end = 1.dp, bottom = 1.dp),
                    modifier = Modifier
                        .padding(2.dp)
                        .background(
                            color = MaterialTheme.colors.error.copy(alpha = 0.1f),
                            shape = RoundedCornerShape(8.dp)
                        )
                ) {
                    Text(
                        text = "Rnd",
                        color = MaterialTheme.colors.error,
                        fontSize = 14.sp
                    )
                }

                // Delete Button
                TextButton(
                    onClick = { onDelete(person) },
                    contentPadding = PaddingValues(start = 1.dp, top = 1.dp, end = 1.dp, bottom = 1.dp),
                    modifier = Modifier
                        .padding(2.dp)
                        .background(
                            color = MaterialTheme.colors.error.copy(alpha = 0.1f),
                            shape = RoundedCornerShape(8.dp)
                        )
                ) {
                    Text(
                        text = "Del",
                        color = MaterialTheme.colors.error,
                        fontSize = 14.sp
                    )
                }
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
            month = Random.nextInt(1, 13),
            day = Random.nextInt(1, 29)
        )
    } else null

    val notes = if (Random.nextBoolean()) {
        PersonNote(
            title = "Random Note",
            content = "This is a randomly generated note for $firstName $lastName"
        )
    } else null

    try {
        val results = db.person.addReturning.list(
            PersonQuery.AddReturning.Params(
                firstName = firstName,
                lastName = lastName,
                email = email,
                phone = phone ?: "",        // because we marked "phone" as not-null
                birthDate = birthDate,
                notes = notes,
            )
        )
        println(results)
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
            PersonQuery.DeleteByIds.Params(
                ids = listOf(personId)
            )
        )
    } catch (e: SQLiteException) {
        e.printStackTrace()
        onError("Failed to delete person: ${e.message}")
    } catch (e: Exception) {
        e.printStackTrace()
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
        db.transaction {
            db.person.updateById(
                PersonQuery.UpdateById.Params(
                    id = person.id,
                    firstName = firstName,
                    lastName = lastName,
                    email = email,
                    phone = phone,
                    birthDate = birthDate,
                    notes = notes
                )
            )
        }
    } catch (e: SQLiteException) {
        e.printStackTrace()
        onError("Failed to update person: ${e.message}")
    } catch (e: Exception) {
        e.printStackTrace()
        onError("Unexpected error: ${e.message}")
    }
}
