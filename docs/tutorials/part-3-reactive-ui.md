---
layout: doc
title: Part 3 – Reactive Mood Dashboard
permalink: /tutorials/part-3-reactive-ui/
parent: Tutorials
nav_order: 3
---

# Part 3 – Reactive Mood Dashboard

We now have a generated SQLite API, tagging workflow, and typed repositories. In this article we
wire everything into a cross-platform UI, lean on SQLiteNow's query flows, and show how the
ViewModel keeps the screen up to date with almost no glue code.

We will:

- promote the generated `selectRecentWithTags` query to a shared `StateFlow`,
- compute a "this week" mood summary that recomputes whenever data changes,
- build a Compose screen with a quick-add form, the weekly summary card, and a reactive list,
- ensure the Android build stores the database under app-private storage, and
- round the weekly average to a single decimal without using platform-specific formatters.

## Step 1 – Add a Flow-backed ViewModel

Create the ViewModel in
`composeApp/src/commonMain/kotlin/dev/goquick/sample/moodtracker/data/MoodTrackerViewModel.kt`.
The ViewModel turns the generated query runners into hot `StateFlow` instances so the UI can
collect them safely.

```kotlin
class MoodTrackerViewModel(
    database: MoodTrackerDatabase,
    private val entryRepository: MoodEntryRepository,
    private val scope: CoroutineScope,
    recentLimit: Int = 30,
) {
    private val entriesFlow = database.moodEntry
        .selectRecentWithTags(
            MoodEntryQuery.SelectRecentWithTags.Params(limit = recentLimit.toLong())
        )
        .asFlow()

    val entries: StateFlow<List<MoodEntryWithTags>> = entriesFlow.stateIn(
        scope = scope,
        started = SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
        initialValue = emptyList(),
    )

    val weeklySummary: StateFlow<WeeklySummary> = entriesFlow
        .map { computeWeeklySummary(it) }
        .stateIn(
            scope = scope,
            started = SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
            initialValue = WeeklySummary.Empty,
        )

    private val _isSaving = MutableStateFlow(false)
    val isSaving: StateFlow<Boolean> = _isSaving.asStateFlow()

    fun addEntry(moodScore: Int, note: String) {
        if (_isSaving.value) return
        scope.launch {
            _isSaving.value = true
            try {
                entryRepository.add(
                    MoodEntryRepository.NewMoodEntry(
                        entryTime = Clock.System.now()
                            .toLocalDateTime(TimeZone.currentSystemDefault()),
                        moodScore = moodScore,
                        note = note.ifBlank { null },
                    )
                )
            } finally {
                _isSaving.value = false
            }
        }
    }

    private fun computeWeeklySummary(entries: List<MoodEntryWithTags>): WeeklySummary {
        if (entries.isEmpty()) return WeeklySummary.Empty

        val today = Clock.System.now()
            .toLocalDateTime(TimeZone.currentSystemDefault())
            .date
        val weekStart = today.startOfWeek()
        val weekEntries = entries.filter { it.entryTime.date >= weekStart }
        if (weekEntries.isEmpty()) {
            return WeeklySummary.Empty.copy(startDate = weekStart, endDate = today)
        }

        val average = weekEntries.map { it.moodScore }.average()
        return WeeklySummary(
            averageScore = average,
            entryCount = weekEntries.size,
            startDate = weekStart,
            endDate = today,
        )
    }

    data class WeeklySummary(
        val averageScore: Double?,
        val entryCount: Int,
        val startDate: LocalDate,
        val endDate: LocalDate,
    ) {
        companion object {
            val Empty = WeeklySummary(
                averageScore = null,
                entryCount = 0,
                startDate = LocalDate(1970, 1, 1),
                endDate = LocalDate(1970, 1, 1),
            )
        }
    }
}

private fun LocalDate.startOfWeek(): LocalDate {
    val dayOffset = (dayOfWeek.ordinal - DayOfWeek.MONDAY.ordinal + 7) % 7
    return this.minus(DatePeriod(days = dayOffset))
}
```

- `stateIn` turns the cold SQLiteNow flow into a hot `StateFlow`, caching the latest rows so every
  collector sees the same list immediately. Because `selectRecentWithTags.sql` uses a dynamic-field
  collection, each element already includes its `tags: List<MoodTagRow>`.
- We stamp new entries with `Clock.System.now().toLocalDateTime(TimeZone.currentSystemDefault())`
  and guard the insert with `_isSaving` to avoid double taps.
- The summary helpers live alongside the ViewModel and reuse the generated row model to compute the
  average and streak window.

Behind the scenes, SQLiteNow keeps the query flow hot: every insert, update, or delete that touches
`mood_entry` will push new rows downstream. Feeding that flow into `stateIn` gives Compose a single
source of truth. Screens simply `collectAsState()` and redraw; no manual observers, cursors, or DAO
callbacks are needed.

## Step 2 – Compose Screen Powered by Flows

Replace the template UI in `App.kt` with the reactive surface. The screen wires the ViewModel,
quick-add controls, weekly summary, and the entries list together. Mood buttons use a tonal style
for the selected value so the user can see what is active.

```kotlin
// App.kt – entry points

@Composable
@Preview
fun App() {
    MaterialTheme {
        Surface(modifier = Modifier.fillMaxSize()) {
            val databaseState = rememberDatabase()
            val database = databaseState.value
            if (database == null) {
                LoadingState()
            } else {
                MoodTrackerContent(database)
            }
        }
    }
}

@Composable
private fun rememberDatabase(): MutableState<MoodTrackerDatabase?> {
    val state = remember { mutableStateOf<MoodTrackerDatabase?>(null) }
    LaunchedEffect(Unit) {
        if (state.value == null) {
            state.value = MoodDatabaseFactory().create()
        }
    }
    return state
}

@Composable
private fun MoodTrackerContent(database: MoodTrackerDatabase) {
    val scope = rememberCoroutineScope()
    val viewModel = remember(database) {
        MoodTrackerViewModel(
            database = database,
            entryRepository = MoodEntryRepository(database),
            scope = scope,
        )
    }

    val entries by viewModel.entries.collectAsState()
    val summary by viewModel.weeklySummary.collectAsState()
    val isSaving by viewModel.isSaving.collectAsState()

    MoodTrackerScreen(
        entries = entries,
        summary = summary,
        isSaving = isSaving,
        onAdd = { score, note -> viewModel.addEntry(score, note) },
    )
}
```

`collectAsState()` bridges the hot flows from the ViewModel into Compose state. Whenever the
database notifies a change, `entries` and `summary` update automatically and the UI re-composes.
Because each item is a `MoodEntryWithTags`, the screen can render tag chips without additional
queries or grouping logic.

Next, build the screen scaffold. This piece arranges the quick-add card, weekly summary, and list
while delegating the individual blocks to helper composables.

```kotlin
// App.kt – screen scaffold

@Composable
private fun MoodTrackerScreen(
    entries: List<MoodEntryWithTags>,
    summary: MoodTrackerViewModel.WeeklySummary,
    isSaving: Boolean,
    onAdd: (Int, String) -> Unit,
) {
    var note by remember { mutableStateOf("") }
    var selectedMood by remember { mutableStateOf(3) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(horizontal = 20.dp, vertical = 24.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text(text = "Mood Tracker", style = MaterialTheme.typography.headlineSmall)

        QuickAddBlock(
            note = note,
            onNoteChange = { note = it },
            selectedMood = selectedMood,
            onMoodChange = { selectedMood = it },
            onAdd = {
                onAdd(selectedMood, note)
                note = ""
            },
            enabled = !isSaving,
        )

        WeeklySummaryCard(summary = summary)

        EntriesList(
            entries = entries,
            modifier = Modifier.weight(1f, fill = true),
        )
    }
}
```

Finally, drop in the helper composables that render the quick-add form, mood picker, summary card,
entry list, and loading indicator.

```kotlin
// App.kt – helpers

@Composable
private fun QuickAddBlock(
    note: String,
    onNoteChange: (String) -> Unit,
    selectedMood: Int,
    onMoodChange: (Int) -> Unit,
    onAdd: () -> Unit,
    enabled: Boolean,
) {
    Card {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Text(
                text = "Add how you feel right now",
                style = MaterialTheme.typography.titleMedium,
            )

            OutlinedTextField(
                value = note,
                onValueChange = onNoteChange,
                modifier = Modifier.fillMaxWidth(),
                placeholder = { Text("Optional note") },
                maxLines = 3,
                keyboardOptions = KeyboardOptions.Default.copy(imeAction = ImeAction.Done),
                keyboardActions = KeyboardActions(onDone = { onAdd() }),
            )

            MoodPicker(
                selectedMood = selectedMood,
                onMoodChange = onMoodChange,
            )

            Button(
                onClick = onAdd,
                enabled = enabled,
                modifier = Modifier.align(Alignment.End),
            ) {
                Text("Add entry")
            }
        }
    }
}

@Composable
private fun MoodPicker(
    selectedMood: Int,
    onMoodChange: (Int) -> Unit,
) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(text = "Mood score", style = MaterialTheme.typography.labelLarge)
        Row(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            modifier = Modifier.fillMaxWidth(),
        ) {
            (1..5).forEach { score ->
                val selected = score == selectedMood
                val buttonModifier = Modifier.weight(1f)
                if (selected) {
                    FilledTonalButton(
                        onClick = { onMoodChange(score) },
                        modifier = buttonModifier,
                    ) {
                        Text(
                            text = score.toString(),
                            style = MaterialTheme.typography.titleMedium,
                        )
                    }
                } else {
                    OutlinedButton(
                        onClick = { onMoodChange(score) },
                        modifier = buttonModifier,
                    ) {
                        Text(
                            text = score.toString(),
                            style = MaterialTheme.typography.bodyMedium,
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun WeeklySummaryCard(summary: MoodTrackerViewModel.WeeklySummary) {
    Card(
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.secondaryContainer
        ),
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text("This week so far", style = MaterialTheme.typography.titleMedium)
            if (summary.entryCount == 0) {
                Text(
                    text = "No entries yet. Add how you feel to start tracking your week.",
                    style = MaterialTheme.typography.bodyMedium,
                )
            } else {
                val formattedAverage = summary.averageScore?.let { avg ->
                    val scaled = (avg * 10.0).roundToInt() / 10.0
                    scaled.toString()
                }
                Text("Entries: ${summary.entryCount}", style = MaterialTheme.typography.bodyMedium)
                Text(
                    text = "Average mood: ${formattedAverage ?: "-"}",
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
        }
    }
}

@Composable
private fun EntriesList(
    entries: List<MoodEntryWithTags>,
    modifier: Modifier = Modifier,
) {
    if (entries.isEmpty()) {
        Box(
            modifier = modifier.fillMaxWidth(),
            contentAlignment = Alignment.Center,
        ) {
            Text(
                text = "No recent entries yet.",
                style = MaterialTheme.typography.bodyMedium,
                textAlign = TextAlign.Center,
            )
        }
        return
    }

    LazyColumn(
        modifier = modifier.fillMaxWidth(),
        contentPadding = PaddingValues(vertical = 8.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        items(entries) { entry ->
            EntryRow(entry = entry)
        }
    }
}

@Composable
private fun EntryRow(entry: MoodEntryWithTags) {
    Card {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(
                text = entry.entryTime.toString(),
                style = MaterialTheme.typography.labelMedium,
            )
            Text(
                text = "Mood score: ${entry.moodScore}",
                style = MaterialTheme.typography.bodyMedium,
            )
            entry.note?.takeIf { it.isNotBlank() }?.let {
                HorizontalDivider()
                Text(
                    text = it,
                    style = MaterialTheme.typography.bodyLarge,
                )
            }
            if (entry.tags.isNotEmpty()) {
                HorizontalDivider()
                Text(
                    text = entry.tags.joinToString(separator = ", ") { tag -> tag.name },
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.primary,
                )
            }
        }
    }
}

@Composable
private fun LoadingState() {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center,
    ) {
        CircularProgressIndicator()
    }
}
```

Fill in the helper bodies directly from `App.kt`; they mirror the repository version exactly and
keep the tutorial snippets short.

## Step 3 – Point SQLiteNow at Real Files

SQLiteNow already ships a multiplatform helper named `resolveDatabasePath`
(`dev.goquick.sqlitenow.common.resolveDatabasePath`). It returns the appropriate
location on every target: Android uses the app's database directory, JVM/desktop
falls back to a user-specific folder, iOS stores inside Documents, and special
names such as `":memory:"` are passed through untouched. Because this helper
already covers every platform we no longer need a project-specific expect/actual
pair.

Only Android requires a small bootstrap call so the helper can read an
application context. Other platforms work out of the box—no extra code needed on
desktop, iOS, JS, or wasm.

```kotlin
// MainActivity.kt

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)

        setupAndroidAppContext(applicationContext)
        setContent { App() }
    }
}
```

`MoodDatabaseFactory` now relies on the library helper as well:

```kotlin
// MoodDatabaseFactory.kt

val appName = "MoodTracker"
val resolvedName = if (dbName.startsWith(":")) dbName else resolveDatabasePath(
    dbName = dbName,
    appName = appName
)
val database = MoodTrackerDatabase(
    dbName = resolvedName,
    migration = VersionBasedDatabaseMigrations(),
    debug = debug,
    // ... adapters ...
)
```

Nothing else from [Part 1]({{ site.baseurl }}/tutorials/part-1-bootstrap/) or
[Part 2]({{ site.baseurl }}/tutorials/part-2-tags-and-filters/) needs to change because the helper
already handles every target.

## Step 4 – Regenerate and Test

Run the usual verification commands. The first two ensure generated sources are up to date; the
connected test confirms Android instrumentation still passes.

```bash
./gradlew :composeApp:generateMoodTrackerDatabase
./gradlew :composeApp:compileDebugAndroidTestKotlin
./gradlew :composeApp:connectedDebugAndroidTest
```

## Where We Stand

- `MoodTrackerViewModel` keeps query flows hot with `stateIn`, computes a weekly summary, and
  exposes an `isSaving` flag for the UI.
- The Compose screen renders a quick-add form, weekly digest, and reactive list whose rows list
  tags pulled straight from the dynamic-field collection—no manual grouping required.
- The Android build uses SQLiteNow's shared `resolveDatabasePath` helper so the file lives under
  app-private storage; desktop and iOS share the same helper automatically.
- Weekly averages now round to one decimal in a multiplatform-friendly way.

That wraps up the Mood Tracker series. You now have a reactive KMP UI backed by SQLiteNow: typed
queries, flows, and a compose surface that stays in sync across Android, iOS, desktop, and beyond.
Happy shipping!
