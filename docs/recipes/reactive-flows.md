---
layout: doc
title: Reactive Flows
permalink: /recipes/reactive-flows/
parent: Recipes
---

# Reactive Flows

Use the generated flow-based APIs to keep your UI synchronized with database changes.

SQLiteNow tracks table invalidations and notifies observers automatically. Combine those signals
with Kotlin Flows to propagate updates through your UI layers.

## Basic reactive query

```kotlin
@Composable
fun PersonList() {
    var persons by remember { mutableStateOf<List<PersonEntity>>(emptyList()) }
    var isLoading by remember { mutableStateOf(true) }

    LaunchedEffect(Unit) {
        database.person
            .selectAll(PersonQuery.SelectAll.Params(limit = -1, offset = 0))
            .asFlow()               // Reactive flow
            .flowOn(Dispatchers.IO) // DB work on IO thread
            .collect { personList ->
                persons = personList
                isLoading = false
            }
    }

    LazyColumn {
        items(persons) { person ->
            PersonCard(person = person)
        }
    }
}
```

## With data transformation

Perform transformations off the main thread before updating the UI:

```kotlin
LaunchedEffect(Unit) {
    database.person
        .selectAll(PersonQuery.SelectAll.Params(limit = -1, offset = 0))
        .asFlow()
        .map { personList ->
            personList.map { person ->
                person.copy(
                    displayName = "${person.firstName} ${person.lastName}".trim(),
                    isRecent = person.createdAt > recentThreshold
                )
            }
        }
        .flowOn(Dispatchers.IO)
        .collect { transformedPersons ->
            persons = transformedPersons
        }
}
```

## Related data updates

Flows can be collected independently for related tables:

```kotlin
@Composable
fun PersonWithComments(personId: ByteArray) {
    var person by remember { mutableStateOf<PersonEntity?>(null) }
    var comments by remember { mutableStateOf<List<CommentEntity>>(emptyList()) }

    LaunchedEffect(personId.toList()) {
        launch {
            database.person
                .selectById(PersonQuery.SelectById.Params(id = personId))
                .asFlow()
                .flowOn(Dispatchers.IO)
                .collect { personResult ->
                    person = personResult.firstOrNull()
                }
        }

        launch {
            database.comment
                .selectByPersonId(CommentQuery.SelectByPersonId.Params(personId = personId))
                .asFlow()
                .flowOn(Dispatchers.IO)
                .collect { commentList ->
                    comments = commentList
                }
        }
    }
}
```

## Performance tips

- Use `LaunchedEffect` in Compose to automatically cancel collectors when the composable leaves composition.
- Apply `flowOn(Dispatchers.IO)` so database work runs off the main thread.
- Debounce high-frequency updates with `.debounce(100)` if needed.
- Scope queries as tightly as possible instead of broad `SELECT *` operations.

**Tip:** When using SQLiteNow's sync features, flows refresh during sync operations automatically.
See [Reactive Sync Updates]({{ site.baseurl }}/sync/reactive-updates/) for sync-specific patterns.
