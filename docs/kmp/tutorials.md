---
layout: page
title: Tutorials
permalink: /kmp/tutorials/
---

# Tutorials

Hands-on guides that walk through complete SQLiteNow workflows. The Mood Tracker series is a
four-part Kotlin Multiplatform project that demonstrates SQL-first development from the very first
schema file through a reactive Compose UI.

[Browse the full sample project on GitHub](https://github.com/mobiletoly/moodtracker-sample-kmp){:target="_blank" rel="noopener"}.

## Mood Tracker Series

1. [Part 0 – Series Overview]({{ site.baseurl }}/kmp/tutorials/part-0-intro/)
   - Why SQLiteNow is SQL-first, what the generator/runtime provide, and how the tutorial project is
     structured.
2. [Part 1 – Bootstrapping SQLiteNow in MoodTracker]({{ site.baseurl }}/kmp/tutorials/part-1-bootstrap/)
   - Wire the Gradle plugin, configure dependencies, scaffold SQL directories, and generate the first
     Kotlin sources.
3. [Part 2 – Tags, Filters, and Richer Types]({{ site.baseurl }}/kmp/tutorials/part-2-tags-and-filters/)
   - Extend the schema with tag tables, add column-level annotations for UUID/date types, and create
     repositories that operate on generated adapters.
4. [Part 3 – Reactive Mood Dashboard]({{ site.baseurl }}/kmp/tutorials/part-3-reactive-ui/)
   - Turn query results into `StateFlow`, surface them in Compose, and ship a cross-platform mood
     dashboard that reacts to database updates.

Looking for more? Check the [KMP SQL Reference]({{ site.baseurl }}/kmp/sql-reference/) hub for focused
reference guides or the [KMP Recipes]({{ site.baseurl }}/kmp/recipes/) collection for targeted how-tos.
