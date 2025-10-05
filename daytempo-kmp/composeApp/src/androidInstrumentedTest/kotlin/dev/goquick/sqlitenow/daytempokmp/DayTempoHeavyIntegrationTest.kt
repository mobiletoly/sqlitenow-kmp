@file:OptIn(ExperimentalUuidApi::class)

package dev.goquick.sqlitenow.daytempokmp

import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pluralfusion.daytempo.domain.model.ActivityBundlePurchaseMode
import com.pluralfusion.daytempo.domain.model.ActivityBundleWithActivitiesDoc
import com.pluralfusion.daytempo.domain.model.ActivityIconDoc
import com.pluralfusion.daytempo.domain.model.ActivityIconDoc.Format
import com.pluralfusion.daytempo.domain.model.ActivityProgramType
import com.pluralfusion.daytempo.domain.model.ActivityReportingType
import com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat
import com.pluralfusion.daytempo.domain.model.ActivityScheduleTimeRange
import com.pluralfusion.daytempo.domain.model.AlarmHourMinute
import com.pluralfusion.daytempo.domain.model.GoalDirection
import com.pluralfusion.daytempo.domain.model.ProgramItemInputEntry
import com.pluralfusion.daytempo.domain.model.ProgramItemLockItemDisplay
import com.pluralfusion.daytempo.domain.model.ProgramItemPresentation
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityCategoryQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityPackageQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityQuery
import com.pluralfusion.daytempo.domain.model.ActivityScheduleDoc
import dev.goquick.sqlitenow.daytempokmp.db.ActivityScheduleQuery
import dev.goquick.sqlitenow.daytempokmp.db.DailyLogQuery
import dev.goquick.sqlitenow.daytempokmp.db.DayTempoDatabase
import dev.goquick.sqlitenow.daytempokmp.db.ProgramItemQuery
import dev.goquick.sqlitenow.daytempokmp.db.ProviderQuery
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class DayTempoHeavyIntegrationTest {

    private lateinit var database: DayTempoDatabase
    private lateinit var fixture: SeedFixture

    @Before
    fun setUp() = runBlocking {
        database = DayTempoTestDatabaseHelper.createDatabase()
        database.open()
        fixture = DayTempoSeedHelper(database).seedComplexData(
            bundleCount = 2,
            packagesPerBundle = 2,
            activitiesPerPackage = 2,
        )
    }

    @After
    fun tearDown() = runBlocking {
        database.close()
    }

    @Test
    fun selectAllWithEnabledActivitiesHandlesMultipleBundles() = runBlocking {
        val results = database.activityBundle.selectAllWithEnabledActivities.asList()
        assertEquals("Unexpected bundle count", fixture.bundleExpectations.size, results.size)

        val resultsByDocId = results.associateBy { it.main.main.docId }
        fixture.bundleExpectations.forEach { expectedBundle ->
            val bundleResult = resultsByDocId[expectedBundle.docId]
                ?: error("Missing bundle ${expectedBundle.docId} in results")

            val primaryPackage = expectedBundle.packages.firstOrNull()
                ?: error("Expected bundle ${expectedBundle.docId} to contain packages")

            assertEquals(
                "Bundle title mismatch for ${expectedBundle.docId}",
                expectedBundle.title,
                bundleResult.main.main.title,
            )
            val expectedPackageCategories = expectedBundle.packages.map { it.categoryTitle }.toSet()
            assertTrue(
                "Bundle category should align with one of the package categories for ${expectedBundle.docId}",
                expectedPackageCategories.contains(bundleResult.category.title),
            )

            assertEquals(
                "Unexpected package count for bundle ${expectedBundle.docId}",
                expectedBundle.packages.size,
                bundleResult.activityPackages.size,
            )

            val packagesByDocId = bundleResult.activityPackages.associateBy { it.main.docId }
            expectedBundle.packages.forEach { expectedPackage ->
                val packageResult = packagesByDocId[expectedPackage.docId]
                    ?: error("Missing package ${expectedPackage.docId}")

                assertEquals(
                    "Package title mismatch for ${expectedPackage.docId}",
                    expectedPackage.title,
                    packageResult.main.title,
                )
                assertEquals(
                    "Package category mismatch for ${expectedPackage.docId}",
                    expectedPackage.categoryTitle,
                    packageResult.category.title,
                )

                assertEquals(
                    "Unexpected activity count for package ${expectedPackage.docId}",
                    expectedPackage.activities.size,
                    packageResult.activities.size,
                )

                val activitiesByDocId = packageResult.activities.associateBy { it.main.docId }
                expectedPackage.activities.forEach { expectedActivity ->
                    val activityResult = activitiesByDocId[expectedActivity.docId]
                        ?: error("Missing activity ${expectedActivity.docId}")

                    assertEquals(
                        "Activity category mismatch for ${expectedActivity.docId}",
                        expectedActivity.categoryTitle,
                        activityResult.category.title,
                    )
                    assertScheduleMatches(expectedActivity, activityResult.schedule)
                }
            }
        }
    }

    @Test
    fun selectAllDetailedByDateReturnsProgramItemsAndSchedules() = runBlocking {
        val logs = database.dailyLog.selectAllDetailedByDate(
            DailyLogQuery.SelectAllDetailedByDate.Params(date = fixture.dailyLogDate)
        ).asList()

        assertEquals("Unexpected daily log count", fixture.dailyLogExpectations.size, logs.size)

        val logsByDocId = logs.associateBy { it.main.docId }
        fixture.dailyLogExpectations.forEach { expectedLog ->
            val log = logsByDocId[expectedLog.docId] ?: error("Missing daily log ${expectedLog.docId}")

            assertEquals("Activity doc mismatch", expectedLog.activityDocId, log.activity.main.docId)
            assertEquals("Program item doc mismatch", expectedLog.programItemDocId, log.programItem.docId)
            assertEquals(expectedLog.numericValue0, log.main.numericValue00)
            assertEquals(expectedLog.notes, log.main.notes)

            val schedule = log.activity.schedule
            assertEquals("Schedule startAt mismatch", expectedLog.scheduleStartAt, schedule.startAt)
            assertEquals("Schedule repeat mismatch", ActivityScheduleRepeat.WEEK_DAYS, schedule.repeat)
            assertTrue(
                "Schedule points mismatch",
                schedule.timePoints.containsAll(expectedLog.scheduleTimePoints) &&
                    expectedLog.scheduleTimePoints.containsAll(schedule.timePoints)
            )
        }
    }

    @Test
    fun selectAllEnabledWithTimePointsByScheduleRepeatReturnsMatchingActivities() = runBlocking {
        val params = ActivityQuery.SelectAllEnabledWithTimePointsByScheduleRepeat.Params(
            scheduleRepeat = ActivityScheduleRepeat.WEEK_DAYS,
        )

        val results = database.activity
            .selectAllEnabledWithTimePointsByScheduleRepeat(params)
            .asList()

        val expectedActivities = fixture.bundleExpectations
            .flatMap { it.packages }
            .flatMap { it.activities }

        assertEquals("Unexpected activity count", expectedActivities.size, results.size)

        val resultsByDocId = results.associateBy { it.main.docId }
        expectedActivities.forEach { expected ->
            val row = resultsByDocId[expected.docId] ?: error("Missing activity ${expected.docId}")

            assertTrue("Activity should be enabled", row.main.enabled)
            assertEquals(
                "Schedule repeat mismatch for ${expected.docId}",
                ActivityScheduleRepeat.WEEK_DAYS,
                row.schedule.repeat,
            )
            assertEquals(
                "Schedule startAt mismatch for ${expected.docId}",
                expected.scheduleStartAt,
                row.schedule.startAt,
            )
            assertEquals(
                "Time points mismatch for ${expected.docId}",
                expected.scheduleTimePoints,
                row.schedule.timePoints,
            )
        }

        val noneResults = database.activity
            .selectAllEnabledWithTimePointsByScheduleRepeat(
                ActivityQuery.SelectAllEnabledWithTimePointsByScheduleRepeat.Params(
                    scheduleRepeat = ActivityScheduleRepeat.NONE,
                ),
            )
            .asList()

        assertTrue("Expected empty results for NONE repeat", noneResults.isEmpty())
    }

    private fun assertScheduleMatches(expected: ActivityExpectation, actual: ActivityScheduleDoc) {
        assertEquals("Schedule startAt mismatch", expected.scheduleStartAt, actual.startAt)
        assertEquals("Schedule repeat mismatch", expected.scheduleRepeat, actual.repeat)
        assertTrue(
            "Schedule time points mismatch",
            actual.timePoints.containsAll(expected.scheduleTimePoints) &&
                expected.scheduleTimePoints.containsAll(actual.timePoints)
        )
    }
}

private class DayTempoSeedHelper(private val database: DayTempoDatabase) {

    suspend fun seedComplexData(
        bundleCount: Int,
        packagesPerBundle: Int,
        activitiesPerPackage: Int,
    ): SeedFixture {
        val bundleExpectations = mutableListOf<BundleExpectation>()
        val dailyLogExpectations = mutableListOf<DailyLogExpectation>()
        val targetDate = LocalDate(2024, 2, 1)

        database.transaction {
            database.provider.add(
                ProviderQuery.Add.Params(
                    docId = "provider-main",
                    title = "Provider Main",
                )
            ).execute()

            for (bundleIndex in 1..bundleCount) {
                val bundleDocId = "bundle-$bundleIndex"
                val bundleTitle = "Bundle $bundleIndex"
                val bundleCategory = createCategory(
                    docId = "bundle-cat-$bundleIndex",
                    title = "Bundle Category $bundleIndex",
                    iconValue = "icon://bundle/$bundleIndex",
                )

                database.activityBundle.add(
                    ActivityBundleQuery.Add.Params(
                        docId = bundleDocId,
                        providerDocId = "provider-main",
                        version = bundleIndex,
                        title = bundleTitle,
                        descr = "Bundle description $bundleIndex",
                        userDefined = false,
                        purchaseMode = ActivityBundlePurchaseMode.FULLY_FREE,
                        unlockCode = null,
                        purchased = true,
                        installedAt = LocalDateTime(2024, 1, 1 + bundleIndex, 0, 0),
                        resourcesJson = "{}",
                        icon = bundleCategory.icon,
                        promoImage = bundleCategory.icon,
                        promoScr1 = null,
                        promoScr2 = null,
                        promoScr3 = null,
                    )
                ).execute()

                val packageExpectations = mutableListOf<PackageExpectation>()

                for (packageIndex in 1..packagesPerBundle) {
                    val packageDocId = "pkg-$bundleIndex-$packageIndex"
                    val packageTitle = "Package $bundleIndex-$packageIndex"
                    val packageCategory = createCategory(
                        docId = "pkg-cat-$bundleIndex-$packageIndex",
                        title = "Package Category $bundleIndex-$packageIndex",
                        iconValue = "icon://package/$bundleIndex/$packageIndex",
                    )

                    database.activityPackage.add(
                        ActivityPackageQuery.Add.Params(
                            docId = packageDocId,
                            activityBundleDocId = bundleDocId,
                            title = packageTitle,
                            descr = "Package description $bundleIndex-$packageIndex",
                            preStartText = null,
                            userDefined = false,
                            categoryDocId = packageCategory.docId,
                            icon = packageCategory.icon,
                        )
                    ).execute()

                    val activityExpectations = mutableListOf<ActivityExpectation>()

                    for (activityIndex in 1..activitiesPerPackage) {
                        val activityDocId = "act-$bundleIndex-$packageIndex-$activityIndex"
                        val activityTitle = "Activity $bundleIndex-$packageIndex-$activityIndex"
                        val activityCategory = createCategory(
                            docId = "act-cat-$bundleIndex-$packageIndex-$activityIndex",
                            title = "Activity Category $bundleIndex-$packageIndex-$activityIndex",
                            iconValue = "icon://activity/$bundleIndex/$packageIndex/$activityIndex",
                        )

                        database.activity.add(
                            ActivityQuery.Add.Params(
                                docId = activityDocId,
                                dependsOnDocId = null,
                                groupDocId = "group-$bundleIndex-$packageIndex",
                                activityBundleDocId = bundleDocId,
                                activityPackageDocId = packageDocId,
                                deleted = false,
                                enabled = true,
                                userDefined = false,
                                programType = ActivityProgramType.SIMPLE,
                                daysConfirmRequired = false,
                                deleteWhenExpired = false,
                                orderInd = activityIndex,
                                installedAt = LocalDateTime(2024, 1, 10 + activityIndex, 6, 0),
                                title = activityTitle,
                                descr = "Activity description $activityDocId",
                                categoryDocId = activityCategory.docId,
                                icon = activityCategory.icon,
                                monthlyGlanceView = false,
                                requiredUnlockCode = null,
                                priority = 1,
                                unlockedDays = null,
                                reporting = ActivityReportingType.DEFAULT,
                            )
                        ).execute()

                        val activityUuid = fetchActivityUuid(activityDocId)
                        val scheduleStartAt = LocalDate(2024, 3, activityIndex + packageIndex)
                        val scheduleTimePoints = listOf(AlarmHourMinute(alarm = true, hour = 6 + activityIndex, minute = 15))

                        database.activitySchedule.add(
                            ActivityScheduleQuery.Add.Params(
                                activityId = activityUuid,
                                mandatoryToSetup = true,
                                repeat = ActivityScheduleRepeat.WEEK_DAYS,
                                allowedRepeatModes = setOf(ActivityScheduleRepeat.WEEK_DAYS),
                                mon = true,
                                tue = true,
                                wed = true,
                                thu = true,
                                fri = true,
                                sat = false,
                                sun = false,
                                week1 = true,
                                week2 = true,
                                week3 = true,
                                week4 = true,
                                day0 = 1,
                                day1 = 2,
                                day2 = 3,
                                day3 = 4,
                                day4 = 5,
                                startAt = scheduleStartAt,
                                startAtEval = null,
                                readRefStartAt = null,
                                writeRefStartAt = null,
                                startAtLabel = "Start",
                                repeatAfterDays = 1,
                                readRefRepeatAfterDays = null,
                                writeRefRepeatAfterDays = null,
                                repeatAfterDaysLabel = null,
                                repeatAfterDaysMin = null,
                                repeatAfterDaysMax = null,
                                allowEditDaysDuration = true,
                                daysDuration = 1,
                                readRefDaysDuration = null,
                                writeRefDaysDuration = null,
                                daysDurationLabel = null,
                                daysDurationMin = null,
                                daysDurationMax = null,
                                timePoints = scheduleTimePoints,
                                timeRange = ActivityScheduleTimeRange.MORNING,
                            )
                        ).execute()

                        val programItemDocId = "pi-$bundleIndex-$packageIndex-$activityIndex"
                        val programItemTitle = "Program Item $bundleIndex-$packageIndex-$activityIndex"

                        database.programItem.add(
                            ProgramItemQuery.Add.Params(
                                docId = programItemDocId,
                                activityDocId = activityDocId,
                                itemId = "item-$bundleIndex-$packageIndex-$activityIndex",
                                title = programItemTitle,
                                descr = "Program item description",
                                goalValue = 100 + activityIndex,
                                goalDailyInitial = 10,
                                goalDirection = GoalDirection.UP,
                                goalInvert = false,
                                goalAtLeast = true,
                                goalSingle = false,
                                goalHideEditor = false,
                                weekIndex = bundleIndex,
                                dayIndex = activityIndex,
                                preStartText = null,
                                postCompleteText = null,
                                presentation = ProgramItemPresentation.DEFAULT,
                                seqItemsJson = "{}",
                                requiredUnlockCode = null,
                                hasUnlockedSeqItems = false,
                                lockItemDisplay = ProgramItemLockItemDisplay.DEFAULT,
                                inputEntries = listOf(
                                    ProgramItemInputEntry(
                                        format = ProgramItemInputEntry.Format.TEXT,
                                        label = "Entry",
                                        descr = "Entry description",
                                        mandatory = true,
                                        writeRefValue = null,
                                        selectorUnspecified = null,
                                        selectorLabels = emptyList(),
                                        min = null,
                                        max = null,
                                    )
                                ),
                            )
                        ).execute()

                        database.dailyLog.add(
                            DailyLogQuery.Add.Params(
                                docId = "log-$bundleIndex-$packageIndex-$activityIndex",
                                parentDailyLogDocId = null,
                                activityDocId = activityDocId,
                                programItemDocId = programItemDocId,
                                groupDocId = null,
                                date = targetDate,
                                appliedWeekIndex = bundleIndex,
                                appliedDayIndex = activityIndex,
                                counter = activityIndex,
                                addedManually = activityIndex % 2 == 0,
                                confirmed = true,
                                numericValue0 = 10.0 * activityIndex,
                                numericValue1 = null,
                                numericValue2 = null,
                                numericValue3 = null,
                                numericValue4 = null,
                                numericValue5 = null,
                                numericValue6 = null,
                                numericValue7 = null,
                                numericValue8 = null,
                                numericValue9 = null,
                                stringValue0 = "s-$activityDocId",
                                stringValue1 = null,
                                stringValue2 = null,
                                stringValue3 = null,
                                stringValue4 = null,
                                stringValue5 = null,
                                stringValue6 = null,
                                stringValue7 = null,
                                stringValue8 = null,
                                stringValue9 = null,
                                notes = "Notes for $activityDocId",
                            )
                        ).execute()

                        activityExpectations += ActivityExpectation(
                            docId = activityDocId,
                            title = activityTitle,
                            categoryTitle = activityCategory.title,
                            groupDocId = "group-$bundleIndex-$packageIndex",
                            scheduleStartAt = scheduleStartAt,
                            scheduleTimePoints = scheduleTimePoints,
                            scheduleRepeat = ActivityScheduleRepeat.WEEK_DAYS,
                            programItemDocId = programItemDocId,
                            programItemTitle = programItemTitle,
                        )

                        dailyLogExpectations += DailyLogExpectation(
                            docId = "log-$bundleIndex-$packageIndex-$activityIndex",
                            activityDocId = activityDocId,
                            programItemDocId = programItemDocId,
                            numericValue0 = 10.0 * activityIndex,
                            notes = "Notes for $activityDocId",
                            scheduleStartAt = scheduleStartAt,
                            scheduleTimePoints = scheduleTimePoints,
                        )
                    }

                    packageExpectations += PackageExpectation(
                        docId = packageDocId,
                        title = packageTitle,
                        categoryTitle = packageCategory.title,
                        activities = activityExpectations,
                    )
                }

                bundleExpectations += BundleExpectation(
                    docId = bundleDocId,
                    title = bundleTitle,
                    categoryTitle = bundleCategory.title,
                    packages = packageExpectations,
                )
            }
        }

        return SeedFixture(
            bundleExpectations = bundleExpectations,
            dailyLogExpectations = dailyLogExpectations,
            dailyLogDate = targetDate,
        )
    }

    private suspend fun createCategory(docId: String, title: String, iconValue: String): CategoryRecord {
        val icon = ActivityIconDoc(format = Format.IMAGE, value = iconValue, tint = null)
        database.activityCategory.add(
            ActivityCategoryQuery.Add.Params(
                docId = docId,
                title = title,
                icon = icon,
            )
        ).execute()
        return CategoryRecord(docId = docId, title = title, icon = icon)
    }

    private suspend fun fetchActivityUuid(docId: String): Uuid {
        database.connection().prepare("SELECT id FROM activity WHERE doc_id = ?").use { statement ->
            statement.bindText(1, docId)
            check(statement.step()) { "Activity with docId=$docId not found" }
            return Uuid.fromByteArray(statement.getBlob(0))
        }
    }

    private data class CategoryRecord(
        val docId: String,
        val title: String,
        val icon: ActivityIconDoc,
    )
}

private data class SeedFixture(
    val bundleExpectations: List<BundleExpectation>,
    val dailyLogExpectations: List<DailyLogExpectation>,
    val dailyLogDate: LocalDate,
)

private data class BundleExpectation(
    val docId: String,
    val title: String,
    val categoryTitle: String,
    val packages: List<PackageExpectation>,
)

private data class PackageExpectation(
    val docId: String,
    val title: String,
    val categoryTitle: String,
    val activities: List<ActivityExpectation>,
)

private data class ActivityExpectation(
    val docId: String,
    val title: String,
    val categoryTitle: String,
    val groupDocId: String,
    val scheduleStartAt: LocalDate,
    val scheduleTimePoints: List<AlarmHourMinute>,
    val scheduleRepeat: ActivityScheduleRepeat,
    val programItemDocId: String,
    val programItemTitle: String,
)

private data class DailyLogExpectation(
    val docId: String,
    val activityDocId: String,
    val programItemDocId: String,
    val numericValue0: Double?,
    val notes: String?,
    val scheduleStartAt: LocalDate,
    val scheduleTimePoints: List<AlarmHourMinute>,
)
