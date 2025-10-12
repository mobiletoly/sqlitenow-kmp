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

                val activitiesByDocId = packageResult.activities.associateBy { it.docId }
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
    fun selectAllWithPackagesByIdReturnsExpectedBundle() = runBlocking {
        val expectedBundle = fixture.bundleExpectations.first()

        val bundleRow = database.activityBundle
            .selectAllWithPackagesById(
                ActivityBundleQuery.SelectAllWithPackagesById.Params(
                    docId = expectedBundle.docId,
                ),
            )
            .asOneOrNull()
            ?: error("Bundle ${expectedBundle.docId} not found")

        assertEquals(
            "Bundle docId mismatch",
            expectedBundle.docId,
            bundleRow.bndl.main.docId,
        )
        assertEquals(
            "Bundle title mismatch",
            expectedBundle.title,
            bundleRow.bndl.main.title,
        )
        val expectedBundleCategoryTitles = expectedBundle.packages.map { it.categoryTitle }.toSet()
        assertTrue(
            "Bundle category title should align with one of the package categories",
            expectedBundleCategoryTitles.contains(bundleRow.bndl.category.title),
        )

        val packageRow = bundleRow.pkgs.firstOrNull()
            ?: error("Expected at least one package for bundle ${expectedBundle.docId}")

        val packageExpectationsByDocId = expectedBundle.packages.associateBy { it.docId }
        val expectedPackage = packageExpectationsByDocId[packageRow.main.docId]
            ?: error("Package ${packageRow.main.docId} not seeded for bundle ${expectedBundle.docId}")

        assertEquals(
            "Package title mismatch",
            expectedPackage.title,
            packageRow.main.title,
        )
        assertEquals(
            "Package category mismatch",
            expectedPackage.categoryTitle,
            packageRow.category.title,
        )
        assertEquals(
            "Activity summary count mismatch",
            expectedPackage.activities.size,
            packageRow.activitySummary.count,
        )
        val expectedActivityTitles = expectedPackage.activities.map { it.title }.toSet()
        assertEquals(
            "Activity summary titles mismatch",
            expectedActivityTitles,
            packageRow.activitySummary.titles,
        )
        assertTrue(
            "All activities should share the same group doc id",
            packageRow.activitySummary.areAllGroupDocIdsSame,
        )
    }

    @Test
    fun selectAllDetailedByDateReturnsProgramItemsAndSchedules() = runBlocking {
        val logs = database.dailyLog.selectAllDetailedByDate(
            DailyLogQuery.SelectAllDetailedByDate.Params(date = fixture.dailyLogDate)
        ).asList()

        assertEquals("Unexpected daily log count", fixture.dailyLogExpectations.size, logs.size)

        val logsByDocId = logs.associateBy { it.docId }
        fixture.dailyLogExpectations.forEach { expectedLog ->
            val log = logsByDocId[expectedLog.docId] ?: error("Missing daily log ${expectedLog.docId}")

            assertEquals("Activity doc mismatch", expectedLog.activityDocId, log.activity.docId)
            assertEquals("Program item doc mismatch", expectedLog.programItemDocId, log.activity.firstProgramItem.docId)
            assertEquals(expectedLog.numericValue0, log.numericValues[0])
            assertEquals(expectedLog.notes, log.notes)

            val row = log.activity
            assertEquals("Schedule startAt mismatch", expectedLog.scheduleStartAt, row.schedule.startAt)
            assertEquals("Schedule repeat mismatch", ActivityScheduleRepeat.WEEK_DAYS, row.schedule.repeat)
            assertTrue(
                "Schedule points mismatch",
                row.schedule.timePoints.containsAll(expectedLog.scheduleTimePoints) &&
                    expectedLog.scheduleTimePoints.containsAll(expectedLog.scheduleTimePoints)
            )
        }
    }

    @Test
    fun selectAllForDateRangeAndActivityDocIdsFiltersLogs() = runBlocking {
        val start = fixture.dailyLogDate
        val end = fixture.dailyLogDate
        val includedActivities = fixture.dailyLogExpectations.take(2).map { it.activityDocId }

        val params = DailyLogQuery.SelectAllForDateRangeAndActivityDocIds.Params(
            start = start,
            end = end,
            activityDocIds = includedActivities,
        )

        val results = database.dailyLog
            .selectAllForDateRangeAndActivityDocIds(params)
            .asList()

        val expectedLogs = fixture.dailyLogExpectations.filter { it.activityDocId in includedActivities }
        assertEquals(expectedLogs.size, results.size)

        val resultDocIds = results.map { it.docId }.toSet()
        assertEquals(expectedLogs.map { it.docId }.toSet(), resultDocIds)

        val emptyResults = database.dailyLog
            .selectAllForDateRangeAndActivityDocIds(
                DailyLogQuery.SelectAllForDateRangeAndActivityDocIds.Params(
                    start = start,
                    end = end,
                    activityDocIds = emptyList(),
                ),
            )
            .asList()

        assertTrue(emptyResults.isEmpty())
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
            )

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
                )

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
                    )

                    val activityExpectations = mutableListOf<ActivityExpectation>()

                    for (activityIndex in 1..activitiesPerPackage) {
                        val activityDocId = "act-$bundleIndex-$packageIndex-$activityIndex"
                        val activityTitle = "Activity $bundleIndex-$packageIndex-$activityIndex"
                        val activityCategory = createCategory(
                            docId = "act-cat-$bundleIndex-$packageIndex-$activityIndex",
                            title = "Activity Category $bundleIndex-$packageIndex-$activityIndex",
                            iconValue = "icon://activity/$bundleIndex/$packageIndex/$activityIndex",
                        )

                        val scheduleStartAt = LocalDate(2024, 3, activityIndex + packageIndex)
                        val scheduleTimePoints = listOf(AlarmHourMinute(alarm = true, hour = 6 + activityIndex, minute = 15))
                        database.activity.addReturningId.one(
                            ActivityQuery.AddReturningId.Params(
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
                                schedMandatoryToSetup = true,
                                schedRepeat = ActivityScheduleRepeat.WEEK_DAYS,
                                schedAllowedRepeatModes = setOf(ActivityScheduleRepeat.WEEK_DAYS),
                                schedMon = true,
                                schedTue = true,
                                schedWed = true,
                                schedThu = true,
                                schedFri = true,
                                schedSat = false,
                                schedSun = false,
                                schedWeek1 = true,
                                schedWeek2 = true,
                                schedWeek3 = true,
                                schedWeek4 = true,
                                schedDay0 = 1,
                                schedDay1 = 2,
                                schedDay2 = 3,
                                schedDay3 = 4,
                                schedDay4 = 5,
                                schedStartAt = scheduleStartAt,
                                schedStartAtEval = null,
                                schedReadRefStartAt = null,
                                schedWriteRefStartAt = null,
                                schedStartAtLabel = null,
                                schedRepeatAfterDays = 0,
                                schedReadRefRepeatAfterDays = null,
                                schedWriteRefRepeatAfterDays = null,
                                schedRepeatAfterDaysLabel = null,
                                schedRepeatAfterDaysMin = null,
                                schedRepeatAfterDaysMax = null,
                                schedAllowEditDaysDuration = true,
                                schedDaysDuration = 1,
                                schedReadRefDaysDuration = null,
                                schedWriteRefDaysDuration = null,
                                schedDaysDurationLabel = null,
                                schedDaysDurationMin = null,
                                schedDaysDurationMax = null,
                                schedTimePoints = scheduleTimePoints,
                                schedTimeRange = ActivityScheduleTimeRange.MORNING,
                            )
                        )

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
                        )

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
                                numericValue00 = 10.0 * activityIndex,
                                numericValue01 = null,
                                numericValue02 = null,
                                numericValue03 = null,
                                numericValue04 = null,
                                numericValue05 = null,
                                numericValue06 = null,
                                numericValue07 = null,
                                numericValue08 = null,
                                numericValue09 = null,
                                stringValue00 = "s-$activityDocId",
                                stringValue01 = null,
                                stringValue02 = null,
                                stringValue03 = null,
                                stringValue04 = null,
                                stringValue05 = null,
                                stringValue06 = null,
                                stringValue07 = null,
                                stringValue08 = null,
                                stringValue09 = null,
                                notes = "Notes for $activityDocId",
                            )
                        )

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
        )
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
