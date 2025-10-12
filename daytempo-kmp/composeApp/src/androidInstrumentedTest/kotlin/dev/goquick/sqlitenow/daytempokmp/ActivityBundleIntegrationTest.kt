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
import dev.goquick.sqlitenow.daytempokmp.db.DayTempoDatabase
import dev.goquick.sqlitenow.daytempokmp.db.ProviderQuery
import dev.goquick.sqlitenow.daytempokmp.db.ProgramItemQuery
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
class ActivityBundleIntegrationTest {

    private lateinit var database: DayTempoDatabase

    private data class TestCategory(
        val docId: String,
        val title: String,
        val iconValue: String,
    )

    @Before
    fun setUp() = runBlocking {
        database = DayTempoTestDatabaseHelper.createDatabase()
        database.open()
    }

    @After
    fun tearDown() = runBlocking {
        database.close()
    }

    @Test
    fun selectAllWithEnabledActivitiesReturnsNestedStructure() = runBlocking {
        seedActivityBundleHierarchy()

        database.connection().prepare("SELECT doc_id, program_type FROM activity").use { stmt ->
            while (stmt.step()) {
                println("activity row: doc_id=" + stmt.getText(0) + ", program_type=" + stmt.getText(1))
            }
        }

        val results = database.activityBundle.selectAllWithEnabledActivities.asList()

        assertEquals("Expected exactly one bundle", 1, results.size)
        val bundle: ActivityBundleWithActivitiesDoc = results.single()

        assertEquals("Morning Routine", bundle.main.main.title)
        assertEquals(ActivityBundlePurchaseMode.FULLY_FREE, bundle.main.main.purchaseMode)
        assertEquals("Provider One", bundle.provider.title)
        assertEquals("Flexibility", bundle.category.title)

        val packageDoc = bundle.activityPackages.single()
        assertEquals("Starter Package", packageDoc.main.title)
        assertEquals("Flexibility", packageDoc.category.title)

        val activityDoc = packageDoc.activities.single()
        assertEquals("Sunrise Stretch", activityDoc.title)
        assertTrue("Activity should be enabled", activityDoc.enabled)
        assertEquals(ActivityProgramType.SIMPLE, activityDoc.programType)
        assertEquals(ActivityReportingType.DEFAULT, activityDoc.reporting)
        assertEquals(ActivityScheduleRepeat.WEEK_DAYS, activityDoc.schedule.repeat)
        assertEquals(setOf(ActivityScheduleRepeat.WEEK_DAYS), activityDoc.schedule.allowedRepeatModes)
        assertEquals(LocalDate.parse("2024-01-01"), activityDoc.schedule.startAt)
        assertEquals(setOf(AlarmHourMinute(alarm = true, hour = 6, minute = 30)), activityDoc.schedule.timePoints)
        assertEquals(ActivityScheduleTimeRange.MORNING, activityDoc.schedule.timeRange)

        // ensure nested collections are populated and no stray top-level activity list leaks out
        assertEquals(1, bundle.activityPackages.size)
        assertEquals(1, packageDoc.activities.size)
    }

    @Test
    fun selectAllWithEnabledActivitiesPreservesDistinctPackageAndActivityCategories() = runBlocking {
        val packageCategory = TestCategory(
            docId = "category-package",
            title = "Flexibility Package",
            iconValue = "icon://category-package",
        )
        val activityCategory = TestCategory(
            docId = "category-activity",
            title = "Mobility Activity",
            iconValue = "icon://category-activity",
        )

        seedActivityBundleHierarchy(
            packageCategory = packageCategory,
            activityCategory = activityCategory,
        )

        val bundle = database.activityBundle.selectAllWithEnabledActivities.asList().single()
        val packageDoc = bundle.activityPackages.single()
        val activityDoc = packageDoc.activities.single()

        assertEquals(packageCategory.title, bundle.category.title)
        assertEquals(packageCategory.title, packageDoc.category.title)
        assertEquals(activityCategory.title, activityDoc.category.title)
        assertEquals(packageCategory.docId, packageDoc.main.categoryDocId)
        assertEquals(activityCategory.docId, activityDoc.category.docId)
    }

    private suspend fun seedActivityBundleHierarchy(
        packageCategory: TestCategory = TestCategory(
            docId = "category-1",
            title = "Flexibility",
            iconValue = "icon://category-flex",
        ),
        activityCategory: TestCategory = packageCategory,
    ) {
        val bundleInstalledAt = LocalDateTime.parse("2024-01-01T05:00")
        val scheduleStartAt = LocalDate.parse("2024-01-01")

        database.transaction {
            database.provider.add(
                ProviderQuery.Add.Params(
                    docId = "provider-1",
                    title = "Provider One"
                )
            )

            addCategory(packageCategory)
            if (activityCategory.docId != packageCategory.docId) {
                addCategory(activityCategory)
            }

            database.activityBundle.add(
                ActivityBundleQuery.Add.Params(
                    docId = "bundle-1",
                    providerDocId = "provider-1",
                    version = 1,
                    title = "Morning Routine",
                    descr = "Day start routine",
                    userDefined = false,
                    purchaseMode = ActivityBundlePurchaseMode.FULLY_FREE,
                    unlockCode = null,
                    purchased = true,
                    installedAt = bundleInstalledAt,
                    resourcesJson = "{}",
                    icon = ActivityIconDoc(format = Format.IMAGE, value = "icon://bundle", tint = null),
                    promoImage = ActivityIconDoc(format = Format.IMAGE, value = "icon://promo", tint = null),
                    promoScr1 = null,
                    promoScr2 = null,
                    promoScr3 = null,
                )
            )

            database.activityPackage.add(
                ActivityPackageQuery.Add.Params(
                    docId = "package-1",
                    activityBundleDocId = "bundle-1",
                    title = "Starter Package",
                    descr = "Light activities",
                    preStartText = null,
                    userDefined = false,
                    categoryDocId = packageCategory.docId,
                    icon = ActivityIconDoc(format = Format.IMAGE, value = "icon://pkg", tint = null)
                )
            )

            database.activity.addReturningId.one(
                ActivityQuery.AddReturningId.Params(
                    docId = "activity-1",
                    dependsOnDocId = null,
                    groupDocId = "group-1",
                    activityBundleDocId = "bundle-1",
                    activityPackageDocId = "package-1",
                    deleted = false,
                    enabled = true,
                    userDefined = false,
                    programType = ActivityProgramType.SIMPLE,
                    daysConfirmRequired = false,
                    deleteWhenExpired = false,
                    orderInd = 0,
                    installedAt = bundleInstalledAt,
                    title = "Sunrise Stretch",
                    descr = "Gentle stretching",
                    categoryDocId = activityCategory.docId,
                    icon = ActivityIconDoc(format = Format.IMAGE, value = "icon://activity", tint = null),
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
                    schedStartAtLabel = "Start",
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
                    schedTimePoints = listOf(AlarmHourMinute(alarm = true, hour = 6, minute = 30)),
                    schedTimeRange = ActivityScheduleTimeRange.MORNING
                )
            )
            database.connection().prepare("SELECT program_type FROM activity WHERE doc_id = ?").use { stmt ->
                stmt.bindText(1, "activity-1")
                require(stmt.step())
                val storedProgramType = stmt.getText(0)
                require(storedProgramType == ActivityProgramType.SIMPLE.value) {
                    "Unexpected program_type stored: $storedProgramType"
                }
            }

            val stmt = database.connection().prepare("SELECT id FROM activity WHERE doc_id = ?")
            val activityUuid = try {
                stmt.bindText(1, "activity-1")
                require(stmt.step()) { "Inserted activity not found" }
                Uuid.fromByteArray(stmt.getBlob(0))
            } finally {
                stmt.close()
            }

            database.programItem.add(
                ProgramItemQuery.Add.Params(
                    docId = "program-item-1",
                    activityDocId = "activity-1",
                    itemId = "item-1",
                    title = "Sunrise Stretch Program",
                    descr = "Program details",
                    goalValue = 1,
                    goalDailyInitial = 1,
                    goalDirection = GoalDirection.UP,
                    goalInvert = false,
                    goalAtLeast = true,
                    goalSingle = false,
                    goalHideEditor = false,
                    weekIndex = 0,
                    dayIndex = 0,
                    preStartText = null,
                    postCompleteText = null,
                    presentation = ProgramItemPresentation.DEFAULT,
                    seqItemsJson = "{}",
                    requiredUnlockCode = null,
                    hasUnlockedSeqItems = false,
                    lockItemDisplay = ProgramItemLockItemDisplay.DEFAULT,
                    inputEntries = emptyList<ProgramItemInputEntry>(),
                )
            )
        }
    }

    private suspend fun addCategory(category: TestCategory) {
        database.activityCategory.add(
            ActivityCategoryQuery.Add.Params(
                docId = category.docId,
                title = category.title,
                icon = ActivityIconDoc(format = Format.IMAGE, value = category.iconValue, tint = null)
            )
        )
    }
}
