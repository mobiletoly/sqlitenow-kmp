@file:OptIn(ExperimentalUuidApi::class)

package dev.goquick.sqlitenow.daytempokmp

import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pluralfusion.daytempo.domain.model.ActivityBundlePurchaseMode
import com.pluralfusion.daytempo.domain.model.ActivityIconDoc
import com.pluralfusion.daytempo.domain.model.ActivityProgramType
import com.pluralfusion.daytempo.domain.model.ActivityReportingType
import com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat
import com.pluralfusion.daytempo.domain.model.ActivityScheduleTimeRange
import com.pluralfusion.daytempo.domain.model.AlarmHourMinute
import com.pluralfusion.daytempo.domain.model.ActivityIconDoc.Format
import com.pluralfusion.daytempo.domain.model.ActivityBundleFullDoc
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityCategoryQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityPackageQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityQuery
import dev.goquick.sqlitenow.daytempokmp.db.ActivityScheduleQuery
import dev.goquick.sqlitenow.daytempokmp.db.DayTempoDatabase
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
    fun selectAllFullEnabledReturnsNestedStructure() = runBlocking {
        seedActivityBundleHierarchy()

        database.connection().prepare("SELECT doc_id, program_type FROM activity").use { stmt ->
            while (stmt.step()) {
                println("activity row: doc_id=" + stmt.getText(0) + ", program_type=" + stmt.getText(1))
            }
        }

        val results = database.activityBundle.selectAllFullEnabled.asList()

        assertEquals("Expected exactly one bundle", 1, results.size)
        val bundle: ActivityBundleFullDoc = results.single()

        assertEquals("Morning Routine", bundle.main.main.title)
        assertEquals(ActivityBundlePurchaseMode.FULLY_FREE, bundle.main.main.purchaseMode)
        assertEquals("Provider One", bundle.provider.title)
        assertEquals("Flexibility", bundle.category.title)

        val packageDoc = bundle.activityPackages.single()
        assertEquals("Starter Package", packageDoc.main.title)
        assertEquals("Flexibility", packageDoc.category.title)

        val activityDoc = packageDoc.activities.single()
        assertEquals("Sunrise Stretch", activityDoc.main.title)
        assertTrue("Activity should be enabled", activityDoc.main.enabled)
        assertEquals(ActivityProgramType.SIMPLE, activityDoc.main.programType)
        assertEquals(ActivityReportingType.DEFAULT, activityDoc.main.reporting)
        assertEquals(ActivityScheduleRepeat.WEEK_DAYS, activityDoc.schedule.repeat)
        assertEquals(setOf(ActivityScheduleRepeat.WEEK_DAYS), activityDoc.schedule.allowedRepeatModes)
        assertEquals(LocalDate.parse("2024-01-01"), activityDoc.schedule.startAt)
        assertEquals(listOf(AlarmHourMinute(alarm = true, hour = 6, minute = 30)), activityDoc.schedule.timePoints)
        assertEquals(ActivityScheduleTimeRange.MORNING, activityDoc.schedule.timeRange)

        // ensure nested collections are populated and no stray top-level activity list leaks out
        assertEquals(1, bundle.activityPackages.size)
        assertEquals(1, packageDoc.activities.size)
        assertTrue(
            "ActivityBundleFullDoc should only contain expected properties",
            ActivityBundleFullDoc::class.java.declaredFields.none { it.name == "activities" }
        )
    }

    @Test
    fun selectAllFullEnabledPreservesDistinctPackageAndActivityCategories() = runBlocking {
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

        val bundle = database.activityBundle.selectAllFullEnabled.asList().single()
        val packageDoc = bundle.activityPackages.single()
        val activityDoc = packageDoc.activities.single()

        assertEquals(packageCategory.title, bundle.category.title)
        assertEquals(packageCategory.title, packageDoc.category.title)
        assertEquals(activityCategory.title, activityDoc.category.title)
        assertEquals(packageCategory.docId, packageDoc.main.categoryDocId)
        assertEquals(activityCategory.docId, activityDoc.main.categoryDocId)
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
            ).execute()

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
            ).execute()

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
            ).execute()

            database.activity.add(
                ActivityQuery.Add.Params(
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
                    reporting = ActivityReportingType.DEFAULT
                )
            ).execute()
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
                    repeatAfterDays = 0,
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
                    timePoints = listOf(AlarmHourMinute(alarm = true, hour = 6, minute = 30)),
                    timeRange = ActivityScheduleTimeRange.MORNING
                )
            ).execute()
        }
    }

    private suspend fun addCategory(category: TestCategory) {
        database.activityCategory.add(
            ActivityCategoryQuery.Add.Params(
                docId = category.docId,
                title = category.title,
                icon = ActivityIconDoc(format = Format.IMAGE, value = category.iconValue, tint = null)
            )
        ).execute()
    }
}
