package com.pluralfusion.daytempo.domain.model

import dev.goquick.sqlitenow.core.util.EnumByValueLookup
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleDetailedRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleMetaRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleWithActivitiesRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleWithPackagesRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityCategoryRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityDetailedRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityPackageRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityPackageWithActivitiesRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityRow
import dev.goquick.sqlitenow.daytempokmp.db.ActivityWithProgramItemsRow
import dev.goquick.sqlitenow.daytempokmp.db.DailyLogDetailedRow
import dev.goquick.sqlitenow.daytempokmp.db.DailyLogRow
import dev.goquick.sqlitenow.daytempokmp.db.ProgramItemRow
import dev.goquick.sqlitenow.daytempokmp.db.ProviderRow
import kotlinx.datetime.DayOfWeek
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.text.category
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

//typealias ActivityDetailedDoc = ActivityDetailedRow
//typealias ActivityWithProgramItemsDoc = ActivityWithProgramItemsRow
typealias ActivityBundleDoc = ActivityBundleRow
typealias ActivityBundleDetailedDoc = ActivityBundleDetailedRow
typealias ActivityBundleMetaDoc = ActivityBundleMetaRow
typealias ActivityBundleWithPackagesDoc = ActivityBundleWithPackagesRow
typealias ActivityCategoryDoc = ActivityCategoryRow
typealias ActivityPackageDoc = ActivityPackageRow
//typealias ActivityScheduleDoc = ActivityScheduleRow
typealias DailyLogDoc = DailyLogRow
typealias DailyLogDetailedDoc = DailyLogDetailedRow
typealias ProviderDoc = ProviderRow
typealias ProgramItemDoc = ProgramItemRow

const val ACTIVITY_PRIORITY_LOW = 0
const val ACTIVITY_PRIORITY_NORMAL = 1
const val ACTIVITY_PRIORITY_HIGH = 2

data class ActivityDoc(
    val docId: String,
    val dependsOnDocId: String?,
    val groupDocId: String,
    val activityBundleDocId: String,
    val activityPackageDocId: String,
    val firstProgramItemDocId: String,
    val deleted: Boolean,
    val enabled: Boolean,
    val userDefined: Boolean,
    val programType: ActivityProgramType,
    val deleteWhenExpired: Boolean,
    val daysConfirmRequired: Boolean,
    val orderInd: Int,
    val installedAt: LocalDateTime?,
    val title: String,
    val descr: String,
    val icon: ActivityIconDoc,
    val monthlyGlanceView: Boolean,
    val requiredUnlockCode: String?,
    val priority: Int,
    val unlockedDays: Int?,
    val reporting: ActivityReportingType,
    val category: ActivityCategoryDoc,
    val schedule: ActivityScheduleDoc,
) {
    companion object {
        fun from(row: ActivityDetailedRow) = ActivityDoc(
            docId = row.main.docId,
            dependsOnDocId = row.main.dependsOnDocId,
            groupDocId = row.main.groupDocId,
            activityBundleDocId = row.main.activityBundleDocId,
            activityPackageDocId = row.main.activityPackageDocId,
            firstProgramItemDocId = row.firstProgramItemDocId,
            deleted = row.main.deleted,
            enabled = row.main.enabled,
            userDefined = row.main.userDefined,
            programType = row.main.programType,
            deleteWhenExpired = row.main.deleteWhenExpired,
            daysConfirmRequired = row.main.daysConfirmRequired,
            orderInd = row.main.orderInd,
            installedAt = row.main.installedAt,
            title = row.main.title,
            descr = row.main.descr,
            icon = row.main.icon,
            monthlyGlanceView = row.main.monthlyGlanceView,
            requiredUnlockCode = row.main.requiredUnlockCode,
            priority = row.main.priority,
            unlockedDays = row.main.unlockedDays,
            reporting = row.main.reporting,
            schedule = ActivityScheduleDoc.from(row.main),
            category = row.category,
        )

        fun empty() = ActivityDoc(
            docId = "",
            dependsOnDocId = null,
            groupDocId = "",
            activityBundleDocId = "",
            activityPackageDocId = "",
            firstProgramItemDocId = "",
            deleted = false,
            enabled = true,
            programType = ActivityProgramType.SIMPLE,
            deleteWhenExpired = false,
            daysConfirmRequired = false,
            orderInd = 0,
            installedAt = null,
            title = "",
            descr = "",
            icon = ActivityIconDoc.empty(),
            monthlyGlanceView = false,
            requiredUnlockCode = null,
            priority = ACTIVITY_PRIORITY_NORMAL,
            unlockedDays = null,
            reporting = ActivityReportingType.DEFAULT,
            userDefined = true,
            schedule = ActivityScheduleDoc.empty(),
            category = ActivityCategoryDoc.empty(),
        )
    }
}

data class ActivityScheduleDoc(
    val mandatoryToSetup: Boolean,
    val repeat: ActivityScheduleRepeat,
    val allowedRepeatModes: Set<ActivityScheduleRepeat>,
    val daysOfWeek: Set<DayOfWeek>,
    val daysOfMonth: Set<Int>,
    val weeks: Set<Int>,
    val startAt: LocalDate?,
    val startAtEval: String?,
    val startAtLabel: String?,
    val readRefStartAt: String?,
    val writeRefStartAt: String?,
    val repeatAfterDays: Int,
    val repeatAfterDaysLabel: String?,
    val repeatAfterDaysMin: Int?,
    val repeatAfterDaysMax: Int?,
    val readRefRepeatAfterDays: String?,
    val writeRefRepeatAfterDays: String?,
    val daysDuration: Int,
    val allowEditDaysDuration: Boolean,
    val readRefDaysDuration: String?,
    val writeRefDaysDuration: String?,
    val daysDurationLabel: String?,
    val daysDurationMin: Int?,
    val daysDurationMax: Int?,
    val timePoints: Set<AlarmHourMinute>,
    val timeRange: ActivityScheduleTimeRange?,
) {
    companion object {
        fun from(act: ActivityRow): ActivityScheduleDoc {
            return ActivityScheduleDoc(
                mandatoryToSetup = act.schedMandatoryToSetup,
                repeat = act.schedRepeat,
                allowedRepeatModes = act.schedAllowedRepeatModes,
                daysOfWeek = buildDayOfWeekSet(
                    act.schedMon,
                    act.schedTue,
                    act.schedWed,
                    act.schedThu,
                    act.schedFri,
                    act.schedSat,
                    act.schedSun,
                ),
                daysOfMonth = setOf(act.schedDay0, act.schedDay1, act.schedDay2, act.schedDay3, act.schedDay4),
                weeks = buildWeekSet(act.schedWeek1, act.schedWeek2, act.schedWeek3, act.schedWeek4),
                startAt = act.schedStartAt,
                startAtEval = act.schedStartAtEval,
                startAtLabel = act.schedStartAtLabel,
                readRefStartAt = act.schedReadRefStartAt,
                writeRefStartAt = act.schedWriteRefStartAt,
                repeatAfterDays = act.schedRepeatAfterDays,
                repeatAfterDaysLabel = act.schedRepeatAfterDaysLabel,
                repeatAfterDaysMin = act.schedRepeatAfterDaysMin,
                repeatAfterDaysMax = act.schedRepeatAfterDaysMax,
                readRefRepeatAfterDays = act.schedReadRefRepeatAfterDays,
                writeRefRepeatAfterDays = act.schedWriteRefRepeatAfterDays,
                daysDuration = act.schedDaysDuration,
                allowEditDaysDuration = act.schedAllowEditDaysDuration,
                readRefDaysDuration = act.schedReadRefDaysDuration,
                writeRefDaysDuration = act.schedWriteRefDaysDuration,
                daysDurationLabel = act.schedDaysDurationLabel,
                daysDurationMin = act.schedDaysDurationMin,
                daysDurationMax = act.schedDaysDurationMax,
                timePoints = act.schedTimePoints.toSet(),
                timeRange = act.schedTimeRange,
            )
        }

        fun empty() = ActivityScheduleDoc(
            mandatoryToSetup = false,
            repeat = ActivityScheduleRepeat.NONE,
            allowedRepeatModes = emptySet(),
            daysOfWeek = emptySet(),
            daysOfMonth = emptySet(),
            weeks = emptySet(),
            startAt = null,
            startAtEval = null,
            startAtLabel = null,
            readRefStartAt = null,
            writeRefStartAt = null,
            repeatAfterDays = 0,
            repeatAfterDaysLabel = null,
            repeatAfterDaysMin = null,
            repeatAfterDaysMax = null,
            readRefRepeatAfterDays = null,
            writeRefRepeatAfterDays = null,
            daysDuration = 0,
            allowEditDaysDuration = false,
            readRefDaysDuration = null,
            writeRefDaysDuration = null,
            daysDurationLabel = null,
            daysDurationMin = null,
            daysDurationMax = null,
            timePoints = emptySet(),
            timeRange = null,
        )
    }
}

@Serializable
data class ActivityIconDoc(
    @SerialName("format")
    val format: Format,

    @SerialName("value")
    val value: String,

    @SerialName("tint")
    val tint: String?,
) {
    enum class Format(val value: String) {
        NONE("none"),
        IMAGE("image"),
        FONT_AWESOME("fontawesome");

        companion object : EnumByValueLookup<String, Format>(
            entries.associateBy(
                Format::value
            )
        )
    }

    companion object {
        fun empty() = ActivityIconDoc(
            format = Format.NONE,
            value = "",
            tint = null,
        )
    }
}

enum class ActivityBundlePurchaseMode(val value: String) {
    FULLY_FREE("fullyFree"),
    PARTIALLY_FREE("partiallyFree"),
    PAID("paid");

    companion object :
        EnumByValueLookup<String, ActivityBundlePurchaseMode>(
            ActivityBundlePurchaseMode.entries.associateBy(
                ActivityBundlePurchaseMode::value
            )
        )
}

enum class ActivityProgramType(override val value: String) : HasStringValue {
    MARKER("marker"),
    SIMPLE("simple"),
    FINITE("finite"),
    DAILY_COUNTER("dailyCounter");

    fun hasSingleAssociatedProgramItem(): Boolean = when (this) {
        MARKER, SIMPLE, DAILY_COUNTER -> true
        FINITE -> false
    }

    companion object :
        EnumByValueLookup<String, ActivityProgramType>(
            entries.associateBy(
                ActivityProgramType::value
            )
        )
}

enum class ActivityReportingType(override val value: String) : HasStringValue {
    DEFAULT("default"),
    DISABLED("disabled");

    companion object :
        EnumByValueLookup<String, ActivityReportingType>(
            ActivityReportingType.entries.associateBy(
                ActivityReportingType::value
            )
        )
}

enum class ActivityScheduleRepeat(override val value: String) : HasStringValue {
    NONE("none"),

    //    SINGLE("single"),
    WEEK_DAYS("weekDays"),
    DAYS_OF_MONTH("daysOfMonth"),
    DAYS_INTERVAL("daysInterval");

    companion object :
        EnumByValueLookup<String, ActivityScheduleRepeat>(
            ActivityScheduleRepeat.entries.associateBy(
                ActivityScheduleRepeat::value
            )
        )
}

enum class ActivityScheduleTimeRange(override val value: String) : HasStringValue {
    MORNING("morning"),
    AFTERNOON("afternoon"),
    EVENING("evening");

    companion object :
        EnumByValueLookup<String, ActivityScheduleTimeRange>(
            ActivityScheduleTimeRange.entries.associateBy(
                ActivityScheduleTimeRange::value
            )
        )
}

data class AlarmHourMinute(
    val alarm: Boolean,
    val hour: Int,
    val minute: Int,
) : HasStringValue {
    override val value: String
        get() = "$alarm:$hour:$minute"

    companion object {
    }
}

enum class GoalDirection(override val value: String) : HasStringValue {
    @SerialName("UP")
    UP("up"),

    @SerialName("DOWN")
    DOWN("down");

    companion object : EnumByValueLookup<String, GoalDirection>(entries.associateBy(GoalDirection::value))
}

@Serializable
data class ProgramItemInputEntry(
    @SerialName("format")
    val format: Format,
    @SerialName("label")
    val label: String,
    @SerialName("descr")
    val descr: String,
    @SerialName("mandatory")
    val mandatory: Boolean,
    @SerialName("showInfo")
    val showInfo: ShowInfo? = null,
    @SerialName("writeRefValue")
    val writeRefValue: String?,
    @SerialName("selectorUnspecified")
    val selectorUnspecified: String?,
    @SerialName("selectorLabels")
    val selectorLabels: List<String>,
    @SerialName("selectorValues")
    val selectorValues: List<Double> = listOf(),
    @SerialName("selectorValueSuffix")
    val selectorValueSuffix: String? = null,
    @SerialName("min")
    val min: Double?,
    @SerialName("max")
    val max: Double?,
    @SerialName("leftImage")
    val leftImage: String? = null,
    @SerialName("topImage")
    val topImage: String? = null,
) {
    enum class Format(val value: String) {
        @SerialName("UNSIGNED_INTEGER")
        UNSIGNED_INTEGER("unsignedInteger"),

        @SerialName("SMALL_WEIGHT")
        SMALL_WEIGHT("small_weight"),

        @SerialName("LARGE_WEIGHT")
        LARGE_WEIGHT("large_weight"),

        @SerialName("TEMPERATURE")
        TEMPERATURE("temperature"),

        @SerialName("SMALL_VOLUME")
        SMALL_VOLUME("volume"),

        @SerialName("LARGE_VOLUME")
        LARGE_VOLUME("large_volume"),

        @SerialName("TEXT")
        TEXT("text"),

        @SerialName("SINGLE_SELECTOR")
        SINGLE_SELECTOR("singleSelector"),

        @SerialName("NUMERIC_SLIDER")
        NUMERIC_SLIDER("numericSlider"),

        @SerialName("LABEL")
        LABEL("label");
    }

    enum class ShowInfo(val value: String) {
        @SerialName("PREVIOUS_VALUE")
        PREVIOUS_VALUE("previousValue");

        companion object : EnumByValueLookup<String, ShowInfo>(entries.associateBy(ShowInfo::value))
    }
}

enum class ProgramItemLockItemDisplay(override val value: String) : HasStringValue {
    @SerialName("DEFAULT")
    DEFAULT("default"),

    @SerialName("BLURRED")
    BLURRED("blurred");

    companion object :
        EnumByValueLookup<String, ProgramItemLockItemDisplay>(entries.associateBy(ProgramItemLockItemDisplay::value))
}

enum class ProgramItemPresentation(override val value: String) : HasStringValue {
    @SerialName("DEFAULT")
    DEFAULT("default"),

    @SerialName("VERTICAL_COUNTER_BAR")
    VERTICAL_COUNTER_BAR("verticalCounterBars");

    companion object :
        EnumByValueLookup<String, ProgramItemPresentation>(entries.associateBy(ProgramItemPresentation::value))
}

enum class Gender(val value: String) {
    MALE("male"),
    FEMALE("female"),
    OTHER("other");

    companion object : EnumByValueLookup<String, Gender>(Gender.entries.associateBy { it.value })
}

enum class MeasureSystem(override val value: String) : HasStringValue {
    METRIC("metric"),
    US("US"),
    UK("UK");

    companion object :
        EnumByValueLookup<String, MeasureSystem>(entries.associateBy(MeasureSystem::value))
}

enum class WeightMeasure(val value: String) {
    G("g"),
    KG("kg"),
    OZ("oz"),
    LB("lb");
}

enum class VolumeMeasure(val value: String) {
    ML("ml"),
    L("liter"),
    UK_FL_OZ("fl oz"),
    US_FL_OZ("fl oz");
}

enum class TemperatureSystem(override val value: String) : HasStringValue {
    CELSIUS("C"),
    FAHRENHEIT("F");

    companion object :
        EnumByValueLookup<String, TemperatureSystem>(entries.associateBy(TemperatureSystem::value))
}

enum class RegisteredValueType(val value: String) {
    NULL("null"),
    STRING("string"),
    NUMERIC("numeric");

    companion object :
        EnumByValueLookup<String, RegisteredValueType>(entries.associateBy(RegisteredValueType::value))
}

enum class ActivityStatus {
    READY,
    SETUP_REQUIRED,
}

data class ActivityPackageWithActivitiesDoc(
    val main: ActivityPackageDoc,
    val activities: List<ActivityDoc>,
    val category: ActivityCategoryDoc,
) {
    val activityTitles: List<String> = activities
        .filter { it.enabled && !it.deleted }
        .map { it.title }
    val activityStatuses: List<ActivityStatus> = activities
        .filter { !it.deleted }
        .map {
            if (it.schedule.mandatoryToSetup &&
                it.schedule.repeat == ActivityScheduleRepeat.DAYS_INTERVAL &&
                it.schedule.startAt == null &&
                it.schedule.startAtEval == null
            ) {
                ActivityStatus.SETUP_REQUIRED
            } else {
                ActivityStatus.READY
            }
        }
    val allGroupDocIdsSame: Boolean = activities
        .map { it.groupDocId }
        .distinct()
        .size == 1

    companion object {
        fun from(row: ActivityPackageWithActivitiesRow) = ActivityPackageWithActivitiesDoc(
            main = row.main,
            activities = row.activities.map { ActivityDoc.from(it) },
            category = row.category,
        )
    }
}

internal fun buildDayOfWeekSet(
    activityScheduleMon: Boolean,
    activityScheduleTue: Boolean,
    activityScheduleWed: Boolean,
    activityScheduleThu: Boolean,
    activityScheduleFri: Boolean,
    activityScheduleSat: Boolean,
    activityScheduleSun: Boolean,
) = buildSet {
    if (activityScheduleMon) add(DayOfWeek.MONDAY)
    if (activityScheduleTue) add(DayOfWeek.TUESDAY)
    if (activityScheduleWed) add(DayOfWeek.WEDNESDAY)
    if (activityScheduleThu) add(DayOfWeek.THURSDAY)
    if (activityScheduleFri) add(DayOfWeek.FRIDAY)
    if (activityScheduleSat) add(DayOfWeek.SATURDAY)
    if (activityScheduleSun) add(DayOfWeek.SUNDAY)
}

internal fun buildWeekSet(
    week1: Boolean,
    week2: Boolean,
    week3: Boolean,
    week4: Boolean,
) = buildSet {
    if (week1) add(1)
    if (week2) add(2)
    if (week3) add(3)
    if (week4) add(4)
}

@OptIn(ExperimentalUuidApi::class)
fun ActivityCategoryRow.Companion.empty() = ActivityCategoryDoc(
    id = Uuid.NIL,
    docId = "",
    title = "",
    icon = ActivityIconDoc.empty(),
)

class ActivityBundleWithActivitiesDoc(
    val main: ActivityBundleDetailedDoc,
    val activityPackages: List<ActivityPackageWithActivitiesDoc>,
    val provider: ProviderDoc,
    val category: ActivityCategoryDoc,
) {
    companion object {
        fun from(row: ActivityBundleWithActivitiesRow) = ActivityBundleWithActivitiesDoc(
            main = row.detailedBundle,
            activityPackages = row.activityPackages.map { ActivityPackageWithActivitiesDoc.from(it) },
            provider = row.provider,
            category = row.category,
        )
    }
}

data class ActivityWithProgramItemsDoc(
    val activity: ActivityDoc,
    val programItems: List<ProgramItemDoc>,
) {
    companion object {
        fun from(row: ActivityWithProgramItemsRow) = ActivityWithProgramItemsDoc(
            activity = ActivityDoc.from(row.main),
            programItems = row.programItems,
        )
    }
}
