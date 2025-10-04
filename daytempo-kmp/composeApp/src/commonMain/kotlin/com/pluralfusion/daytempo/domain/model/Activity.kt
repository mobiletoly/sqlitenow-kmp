package com.pluralfusion.daytempo.domain.model

import dev.goquick.sqlitenow.core.util.EnumByValueLookup
import dev.goquick.sqlitenow.daytempokmp.db.ActivityBundleDetailedDoc
import dev.goquick.sqlitenow.daytempokmp.db.ActivityCategoryDoc
import dev.goquick.sqlitenow.daytempokmp.db.ActivityDetailedDoc
import dev.goquick.sqlitenow.daytempokmp.db.ActivityPackageDoc
import dev.goquick.sqlitenow.daytempokmp.db.ActivityPackageWithActivitiesRow
import dev.goquick.sqlitenow.daytempokmp.db.ProviderDoc
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.jetbrains.compose.resources.StringResource

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

class ActivityPackageFullDoc(
    val main: ActivityPackageDoc,
    val activities: List<ActivityDetailedDoc>,
    val category: ActivityCategoryDoc,
) {
    val activityTitles: List<String> = activities
        .filter { it.main.enabled && !it.main.deleted }
        .map { it.main.title }
    val activityStatuses: List<ActivityStatus> = activities
        .filter { !it.main.deleted }
        .map {
            if (it.schedule.mandatoryToSetup &&
                it.schedule.repeat == ActivityScheduleRepeat.DAYS_INTERVAL &&
                it.schedule.startAt.toEpochDays() == 0L &&
                it.schedule.startAtEval == null
            ) {
                ActivityStatus.SETUP_REQUIRED
            } else {
                ActivityStatus.READY
            }
        }
    val allGroupDocIdsSame: Boolean = activities
        .map { it.main.groupDocId }
        .distinct()
        .size == 1

    companion object {
        fun fromActivityPackageWithActivitiesRow(doc: ActivityPackageWithActivitiesRow) = ActivityPackageFullDoc(
            main = doc.main,
            activities = doc.activities,
            category = doc.category,
        )
    }
}

class ActivityBundleFullDoc(
    val main: ActivityBundleDetailedDoc,
    val activityPackages: List<ActivityPackageFullDoc>,
    val provider: ProviderDoc,
    val category: ActivityCategoryDoc,
)
