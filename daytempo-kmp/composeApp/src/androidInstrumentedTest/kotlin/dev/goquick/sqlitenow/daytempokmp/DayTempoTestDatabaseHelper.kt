@file:OptIn(ExperimentalUuidApi::class)

package dev.goquick.sqlitenow.daytempokmp

import com.pluralfusion.daytempo.domain.model.ActivityBundlePurchaseMode
import com.pluralfusion.daytempo.domain.model.ActivityBundleWithActivitiesDoc
import com.pluralfusion.daytempo.domain.model.ActivityIconDoc
import com.pluralfusion.daytempo.domain.model.ActivityProgramType
import com.pluralfusion.daytempo.domain.model.ActivityReportingType
import com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat
import com.pluralfusion.daytempo.domain.model.ActivityScheduleTimeRange
import com.pluralfusion.daytempo.domain.model.AlarmHourMinute
import com.pluralfusion.daytempo.domain.model.Gender
import com.pluralfusion.daytempo.domain.model.GoalDirection
import com.pluralfusion.daytempo.domain.model.HasStringValue
import com.pluralfusion.daytempo.domain.model.MeasureSystem
import com.pluralfusion.daytempo.domain.model.ActivityPackageWithActivitiesDoc
import com.pluralfusion.daytempo.domain.model.ProgramItemLockItemDisplay
import com.pluralfusion.daytempo.domain.model.ProgramItemPresentation
import com.pluralfusion.daytempo.domain.model.RegisteredValueType
import com.pluralfusion.daytempo.domain.model.TemperatureSystem
import dev.goquick.sqlitenow.core.util.EnumByValueLookup
import dev.goquick.sqlitenow.daytempokmp.db.DayTempoDatabase
import dev.goquick.sqlitenow.daytempokmp.db.VersionBasedDatabaseMigrations
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.number
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.json.Json
import kotlin.collections.LinkedHashSet
import kotlin.time.ExperimentalTime
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/** Helper for creating fully configured DayTempoDatabase instances in instrumentation tests. */
object DayTempoTestDatabaseHelper {

    fun createDatabase(dbName: String = ":memory:", debug: Boolean = true): DayTempoDatabase {
        return DayTempoDatabase(
            dbName = dbName,
            migration = VersionBasedDatabaseMigrations(),
            debug = debug,
            profileAdapters = DayTempoDatabase.ProfileAdapters(
                sqlValueToGender = { Gender.from(it) },
                sqlValueToMeasureSystem = { MeasureSystem.from(it) },
                sqlValueToTemperatureSystem = { TemperatureSystem.from(it) },
                sqlValueToBodyHeightLastUpdate = { LocalDate.fromEpochDays(it) },
                sqlValueToBodyWeightLastUpdate = { LocalDate.fromEpochDays(it) },
                sqlValueToUnlockCodes = { it.columnWordsSplitter() },
                sqlValueToBirthday = { LocalDate.parse(it) },
                birthdayToSqlValue = { it.toDashedYMD() },
                genderToSqlValue = { it.value },
                temperatureSystemToSqlValue = { it.value },
                bodyWeightLastUpdateToSqlValue = { it.toEpochDays() },
                unlockCodesToSqlValue = { it.columnWordsJoiner() },
                measureSystemToSqlValue = { it.value },
                bodyHeightLastUpdateToSqlValue = { it.toEpochDays() },
            ),
            valueRegistryAdapters = DayTempoDatabase.ValueRegistryAdapters(
                valueTypeToSqlValue = { it.value },
                updatedAtToSqlValue = { it.toDayTempoEpochSeconds() },
                sqlValueToValueType = { RegisteredValueType.from(it) },
                sqlValueToUpdatedAt = { LocalDateTime.fromDayTempoEpochSeconds(it) },
            ),
            activityPackageAdapters = DayTempoDatabase.ActivityPackageAdapters(
                activityPackageWithActivitiesRowMapper = ActivityPackageWithActivitiesDoc::from,
            ),
            activityBundleAdapters = DayTempoDatabase.ActivityBundleAdapters(
                sqlValueToPurchaseMode = { ActivityBundlePurchaseMode.from(it) },
                sqlValueToPromoImage = { Json.decodeFromString(it) },
                sqlValueToPromoScr1 = { it?.decodeFromJson() },
                sqlValueToPromoScr2 = { it?.decodeFromJson() },
                sqlValueToPromoScr3 = { it?.decodeFromJson() },
                purchaseModeToSqlValue = { it.value },
                promoImageToSqlValue = { Json.encodeToString(it) },
                promoScr1ToSqlValue = { it?.let { Json.encodeToString(it) } },
                promoScr2ToSqlValue = { it?.let { Json.encodeToString(it) } },
                promoScr3ToSqlValue = { it?.let { Json.encodeToString(it) } },
                activityBundleWithActivitiesRowMapper = ActivityBundleWithActivitiesDoc::from,
            ),
            activityAdapters = DayTempoDatabase.ActivityAdapters(
                iconToSqlValue = { Json.encodeToString(it) },
                programTypeToSqlValue = { it.value },
                installedAtToSqlValue = { it?.toDayTempoEpochSeconds() },
                reportingToSqlValue = { it.value },
                sqlValueToId = { Uuid.fromByteArray(it) },
                sqlValueToProgramType = { ActivityProgramType.from(it) },
                sqlValueToInstalledAt = { it?.let { LocalDateTime.fromDayTempoEpochSeconds(it) } },
                sqlValueToIcon = { Json.decodeFromString(it) },
                sqlValueToReporting = { ActivityReportingType.from(it) },
            ),
            programItemAdapters = DayTempoDatabase.ProgramItemAdapters(
                goalDirectionToSqlValue = { it.value },
                presentationToSqlValue = { it?.value },
                lockItemDisplayToSqlValue = { it.value },
                inputEntriesToSqlValue = { Json.encodeToString(it) },
                sqlValueToGoalDirection = { GoalDirection.from(it) },
                sqlValueToPresentation = { ProgramItemPresentation.from(it) },
                sqlValueToLockItemDisplay = { ProgramItemLockItemDisplay.from(it) },
                sqlValueToInputEntries = { Json.decodeFromString(it) },
            ),
            activityScheduleAdapters = DayTempoDatabase.ActivityScheduleAdapters(
                activityIdToSqlValue = { it.toByteArray() },
                repeatToSqlValue = { it.value },
                allowedRepeatModesToSqlValue = { it.joinHasStringValues() },
                startAtToSqlValue = { it?.toEpochDays() ?: 0 },
                timePointsToSqlValue = { it.joinAlarmPoints() },
                timeRangeToSqlValue = { it?.value },
                sqlValueToActivityId = { Uuid.fromByteArray(it) },
                sqlValueToRepeat = { ActivityScheduleRepeat.from(it) },
                sqlValueToAllowedRepeatModes = { ActivityScheduleRepeat.parseSet(it) },
                sqlValueToStartAt = { LocalDate.fromEpochDays(it) },
                sqlValueToTimePoints = { it.columnWordsSplitter().map(AlarmHourMinute.Companion::parse) },
                sqlValueToTimeRange = { it?.let { ActivityScheduleTimeRange.from(it) } },
            ),
            dailyLogAdapters = DayTempoDatabase.DailyLogAdapters(
                sqlValueToDate = { LocalDate.fromEpochDays(it) },
                dateToSqlValue = { it.toEpochDays() },
            )
        )
    }
}

private fun String.columnWordsSplitter(): Set<String> =
    if (isBlank()) emptySet() else split("~").map(String::trim).filter(String::isNotEmpty).toSet()

private fun Collection<String>.columnWordsJoiner(): String =
    joinToString(separator = "~")

@OptIn(ExperimentalTime::class)
private fun LocalDateTime.toDayTempoEpochSeconds(): Long =
    toInstant(TimeZone.UTC).epochSeconds

@OptIn(ExperimentalTime::class)
private fun LocalDateTime.Companion.fromDayTempoEpochSeconds(epochSeconds: Long): LocalDateTime =
    Instant.fromEpochSeconds(epochSeconds).toLocalDateTime(TimeZone.UTC)

private fun <T> EnumByValueLookup<String, T>.parseEnumSet(serialized: String?): Set<T> {
    if (serialized.isNullOrBlank()) return emptySet()
    return serialized.split("~")
        .asSequence()
        .map(String::trim)
        .filter(String::isNotEmpty)
        .map(::from)
        .toCollection(LinkedHashSet())
}

private fun Iterable<HasStringValue>.joinHasStringValues(): String =
    joinToString(separator = "~") { it.value }

private fun Iterable<AlarmHourMinute>.joinAlarmPoints(): String =
    joinToString(separator = "~") { it.value }

private fun String?.decodeFromJson(): ActivityIconDoc? =
    this?.let { Json.decodeFromString(it) }

private fun AlarmHourMinute.Companion.parse(encoded: String): AlarmHourMinute {
    val parts = encoded.split(':')
    require(parts.size == 3) { "Invalid AlarmHourMinute encoding: '$encoded'" }
    val alarm = parts[0].toBooleanStrict()
    val hour = parts[1].toInt()
    val minute = parts[2].toInt()
    return AlarmHourMinute(alarm = alarm, hour = hour, minute = minute)
}

private fun LocalDate.toDashedYMD(): String {
    return "${this.year}-${this.month.number.toString().padStart(2, '0')}-${this.day.toString().padStart(2, '0')}"
}

private fun ActivityScheduleRepeat.Companion.parseSet(serialized: String?): Set<ActivityScheduleRepeat> =
    (this as EnumByValueLookup<String, ActivityScheduleRepeat>).parseEnumSet(serialized)
