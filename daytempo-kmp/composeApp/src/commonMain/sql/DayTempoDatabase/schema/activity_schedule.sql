CREATE TABLE activity_schedule (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    -- @@{ field=activity_id, propertyType=kotlin.uuid.Uuid }
    activity_id BLOB NOT NULL REFERENCES activity(id) ON DELETE CASCADE,

    -- @@{ field=mandatory_to_setup, propertyType=kotlin.Boolean }
    mandatory_to_setup INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=repeat, propertyType=com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat }
    repeat TEXT NOT NULL,

    -- @@{ field=allowed_repeat_modes, propertyType=kotlin.collections.Set<com.pluralfusion.daytempo.domain.model.ActivityScheduleRepeat> }
    allowed_repeat_modes TEXT NOT NULL,

    -- @@{ field=mon, propertyType=kotlin.Boolean }
    mon INTEGER NOT NULL,

    -- @@{ field=tue, propertyType=kotlin.Boolean }
    tue INTEGER NOT NULL,

    -- @@{ field=wed, propertyType=kotlin.Boolean }
    wed INTEGER NOT NULL,

    -- @@{ field=thu, propertyType=kotlin.Boolean }
    thu INTEGER NOT NULL,

    -- @@{ field=fri, propertyType=kotlin.Boolean }
    fri INTEGER NOT NULL,

    -- @@{ field=sat, propertyType=kotlin.Boolean }
    sat INTEGER NOT NULL,

    -- @@{ field=sun, propertyType=kotlin.Boolean }
    sun INTEGER NOT NULL,

    -- @@{ field=week1, propertyType=kotlin.Boolean }
    week1 INTEGER NOT NULL,

    -- @@{ field=week2, propertyType=kotlin.Boolean }
    week2 INTEGER NOT NULL,

    -- @@{ field=week3, propertyType=kotlin.Boolean }
    week3 INTEGER NOT NULL,

    -- @@{ field=week4, propertyType=kotlin.Boolean }
    week4 INTEGER NOT NULL,

    -- @@{ field=day0, propertyType=kotlin.Int }
    day0 INTEGER NOT NULL,

    -- @@{ field=day1, propertyType=kotlin.Int }
    day1 INTEGER NOT NULL,

    -- @@{ field=day2, propertyType=kotlin.Int }
    day2 INTEGER NOT NULL,

    -- @@{ field=day3, propertyType=kotlin.Int }
    day3 INTEGER NOT NULL,

    -- @@{ field=day4, propertyType=kotlin.Int }
    day4 INTEGER NOT NULL,

    -- @@{ field=start_at, propertyType=kotlinx.datetime.LocalDate, notNull=false }
    start_at INTEGER NOT NULL,

    -- @@{ field=start_at_eval }
    start_at_eval TEXT,

    -- @@{ field=read_ref_start_at }
    read_ref_start_at TEXT,

    -- @@{ field=write_ref_start_at }
    write_ref_start_at TEXT,

    -- @@{ field=start_at_label }
    start_at_label TEXT,

    -- @@{ field=repeat_after_days, propertyType=kotlin.Int }
    repeat_after_days INTEGER NOT NULL,

    -- @@{ field=read_ref_repeat_after_days }
    read_ref_repeat_after_days TEXT,

    -- @@{ field=write_ref_repeat_after_days }
    write_ref_repeat_after_days TEXT,

    -- @@{ field=repeat_after_days_label }
    repeat_after_days_label TEXT,

    -- @@{ field=repeat_after_days_min, propertyType=kotlin.Int }
    repeat_after_days_min INTEGER,

    -- @@{ field=repeat_after_days_max, propertyType=kotlin.Int }
    repeat_after_days_max INTEGER,

    -- @@{ field=allow_edit_days_duration, propertyType=kotlin.Boolean }
    allow_edit_days_duration INTEGER NOT NULL,

    -- @@{ field=days_duration, propertyType=kotlin.Int }
    days_duration INTEGER NOT NULL,

    -- @@{ field=read_ref_days_duration }
    read_ref_days_duration TEXT,

    -- @@{ field=write_ref_days_duration }
    write_ref_days_duration TEXT,

    -- @@{ field=days_duration_label }
    days_duration_label TEXT,

    -- @@{ field=days_duration_min, propertyType=kotlin.Int }
    days_duration_min INTEGER,

    -- @@{ field=days_duration_max, propertyType=kotlin.Int }
    days_duration_max INTEGER,

    -- @@{ field=time_points, propertyType=kotlin.collections.List<com.pluralfusion.daytempo.domain.model.AlarmHourMinute> }
    time_points TEXT NOT NULL,

    -- @@{ field=time_range, propertyType=com.pluralfusion.daytempo.domain.model.ActivityScheduleTimeRange }
    time_range TEXT
) WITHOUT ROWID;

CREATE INDEX idx_activitySchedule_activityId ON activity_schedule(activity_id);
CREATE INDEX idx_activitySchedule_repeat ON activity_schedule(repeat);
CREATE INDEX idx_activitySchedule_mon ON activity_schedule(mon);
CREATE INDEX idx_activitySchedule_tue ON activity_schedule(tue);
CREATE INDEX idx_activitySchedule_wed ON activity_schedule(wed);
CREATE INDEX idx_activitySchedule_thu ON activity_schedule(thu);
CREATE INDEX idx_activitySchedule_fri ON activity_schedule(fri);
CREATE INDEX idx_activitySchedule_sat ON activity_schedule(sat);
CREATE INDEX idx_activitySchedule_sun ON activity_schedule(sun);
CREATE INDEX idx_activitySchedule_week1 ON activity_schedule(week1);
CREATE INDEX idx_activitySchedule_week2 ON activity_schedule(week2);
CREATE INDEX idx_activitySchedule_week3 ON activity_schedule(week3);
CREATE INDEX idx_activitySchedule_week4 ON activity_schedule(week4);
CREATE INDEX idx_activitySchedule_day0 ON activity_schedule(day0);
CREATE INDEX idx_activitySchedule_day1 ON activity_schedule(day1);
CREATE INDEX idx_activitySchedule_day2 ON activity_schedule(day2);
CREATE INDEX idx_activitySchedule_day3 ON activity_schedule(day3);
CREATE INDEX idx_activitySchedule_day4 ON activity_schedule(day4);
CREATE INDEX idx_activitySchedule_startAt ON activity_schedule(start_at);
CREATE INDEX idx_activitySchedule_readRefStartAt ON activity_schedule(read_ref_start_at);
CREATE INDEX idx_activitySchedule_writeRefStartAt ON activity_schedule(write_ref_start_at);
CREATE INDEX idx_activitySchedule_repeatAfterDays ON activity_schedule(repeat_after_days);
CREATE INDEX idx_activitySchedule_readRefRepeatAfterDays ON activity_schedule(read_ref_repeat_after_days);
CREATE INDEX idx_activitySchedule_writeRefRepeatAfterDays ON activity_schedule(write_ref_repeat_after_days);
CREATE INDEX idx_activitySchedule_daysDuration ON activity_schedule(days_duration);
CREATE INDEX idx_activitySchedule_readRefDaysDuration ON activity_schedule(read_ref_days_duration);
CREATE INDEX idx_activitySchedule_writeRefDaysDuration ON activity_schedule(write_ref_days_duration);

CREATE VIEW activity_schedule_to_join AS
SELECT
    sch.id AS schedule__id,
    sch.activity_id AS schedule__activity_id,
    sch.mandatory_to_setup AS schedule__mandatory_to_setup,
    sch.repeat AS schedule__repeat,
    sch.allowed_repeat_modes AS schedule__allowed_repeat_modes,
    sch.mon AS schedule__mon,
    sch.tue AS schedule__tue,
    sch.wed AS schedule__wed,
    sch.thu AS schedule__thu,
    sch.fri AS schedule__fri,
    sch.sat AS schedule__sat,
    sch.sun AS schedule__sun,
    sch.week1 AS schedule__week1,
    sch.week2 AS schedule__week2,
    sch.week3 AS schedule__week3,
    sch.week4 AS schedule__week4,
    sch.day0 AS schedule__day0,
    sch.day1 AS schedule__day1,
    sch.day2 AS schedule__day2,
    sch.day3 AS schedule__day3,
    sch.day4 AS schedule__day4,
    sch.start_at AS schedule__start_at,
    sch.start_at_eval AS schedule__start_at_eval,
    sch.read_ref_start_at AS schedule__read_ref_start_at,
    sch.write_ref_start_at AS schedule__write_ref_start_at,
    sch.start_at_label AS schedule__start_at_label,
    sch.repeat_after_days AS schedule__repeat_after_days,
    sch.read_ref_repeat_after_days AS schedule__read_ref_repeat_after_days,
    sch.write_ref_repeat_after_days AS schedule__write_ref_repeat_after_days,
    sch.repeat_after_days_label AS schedule__repeat_after_days_label,
    sch.repeat_after_days_min AS schedule__repeat_after_days_min,
    sch.repeat_after_days_max AS schedule__repeat_after_days_max,
    sch.allow_edit_days_duration AS schedule__allow_edit_days_duration,
    sch.days_duration AS schedule__days_duration,
    sch.read_ref_days_duration AS schedule__read_ref_days_duration,
    sch.write_ref_days_duration AS schedule__write_ref_days_duration,
    sch.days_duration_label AS schedule__days_duration_label,
    sch.days_duration_min AS schedule__days_duration_min,
    sch.days_duration_max AS schedule__days_duration_max,
    sch.time_points AS schedule__time_points,
    sch.time_range AS schedule__time_range
FROM activity_schedule AS sch;
