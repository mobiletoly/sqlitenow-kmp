CREATE TABLE profile (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    name TEXT NOT NULL UNIQUE,

    -- @@{ field=measure_system, propertyType=com.pluralfusion.daytempo.domain.model.MeasureSystem }
    measure_system TEXT NOT NULL DEFAULT 'metric',

    -- @@{ field=temperature_system, propertyType=com.pluralfusion.daytempo.domain.model.TemperatureSystem }
    temperature_system TEXT NOT NULL DEFAULT 'F',

    -- @@{ field=body_weight, propertyType=kotlin.Int }
    body_weight INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=body_weight_last_update, propertyType=kotlinx.datetime.LocalDate }
    body_weight_last_update INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=body_height, propertyType=kotlin.Int }
    body_height INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=body_height_last_update, propertyType=kotlinx.datetime.LocalDate }
    body_height_last_update INTEGER NOT NULL DEFAULT 0,

    -- @@{ field=unlock_codes, propertyType=kotlin.collections.Set<kotlin.String> }
    unlock_codes TEXT NOT NULL DEFAULT '',

    -- @@{ field=essential_plan, propertyType=kotlin.Boolean }
    essential_plan INTEGER NOT NULL DEFAULT 0 ,

    -- @@{ field=gender, propertyType=com.pluralfusion.daytempo.domain.model.Gender }
    gender TEXT NOT NULL DEFAULT 'male',

    -- @@{ field=birthday, propertyType=kotlinx.datetime.LocalDate }
    birthday TEXT NOT NULL DEFAULT '1980-01-01'
) WITHOUT ROWID;

CREATE INDEX idx_profile_name ON profile(name);
