INSERT INTO typed_rows (
    id,
    name,
    note,
    count_value,
    small_count,
    medium_count,
    exact_amount,
    enabled_flag,
    rating,
    float4_value,
    data,
    created_at
)
VALUES (
    :id,
    :name,
    :note,
    :countValue,
    :smallCount,
    :mediumCount,
    :exactAmount,
    :enabledFlag,
    :rating,
    :float4Value,
    :data,
    :createdAt
);
