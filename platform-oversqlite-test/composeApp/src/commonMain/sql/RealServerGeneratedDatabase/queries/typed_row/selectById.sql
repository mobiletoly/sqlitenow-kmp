-- @@{name=SelectById}
SELECT
    id,
    name,
    count_value,
    small_count,
    medium_count,
    exact_amount,
    enabled_flag,
    rating,
    float4_value
FROM typed_rows
WHERE id = :id
