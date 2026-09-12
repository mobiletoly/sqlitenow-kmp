INSERT INTO parity_value (
    text_value,
    nullable_value,
    real_value,
    blob_value,
    exact_value
)
VALUES (
    :textValue,
    :nullableValue,
    :realValue,
    :blobValue,
    :exactValue
)
RETURNING *;
