UPDATE activity_bundle
SET installed_at = :installedAt,
    purchased = :purchased
WHERE doc_id = :docId;
