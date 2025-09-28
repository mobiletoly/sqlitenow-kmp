INSERT INTO activity_package (
    doc_id,
    activity_bundle_doc_id,
    title,
    descr,
    pre_start_text,
    user_defined,
    category_doc_id,
    icon
) VALUES (
    :docId,
    :activityBundleDocId,
    :title,
    :descr,
    :preStartText,
    :userDefined,
    :categoryDocId,
    :icon
);
