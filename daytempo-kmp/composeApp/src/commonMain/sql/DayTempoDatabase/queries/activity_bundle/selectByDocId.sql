-- @@{ queryResult=ActivityBundleDoc }
SELECT  id, doc_id, provider_doc_id, version, title, descr, user_defined, purchase_mode,
        unlock_code, purchased, installed_at, resources_json, icon,
        promo_image, promo_scr1, promo_scr2, promo_scr3
FROM activity_bundle act WHERE act.doc_id = :docId;
