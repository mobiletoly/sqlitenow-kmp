-- @@{ queryResult=ParentWithChildrenDoc }
SELECT parent.*
FROM parent_with_children_view parent
WHERE parent.parent__doc_id = :docId;
