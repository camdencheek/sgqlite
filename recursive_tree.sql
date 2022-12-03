-- List file versions at a given commit
WITH RECURSIVE paths (path, kind, oid) AS (
    SELECT te.name, te.kind, te.oid
    FROM tree_entries te
    JOIN commits ON commits.tree_oid = te.tree_oid
    WHERE commits.oid = X'9294252DD315837C81CD67A58FD21012873D4243'

    UNION ALL

    SELECT paths.path || '/' || te.name, te.kind, te.oid
    FROM paths
    JOIN tree_entries te
    ON te.tree_oid = paths.oid AND paths.kind == 3
)
SELECT path, hex(oid) 
FROM paths 
WHERE kind == 4;

-- List commits and paths a blob can be found in
WITH RECURSIVE trees (tree_oid, path) AS (
    SELECT tree_oid, name
    FROM tree_entries
    -- WHERE oid = X'2124AAE8AA0815067498FE215156089FAF528760'
    WHERE oid IN (SELECT oid FROM blobs ORDER BY random() LIMIT 1)

    UNION ALL

    SELECT te.tree_oid, te.name || '/' || trees.path
    FROM trees
    JOIN tree_entries te
    ON te.oid = trees.tree_oid
)
SELECT hex(commits.oid), path
FROM trees
JOIN commits ON trees.tree_oid = commits.tree_oid;
