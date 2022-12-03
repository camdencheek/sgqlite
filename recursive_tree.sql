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
SELECT path, hex(paths.oid)
FROM paths WHERE kind == 4;

-- Given 1000 random blobs, find which ones are reachable from
-- some commit
WITH RECURSIVE paths (path, kind, oid) AS (
    SELECT te.name, te.kind, te.oid
    FROM tree_entries te
    JOIN commits ON commits.tree_oid = te.tree_oid
    WHERE commits.oid = X'd61f40b2f1aed793d13c393e66bf8d10fc8fff41'

    UNION ALL

    SELECT paths.path || '/' || te.name, te.kind, te.oid
    FROM paths
    JOIN tree_entries te
    ON te.tree_oid = paths.oid AND paths.kind == 3
)
SELECT path, hex(paths.oid)
FROM paths 
JOIN (SELECT oid FROM blobs ORDER BY random() LIMIT 1000) bl
    ON bl.oid = paths.oid
WHERE kind == 4;

-- List commits and paths a blob can be found in (slower than the previous query)
WITH RECURSIVE trees (tree_oid, path) AS (
    SELECT tree_oid, name
    FROM tree_entries
    WHERE oid = X'0d43989d2e1987bc91a20021d942c95b844d9e07'

    UNION ALL

    SELECT te.tree_oid, te.name || '/' || trees.path
    FROM trees
    JOIN tree_entries te
    ON te.oid = trees.tree_oid
)
SELECT hex(commits.oid), path
FROM trees
JOIN commits ON trees.tree_oid = commits.tree_oid
WHERE commits.oid = X'd61f40b2f1aed793d13c393e66bf8d10fc8fff41';
