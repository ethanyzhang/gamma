INSERT INTO n001Md08k04
    SELECT
        row_number() OVER () AS i,
        MOD(row_number() OVER (),
            CAST(LOG(n)/LOG(2) AS INT))+1 AS incr_step,
        X1, X2, X3, X4, X5, X6, X7, X8
    FROM
        n001Md08k04_import,
        (SELECT COUNT(*) AS n FROM n001Md08k04_import) XN;

INSERT INTO n001Md08k08
    SELECT
        row_number() OVER () AS i,
        MOD(row_number() OVER (),
            CAST(LOG(n)/LOG(2) AS INT))+1 AS incr_step,
        X1, X2, X3, X4, X5, X6, X7, X8
    FROM
        n001Md08k08_import,
        (SELECT COUNT(*) AS n FROM n001Md08k08_import) XN;

INSERT INTO n001Md08k16
    SELECT
        row_number() OVER () AS i,
        MOD(row_number() OVER (),
            CAST(LOG(n)/LOG(2) AS INT))+1 AS incr_step,
        X1, X2, X3, X4, X5, X6, X7, X8
    FROM
        n001Md08k16_import,
        (SELECT COUNT(*) AS n FROM n001Md08k16_import) XN;

INSERT INTO n010Md08k04
    SELECT
        row_number() OVER () AS i,
        MOD(row_number() OVER (),
            CAST(LOG(n)/LOG(2) AS INT))+1 AS incr_step,
        X1, X2, X3, X4, X5, X6, X7, X8
    FROM
        n010Md08k04_import,
        (SELECT COUNT(*) AS n FROM n010Md08k04_import) XN;

INSERT INTO n010Md08k08
    SELECT
        row_number() OVER () AS i,
        MOD(row_number() OVER (),
            CAST(LOG(n)/LOG(2) AS INT))+1 AS incr_step,
        X1, X2, X3, X4, X5, X6, X7, X8
    FROM
        n010Md08k08_import,
        (SELECT COUNT(*) AS n FROM n010Md08k08_import) XN;

INSERT INTO n010Md08k16
    SELECT
        row_number() OVER () AS i,
        MOD(row_number() OVER (),
            CAST(LOG(n)/LOG(2) AS INT))+1 AS incr_step,
        X1, X2, X3, X4, X5, X6, X7, X8
    FROM
        n010Md08k16_import,
        (SELECT COUNT(*) AS n FROM n010Md08k16_import) XN;
