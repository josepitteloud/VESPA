SELECT
          count(*)                                         Total_rows
        , count(cb_key_household)                       AS Total_HH
        , count(DISTINCT cb_key_household)              AS Unique_HH
        , count(cb_key_individual)                      AS Total_Ind
        , count(DISTINCT cb_key_individual)             AS Unique_Ind
FROM sk_prodreg.EXPERIAN_CONSUMERVIEW
WHERE cb_source_file like 'Sky_20131113_ConsumerView-PandH_live.dat.gz';
----------------------------------------------------------------
SELECT
          count(*)                                         Total_rows
        , count(cb_key_household)                       AS Total_HH
        , count(DISTINCT cb_key_household)              AS Unique_HH
        , count(cb_key_individual)                      AS Total_Ind
        , count(DISTINCT cb_key_individual)             AS Unique_Ind
FROM sk_prod.EXPERIAN_CONSUMERVIEW
----------------------------------------------------------------
----------------Matching counts --------------------------------
----------------------------------------------------------------
SELECT
        'HH Level' Typ
        , count(DISTINCT a.cb_key_household) Unique_matching_HH
FROM sk_prodreg.EXPERIAN_CONSUMERVIEW   AS a
JOIN sk_prod.EXPERIAN_CONSUMERVIEW                 AS b ON a.cb_key_household = b.cb_key_household AND b.cb_key_household is not null
WHERE a.cb_source_file like 'Sky_20131113_ConsumerView-PandH_live.dat.gz'
UNION
SELECT
        'Ind Level' Typ
        , count(DISTINCT a.cb_key_individual) Unique_matching_ind
FROM sk_prodreg.EXPERIAN_CONSUMERVIEW   AS a
JOIN sk_prod.EXPERIAN_CONSUMERVIEW                 AS c ON a.cb_key_individual = c.cb_key_individual AND c.cb_key_individual  is not null
WHERE a.cb_source_file like 'Sky_20131113_ConsumerView-PandH_live.dat.gz'

SELECT 