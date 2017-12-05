-----------------------------------------------------------------
--Scaling Variables and Indices
-----------------------------------------------------------------
--Universe segment***********************************************
--Get the total number of Sky base & Vespa panel accounts
-----------------------------------------------------------------
SELECT      SUM(weight.sky_base_accounts) AS sb_cat_total
            , SUM(weight.vespa_accounts) AS va_cat_total
INTO #index1_uni
FROM        vespa_analysts.SC2_weightings AS weight
                        INNER JOIN  vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings);

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------
SELECT      lookup.universe AS category
            , SUM(weight.sky_base_accounts) / in1.sb_cat_total AS sb_index
            , SUM(weight.vespa_accounts) / in1.va_cat_total AS va_index
INTO #index2_uni
FROM        #index1_uni in1, vespa_analysts.SC2_weightings AS weight
                        INNER JOIN vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.universe, in1.sb_cat_total, in1.va_cat_total;

-----------------------------------------------------------------
--Present the data in segments and categories, with Sky base &
--Vespa account numbers, coverage and the index - by dividing by
--percent to total
-----------------------------------------------------------------
SELECT      1 AS Seg_num
            , 'HH Box Comp.' AS segment
            , lookup.universe AS category
            , SUM(weight.sky_base_accounts) AS Sky_base
            , SUM(weight.vespa_accounts) AS vespa_accounts
            , ROUND(SUM(weight.vespa_accounts*weighting),0) AS coverage
            , CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) AS "index"
INTO #HH_Box_Comp
FROM        vespa_analysts.SC2_weightings AS weight
                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup ON weight.scaling_segment_id = lookup.scaling_segment_id
                        inner join #index2_uni AS in2 ON lookup.universe = in2.category
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.universe, in2.va_index, in2.sb_index
ORDER BY category ;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #index1_uni;
DROP TABLE #index2_uni;
--select * from #HH_Box_Comp
-----------------------------------------------------------------
--TV Region Segment**********************************************
--Get the total number of Sky base & Vespa panel accounts
-----------------------------------------------------------------
SELECT      SUM(weight.sky_base_accounts) AS sb_cat_total
            , SUM(weight.vespa_accounts) AS va_cat_total
INTO #index1_reg
FROM        vespa_analysts.SC2_weightings AS weight
                        INNER JOIN  vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings);

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------
SELECT      lookup.isba_tv_region AS category
            , SUM(weight.sky_base_accounts) / in1.sb_cat_total AS sb_index
            , SUM(weight.vespa_accounts) / in1.va_cat_total AS va_index
INTO #index2_reg
FROM        #index1_reg in1, vespa_analysts.SC2_weightings AS weight
                        INNER JOIN vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.isba_tv_region, in1.sb_cat_total, in1.va_cat_total;

-----------------------------------------------------------------
--Present the data in segments and categories, with Sky base &
--Vespa account numbers, coverage and the index - by dividing by
--percent to total
-----------------------------------------------------------------
SELECT      2 AS Seg_num
            , 'TV Region' AS segment
            , lookup.isba_tv_region AS category
            , SUM(weight.sky_base_accounts) AS Sky_base
            , SUM(weight.vespa_accounts) AS vespa_accounts
            , ROUND(SUM(weight.vespa_accounts*weighting),0) AS coverage
            , CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) AS "index"
INTO #TV_Region
FROM        vespa_analysts.SC2_weightings AS weight
                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup ON weight.scaling_segment_id = lookup.scaling_segment_id
                        inner join #index2_reg AS in2 ON lookup.isba_tv_region = in2.category
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.isba_tv_region, in2.va_index, in2.sb_index
ORDER BY category ;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #index1_reg;
DROP TABLE #index2_reg;

-----------------------------------------------------------------
--Household Composition Segment**********************************
--Get the total number of Sky base & Vespa panel accounts
-----------------------------------------------------------------
SELECT      SUM(weight.sky_base_accounts) AS sb_cat_total
            , SUM(weight.vespa_accounts) AS va_cat_total
INTO #index1_comp
FROM        vespa_analysts.SC2_weightings AS weight
                        INNER JOIN  vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings);

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------
SELECT      lookup.hhcomposition AS category
            , SUM(weight.sky_base_accounts) / in1.sb_cat_total AS sb_index
            , SUM(weight.vespa_accounts) / in1.va_cat_total AS va_index
INTO #index2_comp
FROM        #index1_comp in1, vespa_analysts.SC2_weightings AS weight
                        INNER JOIN vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.hhcomposition, in1.sb_cat_total, in1.va_cat_total;

-----------------------------------------------------------------
--Present the data in segments and categories, with Sky base &
--Vespa account numbers, coverage and the index - by dividing by
--percent to total
-----------------------------------------------------------------
SELECT      3 AS Seg_num
            , 'HH Composition' AS segment
            ,CASE WHEN hhcomposition = '00' THEN hhcomposition || ': Families'
                  WHEN hhcomposition = '01' THEN hhcomposition || ': Extended family'
                  WHEN hhcomposition = '02' THEN hhcomposition || ': Extended household'
                  WHEN hhcomposition = '03' THEN hhcomposition || ': Pseudo family'
                  WHEN hhcomposition = '04' THEN hhcomposition || ': Single male'
                  WHEN hhcomposition = '05' THEN hhcomposition || ': Single female'
                  WHEN hhcomposition = '06' THEN hhcomposition || ': Male homesharers'
                  WHEN hhcomposition = '07' THEN hhcomposition || ': Female homesharers'
                  WHEN hhcomposition = '08' THEN hhcomposition || ': Mixed homesharers'
                  WHEN hhcomposition = '09' THEN hhcomposition || ': Abbreviated male families'
                  WHEN hhcomposition = '10' THEN hhcomposition || ': Abbreviated female families'
                  WHEN hhcomposition = '11' THEN hhcomposition || ': Multi-occupancy dwelling'
                  WHEN hhcomposition = 'U' THEN hhcomposition || ': Unclassified HHComp'
                  ELSE hhcomposition
            END AS category
            , SUM(weight.sky_base_accounts) AS Sky_base
            , SUM(weight.vespa_accounts) AS vespa_accounts
            , ROUND(SUM(weight.vespa_accounts*weighting),0) AS coverage
            , CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) AS "index"
INTO #HH_Composition
FROM        vespa_analysts.SC2_weightings AS weight
                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup ON weight.scaling_segment_id = lookup.scaling_segment_id
                        inner join #index2_comp AS in2 ON lookup.hhcomposition = in2.category
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.hhcomposition, in2.va_index, in2.sb_index
ORDER BY category ;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #index1_comp;
DROP TABLE #index2_comp;

-----------------------------------------------------------------
--Tenure Segment**********************************************
--Get the total number of Sky base & Vespa panel accounts
-----------------------------------------------------------------
SELECT      SUM(weight.sky_base_accounts) AS sb_cat_total
            , SUM(weight.vespa_accounts) AS va_cat_total
INTO #index1_ten
FROM        vespa_analysts.SC2_weightings AS weight
                        INNER JOIN  vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings);

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------
SELECT      lookup.tenure AS category
            , SUM(weight.sky_base_accounts) / in1.sb_cat_total AS sb_index
            , SUM(weight.vespa_accounts) / in1.va_cat_total AS va_index
INTO #index2_ten
FROM        #index1_ten in1, vespa_analysts.SC2_weightings AS weight
                        INNER JOIN vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.tenure, in1.sb_cat_total, in1.va_cat_total;

-----------------------------------------------------------------
--Present the data in segments and categories, with Sky base &
--Vespa account numbers, coverage and the index - by dividing by
--percent to total
-----------------------------------------------------------------
SELECT      4 AS Seg_num
            , 'Tenure' AS segment
            , lookup.tenure AS category
            , SUM(weight.sky_base_accounts) AS Sky_base
            , SUM(weight.vespa_accounts) AS vespa_accounts
            , ROUND(SUM(weight.vespa_accounts*weighting),0) AS coverage
            , CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) AS "index"
INTO #Tenure
FROM        vespa_analysts.SC2_weightings AS weight
                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup ON weight.scaling_segment_id = lookup.scaling_segment_id
                        inner join #index2_ten AS in2 ON lookup.tenure = in2.category
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.tenure, in2.va_index, in2.sb_index
ORDER BY category ;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #index1_ten;
DROP TABLE #index2_ten;

-----------------------------------------------------------------
--Package Segment**********************************************
--Get the total number of Sky base & Vespa panel accounts
-----------------------------------------------------------------
SELECT      SUM(weight.sky_base_accounts) AS sb_cat_total
            , SUM(weight.vespa_accounts) AS va_cat_total
INTO #index1_pack
FROM        vespa_analysts.SC2_weightings AS weight
                        INNER JOIN  vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings);

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------
SELECT      lookup.package AS category
            , SUM(weight.sky_base_accounts) / in1.sb_cat_total AS sb_index
            , SUM(weight.vespa_accounts) / in1.va_cat_total AS va_index
INTO #index2_pack
FROM        #index1_pack in1, vespa_analysts.SC2_weightings AS weight
                        INNER JOIN vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.package, in1.sb_cat_total, in1.va_cat_total;

-----------------------------------------------------------------
--Present the data in segments and categories, with Sky base &
--Vespa account numbers, coverage and the index - by dividing by
--percent to total
-----------------------------------------------------------------
SELECT      5 AS Seg_num
            , 'Package' AS segment
            , lookup.package AS category
            , SUM(weight.sky_base_accounts) AS Sky_base
            , SUM(weight.vespa_accounts) AS vespa_accounts
            , ROUND(SUM(weight.vespa_accounts*weighting),0) AS coverage
            , CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) AS "index"
INTO #Package
FROM        vespa_analysts.SC2_weightings AS weight
                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup ON weight.scaling_segment_id = lookup.scaling_segment_id
                        inner join #index2_pack AS in2 ON lookup.package = in2.category
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.package, in2.va_index, in2.sb_index
ORDER BY category ;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #index1_pack;
DROP TABLE #index2_pack;

-----------------------------------------------------------------
--Box Type Segment**********************************************
--Get the total number of Sky base & Vespa panel accounts
-----------------------------------------------------------------
SELECT      SUM(weight.sky_base_accounts) AS sb_cat_total
            , SUM(weight.vespa_accounts) AS va_cat_total
INTO #index1_box
FROM        vespa_analysts.SC2_weightings AS weight
                        INNER JOIN  vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings);

-----------------------------------------------------------------
--Divide Sky base & Vespa panel accounts per segment category by
--total accounts from the previous query, to get percent to total
-----------------------------------------------------------------
SELECT      lookup.boxtype AS category
            , SUM(weight.sky_base_accounts) / in1.sb_cat_total AS sb_index
            , SUM(weight.vespa_accounts) / in1.va_cat_total AS va_index
INTO #index2_box
FROM        #index1_box in1, vespa_analysts.SC2_weightings AS weight
                        INNER JOIN vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup
                        ON    weight.scaling_segment_id = lookup.scaling_segment_id
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.boxtype, in1.sb_cat_total, in1.va_cat_total;

-----------------------------------------------------------------
--Present the data in segments and categories, with Sky base &
--Vespa account numbers, coverage and the index - by dividing by
--percent to total
-----------------------------------------------------------------
SELECT      6 AS Seg_num
            , 'Box Type' AS segment
            , lookup.boxtype AS category
            , SUM(weight.sky_base_accounts) AS Sky_base
            , SUM(weight.vespa_accounts) AS vespa_accounts
            , ROUND(SUM(weight.vespa_accounts*weighting),0) AS coverage
            , CAST(ROUND((in2.va_index / in2.sb_index) * 100,2) AS NUMERIC(8,2)) AS "index"
INTO #Box_Type
FROM        vespa_analysts.SC2_weightings AS weight
                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1 AS lookup ON weight.scaling_segment_id = lookup.scaling_segment_id
                        inner join #index2_box AS in2 ON lookup.boxtype = in2.category
WHERE       weight.scaling_day = (SELECT MAX(scaling_day) FROM vespa_analysts.SC2_weightings)
GROUP BY lookup.boxtype, in2.va_index, in2.sb_index
ORDER BY category ;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #index1_box;
DROP TABLE #index2_box;
-----------------------------------------------------------------
--Union all the segements and categories and put into a table
-----------------------------------------------------------------
SELECT * INTO Scaling_Variables FROM (
SELECT * FROM #HH_Box_Comp
UNION
SELECT * FROM #TV_Region
UNION
SELECT * FROM #HH_Composition
UNION
SELECT * FROM #Tenure
UNION
SELECT * FROM #Package
UNION
SELECT * FROM #Box_Type
) AS Scal_Var
;

-----------------------------------------------------------------
--Display the one table and order
-----------------------------------------------------------------
SELECT      segment
            , category
            , Sky_base
            , vespa_accounts
            , coverage
            , "index"
FROM Scaling_Variables
ORDER BY Seg_Num;

-----------------------------------------------------------------
--Drop the temporary tables
-----------------------------------------------------------------
DROP TABLE #HH_Box_Comp;
DROP TABLE #TV_Region;
DROP TABLE #HH_Composition;
DROP TABLE #Tenure;
DROP TABLE #Package;
DROP TABLE #Box_Type;
DROP TABLE #Scaling_Variables;



