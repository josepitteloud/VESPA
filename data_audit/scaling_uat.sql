---------------
--Scaling UAT--
---------------
/*
  Steps:

  Import the test Vespa panel into a table called brim
  Import the test Sky Base into a table called    brim_base
  Add a unique segemnt ID for the segments that aren't in Brim.
  Run the iterative loop
*/

    alter view brim_base as
      select * from brim_base_FINAL1
       where universe_key = 'Single Box Household Universe';
    --Single Box Household Universe
    --Dual Box Household Universe
    --3PLUS Box Household Universe

    alter view brim_sample as
      select * from brim_sample_FINAL1
       where universe_key = 'Single Box Household Universe';


    -- CBI version
    alter view SC2_variables_lookup as
    select * from
       SC2_variables_lookup8;




    drop table SC2_Segments_lookup;
    CREATE TABLE SC2_Segments_lookup (
     "scaling_segment_ID" bigint ,
     "universe"           varchar(20) DEFAULT NULL,
     "isba_tv_region"     varchar(20) DEFAULT NULL,
     "hhcomposition"      varchar(50) DEFAULT NULL,
     "tenure"             varchar(20) DEFAULT NULL,
     "package"            varchar(50) DEFAULT NULL,
     "boxtype"            varchar(30) DEFAULT NULL,
     "scaling_segment_name" varchar(500) DEFAULT NULL
    )
    ;

    truncate table SC2_Segments_lookup;
    insert into SC2_Segments_lookup(
     "scaling_segment_ID"
    , "universe"
    , "isba_tv_region"
    , "hhcomposition"
    , "tenure"
    , "package"
    , "boxtype"
    , "scaling_segment_name"
    )
    select scaling_segment_id
    , null as universe
    , isba_tv_region
    , hhcomposition
    , tenure
    , package
    , boxtype
    , lookup
    from brim_base
    ;


     create variable @cntr           INT;
     create variable @iteration      INT;
     create variable @cntr_var       SMALLINT;
     create variable @scaling_var    VARCHAR(30);
     create variable @convergence    TINYINT;
     create variable @sky_base       DOUBLE;
     create variable @vespa_panel    DOUBLE;
     create variable @sum_of_weights DOUBLE;
     create variable @profiling_date date;
     create variable @QA_catcher     bigint;

     truncate table SC2_weighting_working_table;

     INSERT INTO SC2_weighting_working_table (scaling_segment_id, sky_base_accounts)
     select scaling_segment_id, sky_base_accounts
     from brim_base;

     -- Now tack on the universe flags; a special case of things coming out of the lookup
     --they are all the same in our test case
     update SC2_weighting_working_table
     set universe = null;

     -- Mix in the Vespa panel counts as determined earlier
     update SC2_weighting_working_table as bas
     set vespa_panel = brim.attribute_sample
     from brim_sample as brim
     where bas.scaling_segment_id = cast(brim.scaling_segment_id as bigint);

     SET @cntr           = 1;
     SET @iteration      = 0;
     SET @cntr_var       = 1;
     SET @scaling_var    = (SELECT scaling_variable FROM SC2_Variables_lookup WHERE id = @cntr);

     -- arbitrary value to ensure convergence
     update SC2_weighting_working_table
     set vespa_panel = 0.000001
     where vespa_panel = 0;

     -- Initialise working columns
     update SC2_weighting_working_table
     set sum_of_weights = vespa_panel;

     -- The iterative part.
     -- This works by choosing a particular scaling variable and then summing across the categories
     -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- for that category.
     -- This category weight is then applied back to the segments table and the process repeats until
     -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- base category subtotal within 100 iterations.
     -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- The @convergence variable represents how many categories did not converge.
     -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- has not converged for this particular day.
     -- In this scenario, the person running the code should send the results of the SC2_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

    if object_id('brim_temp_iterations') is not null drop table brim_temp_iterations;
    create table brim_temp_iterations(cow int identity, iteration smallint, convergence int);


    WHILE @cntr < 6
    BEGIN
            DELETE FROM SC2_category_working_table
            SET @cntr_var = 1
            WHILE @cntr_var < 6
                BEGIN
                        SELECT @scaling_var = scaling_variable FROM SC2_Variables_lookup WHERE id = @cntr_var

                        EXECUTE('
                        INSERT INTO SC2_category_working_table (universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT srs.universe
                                   ,@scaling_var
                                   ,ssl.' || @scaling_var || '
                                   ,SUM(srs.sky_base_accounts)
                                   ,SUM(srs.vespa_panel)
                                   ,SUM(srs.sum_of_weights)
                               FROM SC2_weighting_working_table AS srs
                                    inner join SC2_Segments_lookup AS ssl ON srs.scaling_segment_id = cast(ssl.scaling_segment_id as bigint)
                           GROUP BY srs.universe,ssl.' || @scaling_var || '
                           ORDER BY srs.universe
                        ')

                        SET @cntr_var = @cntr_var + 1
                END

            commit

            UPDATE SC2_category_working_table
            SET  category_weight = sky_base_accounts / sum_of_weights
                ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

            SELECT @convergence = SUM(convergence_flag) FROM SC2_category_working_table
            SET @iteration = @iteration + 1
            SELECT @scaling_var = scaling_variable FROM SC2_Variables_lookup WHERE id = @cntr

            EXECUTE('
            UPDATE SC2_weighting_working_table
               SET  wgh.category_weight = sc.category_weight
                   ,wgh.sum_of_weights  = wgh.sum_of_weights * sc.category_weight
              FROM SC2_WEIGHTING_WORKING_TABLE wgh inner join BRIM_SAMPLE ssl on wgh.scaling_segment_id = cast(ssl.scaling_segment_id as bigint)
                   left join SC2_CATEGORY_WORKING_TABLE sc ON ssl.' || @scaling_var || ' = sc.value
            ')
            commit

            --if @iteration <= 5
            --   begin
            --       execute(' select @iteration as Iteration, a.* into SC2_weighting_working_table_ITER_' || @iteration || ' from SC2_weighting_working_table a ')
            --       commit
            --   end

            IF @iteration = 100 OR @convergence = 0 SET @cntr = 6
            ELSE
                 IF @cntr = 5  SET @cntr = 1
                 ELSE
                      SET @cntr = @cntr+1
                 --END IF
            --END IF

           insert into brim_temp_iterations(iteration, convergence) select @iteration, @convergence
           commit
    END;

commit;

--select 'Final' as Iteration, a.* into SC2_weighting_working_table_ITER_FINAL from SC2_weighting_working_table a
--commit;



--Results
select
    1 as Sort_Order,
    'HH Composition' as Dimension,
    b.hhcomposition as Attribute,
    sum(a.sum_of_weights) as SkyIq_Scaled
  from SC2_weighting_working_table a,
       SC2_Segments_lookup b
 where a.scaling_segment_id = b.scaling_segment_id
 group by Attribute

 union all

select
    2 as Sort_Order,
    'Box type' as Dimension,
    b.boxtype as Attribute,
    sum(sum_of_weights) as SkyIq_Scaled
  from SC2_weighting_working_table a,
       SC2_Segments_lookup b
 where a.scaling_segment_id = b.scaling_segment_id
 group by Attribute

 union all

select
    3 as Sort_Order,
    'TV Region' as Dimension,
    b.isba_tv_region as Attribute,
    sum(sum_of_weights) as SkyIq_Scaled
  from SC2_weighting_working_table a,
       SC2_Segments_lookup b
 where a.scaling_segment_id = b.scaling_segment_id
 group by Attribute

 union all

select
    4 as Sort_Order,
    'Package' as Dimension,
    b.package as Attribute,
    sum(sum_of_weights) as SkyIq_Scaled
  from SC2_weighting_working_table a,
       SC2_Segments_lookup b
 where a.scaling_segment_id = b.scaling_segment_id
 group by Attribute

 union all

select
    5 as Sort_Order,
    'Tenure' as Dimension,
    b.tenure as Attribute,
    sum(sum_of_weights) as SkyIq_Scaled
  from SC2_weighting_working_table a,
       SC2_Segments_lookup b
 where a.scaling_segment_id = b.scaling_segment_id
 group by Attribute

 order by 1, 2, 3;









/*

drop table brim_base_FINAL1;
create table brim_base_FINAL1 (
      lookup            varchar(250),
      hhcomposition     varchar(100),
      isba_tv_region    varchar(100),
      package           varchar(100),
      boxtype           varchar(100),
      tenure            varchar(100),
      universe_key      varchar(100),
      sky_base_accounts int,
      Scaling_Segment_ID bigint
  );

create hg index idx1 on brim_base_FINAL1(Scaling_Segment_ID);
create hg index idx2 on brim_base_FINAL1(lookup);


delete from brim_base_FINAL1;
commit;
input into brim_base_FINAL1
 from 'C:\_Playpen_\2012-10-24 VIQ CBI UAT\Scaling\Scaling UAT data 2012-11-23 sample base.csv' format ascii;
commit;


delete from brim_base_FINAL1;
commit;
load table brim_base_FINAL1
(
	lookup',',
	hhcomposition',',
	isba_tv_region',',
	package',',
	boxtype',',
	tenure',',
	universe_key',',
	sky_base_accounts',',
	Scaling_Segment_ID'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/2012-11-23 Scaling UAT - Data 02 (base).csv'
QUOTES ON
ESCAPES OFF
NOTIFY 10000
DELIMITED BY ','
;



drop table brim_sample_FINAL1;
create table brim_sample_FINAL1 (
      lookup            varchar(250),
      ADJUSTED_EVENT_START_DATE_LOCAL varchar(100),
      NK_HOUSEHOLD_DIM varchar(100),
      hhcomposition     varchar(100),
      isba_tv_region    varchar(100),
      package           varchar(100),
      boxtype           varchar(100),
      tenure            varchar(100),
      universe_key      varchar(256),
      Scaling_Segment_ID bigint,
      attribute_sample  int,
      calculated_scaling_weight varchar(100)

  );

create hg index idx1 on brim_sample_FINAL1(Scaling_Segment_ID);
create hg index idx2 on brim_sample_FINAL1(lookup);

input into brim_sample_FINAL1
 from 'C:\_Playpen_\2012-10-24 VIQ CBI UAT\Scaling\Scaling UAT data 2012-11-21 sample sample.csv' format ascii;
commit;

delete from brim_sample_FINAL1;
commit;
load table brim_sample_FINAL1
(
	lookup',',
	ADJUSTED_EVENT_START_DATE_LOCAL',',
	NK_HOUSEHOLD_DIM',',
	hhcomposition',',
	isba_tv_region',',
	package',',
	boxtype',',
	tenure',',
	universe_key',',
	Scaling_Segment_ID',',
	attribute_sample',',
	calculated_scaling_weight'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/2012-11-22 Scaling UAT - Data 02 (sample).csv'
QUOTES ON
ESCAPES OFF
NOTIFY 10000
DELIMITED BY ','
;





drop view if exists brim_base;
create view brim_base as
  select * from brim_base_FINAL1;

drop view if exists brim_sample;
create view brim_sample as
  select * from brim_sample_FINAL1;



-- ###################################################################################
-- ###################################################################################
-- ###################################################################################


drop table brim_base_2;
create table brim_base_2 (
      lookup            varchar(256),
      hhcomposition     varchar(100),
      isba_tv_region    varchar(100),
      package           varchar(100),
      boxtype           varchar(100),
      tenure            varchar(100),
      sky_base_accounts int,
      Scaling_Segment_ID bigint
  );

create unique index idx1 on brim_base_2(Scaling_Segment_ID);

input into brim_base_2
 from 'D:\Temp\SBE\Scaling UAT data 2012-11-21 base.csv' format ascii;
commit;

input into brim_base_2
 from 'D:\Temp\SBE\Scaling UAT data 2012-11-21 base 2.csv' format ascii;
commit;


drop table brim_sample_2;
create table brim_sample_2 (
      lookup            varchar(256),
      ADJUSTED_EVENT_START_DATE_LOCAL varchar(100),
      NK_HOUSEHOLD_DIM varchar(100),
      hhcomposition     varchar(100),
      isba_tv_region    varchar(100),
      package           varchar(100),
      boxtype           varchar(100),
      tenure            varchar(100),
      UNIVERSE_KEY      varchar(256),
      Scaling_Segment_ID bigint,
      attribute_sample  int,
      calculated_scaling_weight varchar(100)

  );

create unique index idx1 on brim_sample_2(Scaling_Segment_ID);

input into brim_sample_2
 from 'D:\Temp\SBE\Scaling UAT data 2012-11-21 sample.csv' format ascii;
commit;


*/









