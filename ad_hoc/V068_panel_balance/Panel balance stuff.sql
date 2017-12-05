/******************************************************************************
**
** PROJECT VESPA: PANEL BALANCE
**
** Refer to 
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=68
**
** We still don't have box return data, nor timelines on when we will. Maybe we
** have something on panel membership? 
**
** We've segmented the Sky Base as of 2012-07-19, and set that aside in
**      stafforr.V068_sky_base_profiling
** For now, there's not a lot else we can do.
**
******************************************************************************/


/************ PART A : SEGMENTATION OF SKY BASE BY SCALING VARIABLES ***********/

-- We're grabbing the scaling build of 2012-07-19 and using that as our profiling date.

-- Copying the Scaling 2 table definition, with "SC2" replaced by "V068":

CREATE TABLE V068_sky_base_profiling (
     account_number                     VARCHAR(20)     primary key
    ,cb_key_household                   BIGINT          not null            -- Needed for VIQ interim solution
    ,cb_key_individual                  BIGINT          not null            -- For ConsumerView linkage
    ,consumerview_cb_row_id             BIGINT                              -- Not really needed for consumerview linkage, but whatever
    ,universe                           VARCHAR(20)                         -- Single, Dual or Multiple box household
    ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
    ,hhcomposition                      VARCHAR(2)      DEFAULT 'U'         -- Scaling variable 2: Household composition from Experian Consumerview
    ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure
    ,num_mix                            INT
    ,mix_pack                           VARCHAR(20)
    ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
    ,boxtype                            VARCHAR(35)                         -- Scaling variable 5: Household boxtype (ranked)
    ,scaling_segment_id                 INT             DEFAULT NULL        -- segment scaling id for identifying segments
    ,mr_boxes                           INT
--    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data - DISCONTINUED; now interfaces with scoring module via defined tables
);

CREATE INDEX for_segment_identification_raw ON V068_sky_base_profiling
    (universe, isba_tv_region,hhcomposition, tenure, package, boxtype);     -- Might it be this one guy? this index rebuild making everything super slow? But it should be going in as a single atomic commit... but on inserts, it still only takes 55 sec...
CREATE INDEX experian_joining ON V068_sky_base_profiling (consumerview_cb_row_id);
CREATE INDEX for_grouping ON V068_sky_base_profiling (scaling_segment_ID);

COMMIT;
go

-- We just ran the scaling build for 2012-07-09:
select count(1) from SC2_scaling_weekly_sample;
-- 9434629

select count(1) from SC2_Sky_base_segment_snapshots
where profiling_date = '2012-07-19';
-- 9434629

-- let's grab all of those details out and preserve them in the V068 table:
insert into V068_sky_base_profiling
select * from SC2_scaling_weekly_sample;
commit;
go

select count(1) from  V068_sky_base_profiling;
-- 9434629

select count(1) from  V068_sky_base_profiling
where scaling_segment_id is null;
-- 9434629

-- Other people might need to use this:
grant select on V068_sky_base_profiling to greenj, dbarnett, jacksons, stafforr, sarahm, gillh, rombaoad, louredaj, patelj, kinnairt;

-- Sweet, our scaling segmentation is complete and cached.

/************ PART B : DECIDING PANEL MEMBERSHIP AND DATA RETURN QUALITY ***********/

-- Yeah, we have no data on this yet, can't really do anything about it. Update:
-- we've got the data load handled elsewhere, and this file just handles the
-- segmentation preservation for the other week, and the main file stitches the
-- bits together.



