/*
        Purpose
        -------
        Create tables to support a server-destroying query example.

*/

--------------------------------------------------------------- B01 - scaling_weekly_sample
-- B01 - scaling_weekly_sample

-- The scaling_weekly_sample contains profile information for the accounts that
-- are being scaled. The profile information is re-calculated on a weekly basis.

IF object_id('scaling_weekly_sample') IS NOT NULL
    DROP TABLE scaling_weekly_sample;

CREATE TABLE scaling_weekly_sample (
     account_number                     VARCHAR(20)     primary key
    ,ilu_cb_row_id                      BIGINT
    ,universe                           VARCHAR(20)                         -- Single, Dual or Multiple box household
    ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
    ,ilu_hhcomposition                  VARCHAR(2)
    ,hhcomposition                      VARCHAR(70)     DEFAULT 'L) Unknown'-- Scaling variable 2: Household composition
    ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure
    ,num_mix                            INT
    ,mix_pack                           VARCHAR(20)
    ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
    ,boxtype                            VARCHAR(35)                         -- Scaling variable 5: Household boxtype (ranked)
    ,scaling_segment_id                 INT             DEFAULT NULL        -- segment scaling id for identifying segments
    ,mr_boxes                           INT
    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
);

CREATE INDEX for_segment_identification_raw ON scaling_weekly_sample
    (universe, isba_tv_region,hhcomposition, tenure, package, boxtype);
CREATE INDEX ilu_joining ON scaling_weekly_sample (ilu_cb_row_id);
CREATE INDEX for_grouping ON scaling_weekly_sample (scaling_segment_ID);
COMMIT;
go

--------------------------------------------------------------- C03 - Scaling_box_level_viewing
-- C03 - Scaling_box_level_viewing

IF object_id('Scaling_box_level_viewing') IS NOT NULL
    DROP TABLE Scaling_box_level_viewing;

CREATE TABLE Scaling_box_level_viewing (
    service_instance_id                 varchar(30)
    ,account_number                     varchar(20)
    ,universe                           varchar(30)
    ,viewing_flag                       tinyint
    ,MR                                 tinyint
    ,SP                                 tinyint
    ,HD                                 tinyint
    ,HDstb                              tinyint
    ,HD1TBstb                           tinyint
);

CREATE UNIQUE hg INDEX indx_ac ON Scaling_box_level_viewing(service_instance_id);
CREATE hg INDEX indx_serv_inst_id ON Scaling_box_level_viewing(account_number);
COMMIT;
go

---------------------------------------------------------------     - DATA CLEANSING

-- OK, and as we're shipping these tables to Sybase as well, we need identical versions with
-- cleansed table names which we'll populate with cleansed values.
--      scaling_weekly_sample       -> table1
--      Scaling_box_level_viewing   -> table2
--      account_number              -> key1
--      service_instance_id         -> key2
-- Refer to the other spreadsheet for column name mappings, they basically
-- just get mapped to flag* or flag** based on the order in the table.

--------------------------------------------------------------- table1

IF object_id('table1') IS NOT NULL
    DROP TABLE table1;

CREATE TABLE table1 (
     key1               VARCHAR(20)     primary key
    ,flag01             BIGINT
    ,flag02             VARCHAR(20)
    ,flag03             VARCHAR(20)
    ,flag04             VARCHAR(2)
    ,flag05             VARCHAR(70)     DEFAULT 'L) Unknown'
    ,flag06             VARCHAR(15)     DEFAULT 'E) Unknown'
    ,flag07             INT
    ,flag08             VARCHAR(20)
    ,flag09             VARCHAR(20)
    ,flag10             VARCHAR(35)
    ,flag11             INT             DEFAULT NULL
    ,flag12             INT
    ,flag13             TINYINT         DEFAULT 0
);

CREATE              INDEX index_1 ON table1 (flag02, flag03, flag05, flag06, flag09, flag10);
CREATE              INDEX index_2 ON table1 (flag01);
CREATE              INDEX index_3 ON table1 (flag11);
COMMIT;
go

--------------------------------------------------------------- table2

IF object_id('table2') IS NOT NULL
    DROP TABLE table2;

CREATE TABLE table2 (
    key2                varchar(30)
    ,key1               varchar(20)
    ,flag1              varchar(30)
    ,flag2              tinyint
    ,flag3              tinyint
    ,flag4              tinyint
    ,flag5              tinyint
    ,flag6              tinyint
    ,flag7              tinyint
);                                      

CREATE UNIQUE hg    INDEX index_1   ON table2(key2);
CREATE hg           INDEX index_2   ON table2(key1);
COMMIT;
go

-- Because we have to pull them out and export them, someone else might do that...
grant select on table1 to public;
grant select on table2 to public;


-- OK, plus now here an additional thing about tracking timings and completeness and stuff,
-- because we want to know how fr we get in each build

IF object_id('crash_tracking') IS NOT NULL
    DROP TABLE crash_tracking;
    
create table crash_tracking (
    id                  bigint identity primary key
    ,flag_name          varchar(100)
    ,recorded_at        datetime default now()
);

commit;