

/**************************************************************************
 **  Build Temporal Roots defined by the library, that have not been     **
 **  made yet.                                                           **
 **  Should adapt this to have the option of building those not          **
 **  built AND a general refresh, just incase of any event back-filling  **
 **************************************************************************/

CREATE or replace procedure SEG01_build_temporal_roots_from_library_definitions(
                ) AS
BEGIN

    declare @temporal_id            bigint
    declare @currentBuildListCount  integer


    --difference btw 'to build' and 'built'
    select uniqid, dense_rank() over(order by uniqid desc) rank
      into #SEG01_temporal_lib_build_list_tmp
      from SEG01_temporal_library_tbl
     where uniqid not in (select r.temporal_library_id
                            from SEG01_root_temporal_tbl r
                        group by temporal_library_id )
  group by uniqid
    commit


    select @currentBuildListCount = count(1)
                                     from #SEG01_temporal_lib_build_list_tmp

    While(@currentBuildListCount > 0)
        BEGIN
            select @temporal_id = uniqid
              from #SEG01_temporal_lib_build_list_tmp
             where rank = @currentBuildListCount

            --update the temporal roots, all are based on viewing time for now....
            exec SEG01_create_temporal_root_aggregations @temporal_id, 1

            -- put some checks in here to make sure all the pk_viewing_instances have been tagged
            -- before moving on to the next ID.. at least make a log.. as this could be a disaster, if it
            -- falls over half-way through a batch run, then restarts at the following temporal_id

            --now delete the uniqid from the build list
            delete #SEG01_temporal_lib_build_list_tmp
             where uniqid = @temporal_id
            commit

            SET @currentBuildListCount = @currentBuildListCount - 1

        END
END;


