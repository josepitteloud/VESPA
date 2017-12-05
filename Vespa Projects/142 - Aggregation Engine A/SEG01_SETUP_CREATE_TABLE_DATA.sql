


/**************************************************************************
 **                                                                      **
 **  Set-up the Aggregation Engine environment                           **
 **                                                                      **
 **************************************************************************/



CREATE or replace procedure SEG01_setup_create_table_data_proc( ) AS
BEGIN



-- #########
-- ##     ##
-- ##  1  ##
-- ##     ##
-- #########


/****************************************
 ***    Compulsory information         **
 ****************************************/


    -- run the procedure for a few parameters
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'genre_description'
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'sub_genre_description'
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'channel_name'
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'playback_type'
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'playback_speed'
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'duration' --used later for highlighting this <duration> in calculations
    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'pay_free_indicator'

    exec SEG01_define_segment 'olive_prod', 'sk_prod', null, '%event%', 'panel_id'

    exec SEG01_define_segment 'olive_prod', 'bednaszs', VAggr_02_Channel_Mapping, null, 'format'
    exec SEG01_define_segment 'olive_prod', 'vespa_shared', Aggr_Account_Attributes, null, 'days_data_returned'
    exec SEG01_define_segment 'olive_prod', 'vespa_shared', Aggr_Account_Attributes, null, 'ent_tv3d_sub'
    exec SEG01_define_segment 'olive_prod', 'vespa_shared', Aggr_Account_Attributes, null, 'movmt_tv3d_sub'



    --## testing ##
    /*
    --useful tables

    select *
    from  seg01_log_tbl

    select *
    from SEG01_Segment_Dictionary_Tag_Types_tbl

    select *
    from SEG01_Segment_Dictionary_tbl


    --this is unique on tag_type_uid, tag_value_uid
    SELECT  *
      from SEG01_Segment_Dictionary_tbl d, SEG01_Segment_Dictionary_Tag_Types_tbl t
     where d.tag_type_uid = t.uniqid

    */
    -- end testing
    -- ** --------- ** --------- ** --------- ** --------- ** --------- ** ---------




    /*******************************************************************************************************************
     **  Define tables for classifying tags so that the Engine is aware what they are related to.                     **
     **       ie. being 'self-aware'                                                                                  **
     **                                                                                                               **
     **  Related to viewing sources, metrics, temporal etc...                                                         **
     *******************************************************************************************************************/



    --execute the procedure to fill table with some values
    exec SEG01_define_selfaware 'olive_prod', 'barbera', 'samplesegintobau', null, 'duration'/*the column*/, 'viewing'/*awareness*/

    -- at the moment 'root' is being used for any dimension that can be related back to the pk_viewing_prog_instance_fact for creation of root segmentations.
    exec SEG01_define_selfaware 'olive_prod', 'barbera', 'samplesegintobau', null, 'duration'/*the column*/, 'root'/*awareness*/
    exec SEG01_define_selfaware 'olive_prod', 'barbera', 'samplesegintobau', null, 'playback_speed'/*the column*/, 'root'/*awareness*/
    exec SEG01_define_selfaware 'olive_prod', 'barbera', 'samplesegintobau', null, 'sub_genre_description'/*the column*/, 'root'/*awareness*/

    exec SEG01_define_selfaware 'olive_prod', 'sk_prod', null, '%event%', 'panel_id'/*the column*/, 'root'/*awareness*/

    exec SEG01_define_selfaware 'olive_prod', 'bednaszs', VAggr_02_Channel_Mapping, null,   'format', 'root'/*awareness*/
    exec SEG01_define_selfaware 'olive_prod', 'vespa_shared', Aggr_Account_Attributes, null, 'days_data_returned', 'root'/*awareness*/
    exec SEG01_define_selfaware 'olive_prod', 'vespa_shared', Aggr_Account_Attributes, null, 'ent_tv3d_sub', 'root'/*awareness*/
    exec SEG01_define_selfaware 'olive_prod', 'vespa_shared', Aggr_Account_Attributes, null, 'movmt_tv3d_sub', 'root'/*awareness*/

    commit


    --- test
    /*
    select top 100 *
      from SEG01_Tag_Self_Aware_tbl;

    select *
    from SEG01_Segment_Dictionary_Tag_Types_tbl

    -- test - add all the bits together....

    select top 100 *
      from SEG01_Table_Association_tbl a;

    SELECT  *
      from  SEG01_Segment_Dictionary_tbl d,
            SEG01_Segment_Dictionary_Tag_Types_tbl t,
            SEG01_Segment_Dictionary_Tag_States_tbl s
     where d.tag_type_uid = t.uniqid
       and s.tag_type_uid = t.uniqid

    */
    ------ end test
    -- ** --------- ** --------- ** --------- ** --------- ** --------- ** ---------



    /****************************************************************************************
     **  CREATE the EVENT TABLE LIBRARY
     **     need to know where all the viewing events are stored at this point
     **     so let's create a library. This creates table <SEG01_viewed_dp_event_table_summary_tbl>
     **
     ****************************************************************************************/

    exec SEG01_create_prog_event_table_library




    /************************************
     **    BUILD the METRIC LIBRARY    **
     ************************************/

    --need a library with a rule name, and code to describe what the engine has to do
    execute SEG01_build_default_metric_library_proc


    --test the created table
    /*
    select *
    from SEG01_metric_library_tbl
    */



    /********************************************************************
     **    Now what if restriction is on account - such as package     **
     ********************************************************************/

    -- construct a group (aggregation_universe) at the account level.











-- #########
-- ##     ##
-- ##  2  ##
-- ##     ##
-- #########


/***********************************************************
 ***      Information for future Engine development       **
 ***********************************************************/




    /******************************************************************
      *  Now - the dictionary is for handling categorical data, but
      *  what about continuous data where filtering can be acheived
      *  using <> or 'between' type functions?
      *
      *  The sensible default for this should be varchar = discrete, int = continuous.
      *  A sample should be taken of the data to find out how many attributes
      *  belong to the variable. If for example the tag_type is an INT, but there are
      *  only 2 attributes (0 & 1) then the variable is binary, so should be classified
      *  as discrete, rather than continuous.
      **********************************************************************************/

    exec SEG01_assign_default_tag_states

    --test
    /*
    select *
    from SEG01_Segment_Dictionary_Tag_States_tbl;

    --test with the other information tables
    SELECT  *
      from  SEG01_Segment_Dictionary_tbl d,
            SEG01_Segment_Dictionary_Tag_Types_tbl t,
            SEG01_Segment_Dictionary_Tag_States_tbl s
     where d.tag_type_uid = t.uniqid
       and s.tag_type_uid = t.uniqid
    */
    -- end test
    -- ** --------- ** --------- ** --------- ** --------- ** --------- ** ---------





    /****************************************************************************************
     **  To automatically construct queries we need to know how tables relate to each other.
     **  We need to define the relationships that exist between them.
     **  The following procedure defines how table columns relate to each other.
     ****************************************************************************************/

    --fill table examples
    --exec SEG01_define_association 'olive_prod', 'barbera', 'samplesegintobau', 'account_number'/*the column*/, 'account_number'/*the type of association*/
    --exec SEG01_define_association 'olive_prod', 'barbera', 'samplesegintobau', 'cb_key_household'/*the column*/, 'cb_key_household'/*the type of association*/

    exec SEG01_define_association 'olive_prod', 'sk_prod', 'experian_consumerview', 'cb_key_household'/*the column*/, 'cb_key_household'/*the type of association*/

    --exec SEG01_define_association 'olive_prod', 'barbera', 'samplesegintobau', 'pk_viewing_prog_instance_fact'/*the column*/, 'unique_viewing_instance'/*the type of association*/

    --- test
    /*
    select top 100 *
      from SEG01_Table_Association_tbl;
    */
    ------ end test





END;

commit



