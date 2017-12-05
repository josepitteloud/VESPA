              drop table temp_Waterfall_Base;
              drop table temp_waterfall_box_rules;
              drop table temp_Ambiguous_Sub_Ids;
              drop table temp_lastcall;
              drop table temp_dl_by_box;
              drop table temp_darwin;
              drop table temp_waterfall_box_base_accounts;

              drop table temp_PanBal_weekly_sample;
              drop table temp_PanBal_segments_lookup;
              drop table temp_matches;
              drop table temp_weekly_sample;
              drop table temp_cv_pp;
              drop table temp_cv_keys;
              drop table temp_roi_region;
              drop table temp_scaling_box_level_viewing;
              drop table temp_onnet_patch;
              drop table temp_bpe;
              drop table temp_p2e;
              drop table temp_onnet_lookup;
              drop table temp_noconsent;
              drop table temp_adsmbl;
              drop table temp_ccs;
              drop table temp_sky_box_count;
              drop table temp_PanBal_segments_lookup_unnormalised;

              drop table temp_panbal_SAV;
              drop table temp_dupes;
              drop table temp_dl;
              drop table temp_box_count;
              drop table temp_rq;

              drop table temp_PanBal_all_aggregated_results;
              drop table temp_panel_households;
              drop table temp_primary_panel_pool;
              drop table temp_PanBal_panel;
              drop table temp_PanBal_Scaling_Segment_Profiling;
              drop table temp_PanBal_traffic_lights;
              drop table temp_descrs;
              drop table temp_panel_segmentation;
              drop table temp_new_adds;
              drop table temp_segment_THIs;
              drop table temp_panbal_segments_lookup_normalised;
              drop table temp_lookup;

              drop table temp_lookup;
              drop table temp_list1_rq;
              drop table temp_panels;
              drop table temp_reqd;
              drop table temp_available;

              drop table temp_PanBal_all_aggregated_results;
              drop table temp_panel_households;
              drop table temp_PanBal_panel;
              drop table temp_PanBal_Scaling_Segment_Profiling;
              drop table temp_panel_segmentation;
              drop table temp_ess2;

  insert into panbal_run_log(run_time
                            ,task
                            ,notes
                             )

  select now()
        ,'Manually set to run finished'
        ,''

