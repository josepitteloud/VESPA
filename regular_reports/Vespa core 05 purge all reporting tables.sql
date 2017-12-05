-- Project Vespa Core Control 5: cleaning up all the reporting tables after reports have gone out
-- Now with slight robustness against procs not actually being there

if object_id('vespa_analysts.Purge_all_transient_report_tables') is not null
   drop procedure vespa_analysts.Purge_all_transient_report_tables;
create procedure vespa_analysts.Purge_all_transient_report_tables as
begin

    if object_id('vespa_analysts.OpDash_clear_transients') is not null
        execute vespa_analysts.OpDash_clear_transients
    if object_id('vespa_analysts.Dialback_clear_transients') is not null
        execute vespa_analysts.Dialback_clear_transients
    if object_id('vespa_analysts.SVD_clear_transients') is not null
        execute vespa_analysts.SVD_clear_transients
    if object_id('vespa_analysts.SkyView_Dialback_clear_transients') is not null
        execute vespa_analysts.SkyView_Dialback_clear_transients
    if object_id('vespa_analysts.WeStat_clear_transients') is not null
        execute vespa_analysts.WeStat_clear_transients
    if object_id('vespa_analysts.PanMan_clear_transients') is not null
        execute vespa_analysts.PanMan_clear_transients

end;

grant execute on Purge_all_transient_report_tables to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;