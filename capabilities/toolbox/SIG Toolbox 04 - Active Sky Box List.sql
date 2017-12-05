 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                         OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
                                        Berwyn Cort      (Berwyn.Cort@SkyIQ.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):
**Sharepoint Folder:

**Business Brief:

        This toolbox unit is to return the records for each box (total number of boxes) listing accounts numbers and boxes.

**Modules:

**Sections:

--------------------------------------------------------------------------------------------------------------
*/

CREATE OR replace PROCEDURE sig_toolbox_04_Active_Sky_Box_List
AS
BEGIN


    --------------------
    -- ACTIVE STBOX LIST
    --------------------
    select  active_boxes.account_number
            ,active_boxes.service_instance_id
            ,active_boxes.x_model_number
            ,CASE   WHEN active_boxes.x_pvr_type ='PVR6'                                 THEN 1
                    WHEN active_boxes.x_pvr_type ='PVR5'                                 THEN 1
                    WHEN active_boxes.x_pvr_type ='PVR4' AND active_boxes.x_manufacturer = 'Samsung'  THEN 1
                    WHEN active_boxes.x_pvr_type ='PVR4' AND active_boxes.x_manufacturer = 'Pace'     THEN 1
                    ELSE 0
            END     AS Adsmart_flag
            ,active_boxes.x_pvr_type
            ,active_boxes.x_manufacturer
            ,active_boxes.x_box_type
            ,active_boxes.currency_code
            ,active_boxes.x_anytime_plus_enabled
            ,active_boxes.x_description
            ,active_boxes.x_personal_storage_capacity
    from    (
                -- Extracting Active Boxes per account (one line per box per account)
                select  *
                from    (
                           -- Ranking STB based on service instance id to dedupe the table
                            Select  account_number
                                    ,service_instance_id
                                    ,x_model_number
                                    ,x_pvr_type
                                    ,x_manufacturer
                                    ,x_box_type
                                    ,currency_code
                                    ,x_anytime_plus_enabled
                                    ,x_description
                                    ,x_personal_storage_capacity
                                    ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                            from    /*sk_prod.*/CUST_SET_TOP_BOX

                        )   as base
                where   active_flag = 1
            )   as active_boxes
    where   active_boxes.currency_code = 'GBP'


END;

COMMIT;
GRANT execute ON sig_toolbox_04_Active_Sky_Box_List TO vespa_group_low_security;
COMMIT;


