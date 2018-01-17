
/*  3.1 Add target and eligibility flags */

    ALTER TABLE cs_raw
    ADD (Up_BB             BIT DEFAULT 0 
        ,Up_Fibre          BIT DEFAULT 0 
        ,Regrade_Fibre     BIT DEFAULT 0 
        ,Up_Box_Sets       BIT DEFAULT 0 
        );

    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt','BB_UNLIMITED','Account_Number','Drop and Replace','Order_BB_UNLIMITED_Added_In_Next_30d');
    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt','BB_LITE','Account_Number','Drop and Replace','Order_BB_LITE_Added_In_Next_30d');
    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt','BB_FIBRE_CAP','Account_Number','Drop and Replace','Order_BB_FIBRE_CAP_Added_In_Next_30d');
    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt','BB_FIBRE_UNLIMITED','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d');
    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt','BB_FIBRE_UNLIMITED_PRO','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d');
    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt', 'FAMILY','Account_Number','Drop and Replace','Order_FAMILY_Added_In_next_30d');
    CALL Decisioning_procs.Add_Software_Orders('cs_raw','Base_Dt','HD_BASIC','Account_Number','Drop and Replace','Order_HD_BASIC_Added_In_next_30d');


/*  3.2 Create target flags */

    /*  3.2.1 Boxsets */

        UPDATE      cs_raw
        SET         Up_Box_Sets = 1
        WHERE       Order_HD_BASIC_Added_In_next_30d > 0;
       
    /*  3.2.2 Non-Fibre Broadband */

        UPDATE      cs_raw
        SET         Up_BB = 1
        WHERE       (Order_BB_UNLIMITED_Added_In_Next_30d > 0
        OR           Order_BB_LITE_Added_In_Next_30d      > 0);

    /*  3.2.3 Fibre Broadband */
   
        UPDATE      cs_raw
        SET         Up_Fibre = 1
        WHERE       (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
        OR           Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
        OR           Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0);

    /*  3.2.4 Regrade Fibre Broadband */
   
    /* Same as Fibre, but different due to eligibility criteria */
    
        UPDATE      cs_raw
        SET         Regrade_Fibre = 1
        WHERE       (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
        OR           Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
        OR           Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0);