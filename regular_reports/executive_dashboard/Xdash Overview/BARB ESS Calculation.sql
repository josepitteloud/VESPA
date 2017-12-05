-----------------------------------------------------------------------------------------

/*
    to calculate the ESS for BARB I need to:

    1)  flag the head of households so we can get the figures on the right context when
        compared against the current vespa panel data we have

    2)  Sample for these individuals who are the head of households to get their weights

    3)  Calculate the ESS
*/

-----------------------------------------------------------------------------------------

-- 3)
select  count(distinct individuals)                                 as panel_size
        ,round((power(sum(theweight),2)/sum(power(theweight,2))),2) as ESS
        ,round((ESS/panel_size),2)                                  as RESS
from    (
            --2)
            select  weight.household_number||'-'||weight.person_number  as individuals
                    ,weight.Processing_Weight                           as theweight
            from    barb_weights    as weight
                    inner join  (
                                    -- 1)
                                    select  household_number    as house_id 
                                            ,person_number		as person
                                    from    BARB_PVF04_Individual_Member_Details
                                    where   date_valid_for_db1 = (select max(date_valid_for_db1) from BARB_PVF04_Individual_Member_Details)
                                    and     household_status in (4,2)
                                )   as hoh
                    on  weight.household_number = hoh.house_id
                    and weight.person_number    = hoh.person
        )   as base