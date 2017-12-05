--Breakdown of Segments --Version 1

---Segment 1

select     Viewing_Type_Detailed
          ,EVENT_START_DOW
          ,event_start_hour
          ,Number_Events
          ,Difference_capping_Duration
          ,Difference_Ntiles_Threshold
          ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
         into Seg_1_breaks
        from Seg1_Comparison_v1
        group by   Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold
--504 Row(s) affected
---Segment 2
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_2_breaks
        from Seg2_Comparison_v1
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--1590 Row(s) affected


---Segment 3

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_3_breaks
        from Seg3_Comparison_v1
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,box_subscription
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--9,625 Row(s) affected

--Breakdown of Segments --Version 2

---Segment 1

select     Viewing_Type_Detailed
          ,EVENT_START_DOW
          ,event_start_hour
          ,Number_Events
          ,Difference_capping_Duration
          ,Difference_Ntiles_Threshold
          ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
         into Seg_1_breaks_v2
        from Seg1_Comparison_v2
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold
--504 Row(s) affected

---Segment 2
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_2_breaks_v2
        from Seg2_Comparison_v2
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--1590 Row(s) affected
---Segment 3

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_3_breaks_v2
        from Seg3_Comparison_v2
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,box_subscription
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--9625 Row(s) affected


--Breakdown of Segments --Version 3

---Segment 1

select     Viewing_Type_Detailed
          ,EVENT_START_DOW
          ,event_start_hour
          ,Number_Events
          ,Difference_capping_Duration
          ,Difference_Ntiles_Threshold
          ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
         into  Seg_1_breaks_v3
        from Seg1_Comparison_v3
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold
--504 Row(s) affected
---Segment 2
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_2_breaks_v3
        from Seg2_Comparison_v3
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--1590 Row(s) affected

---Segment 3

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_3_breaks_v3
        from Seg3_Comparison_v3
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,box_subscription
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--9,625 Row(s) affected


--Breakdown of Segments --Version 4


---Segment 1

select     Viewing_Type_Detailed
          ,EVENT_START_DOW
          ,event_start_hour
          ,Number_Events
          ,Difference_capping_Duration
          ,Difference_Ntiles_Threshold
          ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
         into Seg_1_breaks_v4
        from Seg1_Comparison_v4
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold
--504 Row(s) affected
---Segment 2
select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_2_breaks_v4
        from Seg2_Comparison_v4
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold
--1590 Row(s) affected

---Segment 3

select   Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_description
        ,Number_Events
        ,Difference_capping_Duration
        ,Difference_Ntiles_Threshold
        ,case
            when Number_Events = 0 then 0
            when Number_Events between 1 and 9 then 1
            when Number_Events between 10 and 49 then 10
            when Number_Events between 50 and 99 then 50
            when Number_Events between 100 and 199 then 100
            when Number_Events between 200 and 499 then 200
            when Number_Events between 500 and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events >= 100000 then 100000
        end as Number_Events_Intervals
        ,case
            when Difference_Capping_Duration <= -10000 then -10000
            when Difference_Capping_Duration  between -9999 and -1000 then -1000
            when Difference_Capping_Duration  between -999 and -500 then -500
            when Difference_Capping_Duration  between -499 and -200 then -200
            when Difference_Capping_Duration  between -199 and -100 then -100
            when Difference_Capping_Duration  between -99 and -50 then -50
            when Difference_Capping_Duration  between -49 and -10 then -10
            when Difference_Capping_Duration  between -9 and -1 then -1
            when Difference_Capping_Duration  = 0 then 0
            when Difference_Capping_Duration  between 1 and 9 then 1
            when Difference_Capping_Duration  between 10 and 49 then 10
            when Difference_Capping_Duration between 50 and 99 then 50
            when Difference_Capping_Duration between 100 and 199 then 100
            when Difference_Capping_Duration between 200 and 499 then 200
            when Difference_Capping_Duration between 500 and 999 then 500
            when Difference_Capping_Duration between 1000 and 9999 then 1000
            when Difference_Capping_Duration between 10000 and 99999 then 10000
            when Difference_Capping_Duration >= 100000 then 100000
         end as Difference_Capping_Duration_Intervals
         ,case
           when Difference_Ntiles_Threshold  <= -150 then -150
           when Difference_Ntiles_Threshold  between -149 and -140 then -140
           when Difference_Ntiles_Threshold  between -139 and -130 then -130
           when Difference_Ntiles_Threshold  between -129 and -120 then -120
           when Difference_Ntiles_Threshold  between -119 and -110 then -110
           when Difference_Ntiles_Threshold  between -109 and -100 then -100
           when Difference_Ntiles_Threshold  between -99 and -90 then -90
           when Difference_Ntiles_Threshold  between -89 and -80 then -80
           when Difference_Ntiles_Threshold  between -79 and -70 then -70
           when Difference_Ntiles_Threshold  between -69 and -60 then -60
           when Difference_Ntiles_Threshold  between -59 and -50 then -50
           when Difference_Ntiles_Threshold  between -49 and -40 then -40
           when Difference_Ntiles_Threshold  between -39 and -30 then -30
           when Difference_Ntiles_Threshold  between -29 and -20 then -20
           when Difference_Ntiles_Threshold  between -19 and -10 then -10
           when Difference_Ntiles_Threshold  between -9 and -1  then -1
           when Difference_Ntiles_Threshold  = 0 then 0
           when Difference_Ntiles_Threshold  between 1 and 9 then 1
           when Difference_Ntiles_Threshold  between 10 and 19 then 10
           when Difference_Ntiles_Threshold  between 20 and 29 then 20
           when Difference_Ntiles_Threshold  between 30 and 39 then 30
           when Difference_Ntiles_Threshold  between 40 and 49 then 40
           when Difference_Ntiles_Threshold  between 50 and 59 then 50
           when Difference_Ntiles_Threshold  between 60 and 69 then 60
           when Difference_Ntiles_Threshold  between 70 and 79 then 70
           when Difference_Ntiles_Threshold  between 80 and 89 then 80
           when Difference_Ntiles_Threshold  between 90 and 99 then 90
           when Difference_Ntiles_Threshold  between 100 and 109 then 100
           when Difference_Ntiles_Threshold  between 110 and 119 then 110
           when Difference_Ntiles_Threshold  between 120 and 129 then 120
           when Difference_Ntiles_Threshold  between 130 and 139 then 130
           when Difference_Ntiles_Threshold  between 140 and 149 then 140
           when Difference_Ntiles_Threshold  >= 150 then 150
         end as Difference_Ntiles_Threshold_Intervals
          into Seg_3_breaks_v4
        from Seg3_Comparison_v4
        group by  Viewing_Type_Detailed
                  ,EVENT_START_DOW
                  ,event_start_hour
                  ,box_subscription
                  ,pack_grp
                  ,genre_description
                  ,Number_Events
                  ,Difference_Capping_Duration
                  ,Difference_Ntiles_Threshold

--9,625 Row(s) affected












