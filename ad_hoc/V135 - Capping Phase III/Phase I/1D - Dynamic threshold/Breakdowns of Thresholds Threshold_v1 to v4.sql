/* Breakdown of the Thresholds for the Existing Capping where difference between the existing and New (Version 1 to 3 */

---Segment_1

select   GTS1.Viewing_Type_Detailed
        ,GTS1.EVENT_START_DOW
        ,GTS1.event_start_hour
        ,CTS1.Threshold_Curr
        ,GTS1.Threshold_Grad
        ,GTSN1.Threshold_Grad_v2
        ,GTVN1.Threshold_Grad_v3
         ,case
           when Threshold_Curr  between -39 and -30 then -30
           when Threshold_Curr  between -29 and -20 then -20
           when Threshold_Curr  between -19 and -10 then -10
           when Threshold_Curr  between -9 and -1 then -1
           when Threshold_Curr  between 0 and 9 then 0
           when Threshold_Curr  between 10 and 19 then 10
           when Threshold_Curr  between 20 and 29 then 20
           when Threshold_Curr  between 30 and 39 then 30
           when Threshold_Curr  between 40 and 49 then 40
           when Threshold_Curr  between 50 and 59 then 50
           when Threshold_Curr  between 60 and 69 then 60
           when Threshold_Curr  between 70 and 79 then 70
           when Threshold_Curr  between 80 and 89 then 80
           when Threshold_Curr  between 90 and 99 then 90
           when Threshold_Curr  between 100 and 109 then 100
           when Threshold_Curr  between 110 and 119 then 110
           when Threshold_Curr  between 120 and 129 then 120
           when Threshold_Curr  between 130 and 139 then 130
           when Threshold_Curr  between 140 and 149 then 140
           when Threshold_Curr  between 150 and 159 then 150
           when Threshold_Curr  between 160 and 169 then 160
           when Threshold_Curr  between 170 and 179 then 170
           when Threshold_Curr  between 180 and 189 then 180
           when Threshold_Curr  between 190 and 199 then 190
           when Threshold_Curr  = 200 then 200
         end as Threshold_Curr_Intervals
            ,case
           when Threshold_Grad  between 0 and 9 then 0
           when Threshold_Grad  between 10 and 19 then 10
           when Threshold_Grad  between 20 and 29 then 20
           when Threshold_Grad  between 30 and 39 then 30
           when Threshold_Grad  between 40 and 49 then 40
           when Threshold_Grad  between 50 and 59 then 50
           when Threshold_Grad  between 60 and 69 then 60
           when Threshold_Grad  between 70 and 79 then 70
           when Threshold_Grad  between 80 and 89 then 80
           when Threshold_Grad  between 90 and 99 then 90
           when Threshold_Grad  between 100 and 109 then 100
           when Threshold_Grad  between 110 and 119 then 110
           when Threshold_Grad  between 120 and 129 then 120
           when Threshold_Grad  between 130 and 139 then 130
           when Threshold_Grad  between 140 and 149 then 140
           when Threshold_Grad  between 150 and 159 then 150
           when Threshold_Grad  between 160 and 169 then 160
           when Threshold_Grad  between 170 and 179 then 170
           when Threshold_Grad  between 180 and 189 then 180
           when Threshold_Grad  between 190 and 199 then 190
           when Threshold_Grad  = 200 then 200
         end as Threshold_Grad_Intervals
        ,case
           when Threshold_Grad_v2 between 0 and 9 then 0
           when Threshold_Grad_v2 between 10 and 19 then 10
           when Threshold_Grad_v2 between 20 and 29 then 20
           when Threshold_Grad_v2 between 30 and 39 then 30
           when Threshold_Grad_v2 between 40 and 49 then 40
           when Threshold_Grad_v2 between 50 and 59 then 50
           when Threshold_Grad_v2 between 60 and 69 then 60
           when Threshold_Grad_v2  between 70 and 79 then 70
           when Threshold_Grad_v2  between 80 and 89 then 80
           when Threshold_Grad_v2  between 90 and 99 then 90
           when Threshold_Grad_v2  between 100 and 109 then 100
           when Threshold_Grad_v2  between 110 and 119 then 110
           when Threshold_Grad_v2  between 120 and 129 then 120
           when Threshold_Grad_v2  between 130 and 139 then 130
           when Threshold_Grad_v2  between 140 and 149 then 140
           when Threshold_Grad_v2  between 150 and 159 then 150
           when Threshold_Grad_v2  between 160 and 169 then 160
           when Threshold_Grad_v2  between 170 and 179 then 170
           when Threshold_Grad_v2  between 180 and 189 then 180
           when Threshold_Grad_v2  between 190 and 199 then 190
           when Threshold_Grad_v2  = 200 then 200
         end as Threshold_Grad_v2_Intervals
          ,case
           when Threshold_Grad_v3 between 0 and 9 then 0
           when Threshold_Grad_v3 between 10 and 19 then 10
           when Threshold_Grad_v3 between 20 and 29 then 20
           when Threshold_Grad_v3 between 30 and 39 then 30
           when Threshold_Grad_v3 between 40 and 49 then 40
           when Threshold_Grad_v3 between 50 and 59 then 50
           when Threshold_Grad_v3 between 60 and 69 then 60
           when Threshold_Grad_v3  between 70 and 79 then 70
           when Threshold_Grad_v3  between 80 and 89 then 80
           when Threshold_Grad_v3  between 90 and 99 then 90
           when Threshold_Grad_v3  between 100 and 109 then 100
           when Threshold_Grad_v3  between 110 and 119 then 110
           when Threshold_Grad_v3  between 120 and 129 then 120
           when Threshold_Grad_v3  between 130 and 139 then 130
           when Threshold_Grad_v3  between 140 and 149 then 140
           when Threshold_Grad_v3  between 150 and 159 then 150
           when Threshold_Grad_v3  between 160 and 169 then 160
           when Threshold_Grad_v3  between 170 and 179 then 170
           when Threshold_Grad_v3  between 180 and 189 then 180
           when Threshold_Grad_v3  between 190 and 199 then 190
           when Threshold_Grad_v3  = 200 then 200
         end as Threshold_Grad_v3_Intervals
         into First_Comparison_v1_to_v3
from     Current_Threshold_Seg1 as CTS1
inner join Gradient_Threshold_Seg1_v1 as GTS1
on GTS1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTS1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTS1.event_start_hour = CTS1.event_start_hour
inner join Gradient_Threshold_Seg1_v2 as GTSN1
on GTSN1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTSN1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTSN1.event_start_hour = CTS1.event_start_hour
inner join Gradient_Threshold_Seg1_v3 as GTVN1
on GTVN1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GTVN1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GTVN1.event_start_hour = CTS1.event_start_hour
--504 Row(s) affected


---Segment_2

select   GTS2.Viewing_Type_Detailed
        ,GTS2.EVENT_START_DOW
        ,GTS2.event_start_hour
        ,GTS2.pack_grp
        ,GTS2.genre_description
        ,CTS2.Threshold_Curr
        ,GTS2.Threshold_Grad
        ,GTSN2.Threshold_Grad_v2
        ,GTSV2.Threshold_Grad_v3
 ,case
           when Threshold_Curr  between -39 and -30 then -30
           when Threshold_Curr  between -29 and -20 then -20
           when Threshold_Curr  between -19 and -10 then -10
           when Threshold_Curr  between -9 and -1 then -1
           when Threshold_Curr  between 0 and 9 then 0
           when Threshold_Curr  between 10 and 19 then 10
           when Threshold_Curr  between 20 and 29 then 20
           when Threshold_Curr  between 30 and 39 then 30
           when Threshold_Curr  between 40 and 49 then 40
           when Threshold_Curr  between 50 and 59 then 50
           when Threshold_Curr  between 60 and 69 then 60
           when Threshold_Curr  between 70 and 79 then 70
           when Threshold_Curr  between 80 and 89 then 80
           when Threshold_Curr  between 90 and 99 then 90
           when Threshold_Curr  between 100 and 109 then 100
           when Threshold_Curr  between 110 and 119 then 110
           when Threshold_Curr  between 120 and 129 then 120
           when Threshold_Curr  between 130 and 139 then 130
           when Threshold_Curr  between 140 and 149 then 140
           when Threshold_Curr  between 150 and 159 then 150
           when Threshold_Curr  between 160 and 169 then 160
           when Threshold_Curr  between 170 and 179 then 170
           when Threshold_Curr  between 180 and 189 then 180
           when Threshold_Curr  between 190 and 199 then 190
           when Threshold_Curr  = 200 then 200
         end as Threshold_Curr_Intervals
            ,case
           when Threshold_Grad  between 0 and 9 then 0
           when Threshold_Grad  between 10 and 19 then 10
           when Threshold_Grad  between 20 and 29 then 20
           when Threshold_Grad  between 30 and 39 then 30
           when Threshold_Grad  between 40 and 49 then 40
           when Threshold_Grad  between 50 and 59 then 50
           when Threshold_Grad  between 60 and 69 then 60
           when Threshold_Grad  between 70 and 79 then 70
           when Threshold_Grad  between 80 and 89 then 80
           when Threshold_Grad  between 90 and 99 then 90
           when Threshold_Grad  between 100 and 109 then 100
           when Threshold_Grad  between 110 and 119 then 110
           when Threshold_Grad  between 120 and 129 then 120
           when Threshold_Grad  between 130 and 139 then 130
           when Threshold_Grad  between 140 and 149 then 140
           when Threshold_Grad  between 150 and 159 then 150
           when Threshold_Grad  between 160 and 169 then 160
           when Threshold_Grad  between 170 and 179 then 170
           when Threshold_Grad  between 180 and 189 then 180
           when Threshold_Grad  between 190 and 199 then 190
           when Threshold_Grad  = 200 then 200
         end as Threshold_Grad_Intervals
        ,case
           when Threshold_Grad_v2 between 0 and 9 then 0
           when Threshold_Grad_v2 between 10 and 19 then 10
           when Threshold_Grad_v2 between 20 and 29 then 20
           when Threshold_Grad_v2 between 30 and 39 then 30
           when Threshold_Grad_v2 between 40 and 49 then 40
           when Threshold_Grad_v2 between 50 and 59 then 50
           when Threshold_Grad_v2 between 60 and 69 then 60
           when Threshold_Grad_v2  between 70 and 79 then 70
           when Threshold_Grad_v2  between 80 and 89 then 80
           when Threshold_Grad_v2  between 90 and 99 then 90
           when Threshold_Grad_v2  between 100 and 109 then 100
           when Threshold_Grad_v2  between 110 and 119 then 110
           when Threshold_Grad_v2  between 120 and 129 then 120
           when Threshold_Grad_v2  between 130 and 139 then 130
           when Threshold_Grad_v2  between 140 and 149 then 140
           when Threshold_Grad_v2  between 150 and 159 then 150
           when Threshold_Grad_v2  between 160 and 169 then 160
           when Threshold_Grad_v2  between 170 and 179 then 170
           when Threshold_Grad_v2  between 180 and 189 then 180
           when Threshold_Grad_v2  between 190 and 199 then 190
           when Threshold_Grad_v2  = 200 then 200
         end as Threshold_Grad_v2_Intervals
          ,case
           when Threshold_Grad_v3 between 0 and 9 then 0
           when Threshold_Grad_v3 between 10 and 19 then 10
           when Threshold_Grad_v3 between 20 and 29 then 20
           when Threshold_Grad_v3 between 30 and 39 then 30
           when Threshold_Grad_v3 between 40 and 49 then 40
           when Threshold_Grad_v3 between 50 and 59 then 50
           when Threshold_Grad_v3 between 60 and 69 then 60
           when Threshold_Grad_v3  between 70 and 79 then 70
           when Threshold_Grad_v3  between 80 and 89 then 80
           when Threshold_Grad_v3  between 90 and 99 then 90
           when Threshold_Grad_v3  between 100 and 109 then 100
           when Threshold_Grad_v3  between 110 and 119 then 110
           when Threshold_Grad_v3  between 120 and 129 then 120
           when Threshold_Grad_v3  between 130 and 139 then 130
           when Threshold_Grad_v3  between 140 and 149 then 140
           when Threshold_Grad_v3  between 150 and 159 then 150
           when Threshold_Grad_v3  between 160 and 169 then 160
           when Threshold_Grad_v3  between 170 and 179 then 170
           when Threshold_Grad_v3  between 180 and 189 then 180
           when Threshold_Grad_v3  between 190 and 199 then 190
           when Threshold_Grad_v3  = 200 then 200
         end as Threshold_Grad_v3_Intervals
         into Second_Comparison_v1_to_v3
from     Current_Threshold_Seg2 as CTS2
inner join Gradient_Threshold_Seg2_v1 as GTS2
on GTS2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTS2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTS2.event_start_hour = CTS2.event_start_hour
and coalesce(GTS2.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(GTS2.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')
inner join Gradient_Threshold_Seg2_v2 as GTSN2
on GTSN2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTSN2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTSN2.event_start_hour = CTS2.event_start_hour
and coalesce(GTSN2.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(GTSN2.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')
inner join Gradient_Threshold_Seg2_v3 as GTSV2
on GTSV2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GTSV2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GTSV2.event_start_hour = CTS2.event_start_hour
and coalesce(GTSV2.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(GTSV2.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')
---1,590 Row(s) affected

---Segment_3

select   GTS3.Viewing_Type_Detailed
        ,GTS3.EVENT_START_DOW
        ,GTS3.event_start_hour
        ,GTS3.box_subscription
        ,GTS3.pack_grp
        ,GTS3.genre_description
        ,CTS3.Threshold_Curr
        ,GTS3.Threshold_Grad
        ,GTSN3.Threshold_Grad_v2
        ,GTSV3.Threshold_Grad_v3
 ,case
           when Threshold_Curr  between -39 and -30 then -30
           when Threshold_Curr  between -29 and -20 then -20
           when Threshold_Curr  between -19 and -10 then -10
           when Threshold_Curr  between -9 and -1 then -1
           when Threshold_Curr  between 0 and 9 then 0
           when Threshold_Curr  between 10 and 19 then 10
           when Threshold_Curr  between 20 and 29 then 20
           when Threshold_Curr  between 30 and 39 then 30
           when Threshold_Curr  between 40 and 49 then 40
           when Threshold_Curr  between 50 and 59 then 50
           when Threshold_Curr  between 60 and 69 then 60
           when Threshold_Curr  between 70 and 79 then 70
           when Threshold_Curr  between 80 and 89 then 80
           when Threshold_Curr  between 90 and 99 then 90
           when Threshold_Curr  between 100 and 109 then 100
           when Threshold_Curr  between 110 and 119 then 110
           when Threshold_Curr  between 120 and 129 then 120
           when Threshold_Curr  between 130 and 139 then 130
           when Threshold_Curr  between 140 and 149 then 140
           when Threshold_Curr  between 150 and 159 then 150
           when Threshold_Curr  between 160 and 169 then 160
           when Threshold_Curr  between 170 and 179 then 170
           when Threshold_Curr  between 180 and 189 then 180
           when Threshold_Curr  between 190 and 199 then 190
           when Threshold_Curr  = 200 then 200
         end as Threshold_Curr_Intervals
            ,case
           when Threshold_Grad  between 0 and 9 then 0
           when Threshold_Grad  between 10 and 19 then 10
           when Threshold_Grad  between 20 and 29 then 20
           when Threshold_Grad  between 30 and 39 then 30
           when Threshold_Grad  between 40 and 49 then 40
           when Threshold_Grad  between 50 and 59 then 50
           when Threshold_Grad  between 60 and 69 then 60
           when Threshold_Grad  between 70 and 79 then 70
           when Threshold_Grad  between 80 and 89 then 80
           when Threshold_Grad  between 90 and 99 then 90
           when Threshold_Grad  between 100 and 109 then 100
           when Threshold_Grad  between 110 and 119 then 110
           when Threshold_Grad  between 120 and 129 then 120
           when Threshold_Grad  between 130 and 139 then 130
           when Threshold_Grad  between 140 and 149 then 140
           when Threshold_Grad  between 150 and 159 then 150
           when Threshold_Grad  between 160 and 169 then 160
           when Threshold_Grad  between 170 and 179 then 170
           when Threshold_Grad  between 180 and 189 then 180
           when Threshold_Grad  between 190 and 199 then 190
           when Threshold_Grad  = 200 then 200
         end as Threshold_Grad_Intervals
        ,case
           when Threshold_Grad_v2 between 0 and 9 then 0
           when Threshold_Grad_v2 between 10 and 19 then 10
           when Threshold_Grad_v2 between 20 and 29 then 20
           when Threshold_Grad_v2 between 30 and 39 then 30
           when Threshold_Grad_v2 between 40 and 49 then 40
           when Threshold_Grad_v2 between 50 and 59 then 50
           when Threshold_Grad_v2 between 60 and 69 then 60
           when Threshold_Grad_v2  between 70 and 79 then 70
           when Threshold_Grad_v2  between 80 and 89 then 80
           when Threshold_Grad_v2  between 90 and 99 then 90
           when Threshold_Grad_v2  between 100 and 109 then 100
           when Threshold_Grad_v2  between 110 and 119 then 110
           when Threshold_Grad_v2  between 120 and 129 then 120
           when Threshold_Grad_v2  between 130 and 139 then 130
           when Threshold_Grad_v2  between 140 and 149 then 140
           when Threshold_Grad_v2  between 150 and 159 then 150
           when Threshold_Grad_v2  between 160 and 169 then 160
           when Threshold_Grad_v2  between 170 and 179 then 170
           when Threshold_Grad_v2  between 180 and 189 then 180
           when Threshold_Grad_v2  between 190 and 199 then 190
           when Threshold_Grad_v2  = 200 then 200
         end as Threshold_Grad_v2_Intervals
          ,case
           when Threshold_Grad_v3 between 0 and 9 then 0
           when Threshold_Grad_v3 between 10 and 19 then 10
           when Threshold_Grad_v3 between 20 and 29 then 20
           when Threshold_Grad_v3 between 30 and 39 then 30
           when Threshold_Grad_v3 between 40 and 49 then 40
           when Threshold_Grad_v3 between 50 and 59 then 50
           when Threshold_Grad_v3 between 60 and 69 then 60
           when Threshold_Grad_v3  between 70 and 79 then 70
           when Threshold_Grad_v3  between 80 and 89 then 80
           when Threshold_Grad_v3  between 90 and 99 then 90
           when Threshold_Grad_v3  between 100 and 109 then 100
           when Threshold_Grad_v3  between 110 and 119 then 110
           when Threshold_Grad_v3  between 120 and 129 then 120
           when Threshold_Grad_v3  between 130 and 139 then 130
           when Threshold_Grad_v3  between 140 and 149 then 140
           when Threshold_Grad_v3  between 150 and 159 then 150
           when Threshold_Grad_v3  between 160 and 169 then 160
           when Threshold_Grad_v3  between 170 and 179 then 170
           when Threshold_Grad_v3  between 180 and 189 then 180
           when Threshold_Grad_v3  between 190 and 199 then 190
           when Threshold_Grad_v3  = 200 then 200
         end as Threshold_Grad_v3_Intervals
         into Third_Comparison_v1_to_v3
from     Current_Threshold_Seg3 as CTS3
inner join Gradient_Threshold_Seg3_v1 as GTS3
on GTS3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTS3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTS3.event_start_hour = CTS3.event_start_hour
and GTS3.box_subscription = CTS3.box_subscription
and coalesce(GTS3.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(GTS3.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')
inner join Gradient_Threshold_Seg3_v2 as GTSN3
on GTSN3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTSN3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTSN3.event_start_hour = CTS3.event_start_hour
and GTSN3.box_subscription = CTS3.box_subscription
and coalesce(GTSN3.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(GTSN3.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')
inner join Gradient_Threshold_Seg3_v3 as GTSV3
on GTSV3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GTSV3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GTSV3.event_start_hour = CTS3.event_start_hour
and GTSV3.box_subscription = CTS3.box_subscription
and coalesce(GTSV3.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(GTSV3.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')

---9,625 Row(s) affected

---Version 4

/* Breakdown of the Thresholds for the Existing Capping and the new one--version 4 */

---Segment_1

select   CTS1.Viewing_Type_Detailed
        ,CTS1.EVENT_START_DOW
        ,CTS1.event_start_hour
        ,CTS1.Threshold_Curr
        ,GN1.Threshold_Grad_v4
         ,case
           when Threshold_Curr  between -39 and -30 then -30
           when Threshold_Curr  between -29 and -20 then -20
           when Threshold_Curr  between -19 and -10 then -10
           when Threshold_Curr  between -9 and -1 then -1
           when Threshold_Curr  between 0 and 9 then 0
           when Threshold_Curr  between 10 and 19 then 10
           when Threshold_Curr  between 20 and 29 then 20
           when Threshold_Curr  between 30 and 39 then 30
           when Threshold_Curr  between 40 and 49 then 40
           when Threshold_Curr  between 50 and 59 then 50
           when Threshold_Curr  between 60 and 69 then 60
           when Threshold_Curr  between 70 and 79 then 70
           when Threshold_Curr  between 80 and 89 then 80
           when Threshold_Curr  between 90 and 99 then 90
           when Threshold_Curr  between 100 and 109 then 100
           when Threshold_Curr  between 110 and 119 then 110
           when Threshold_Curr  between 120 and 129 then 120
           when Threshold_Curr  between 130 and 139 then 130
           when Threshold_Curr  between 140 and 149 then 140
           when Threshold_Curr  between 150 and 159 then 150
           when Threshold_Curr  between 160 and 169 then 160
           when Threshold_Curr  between 170 and 179 then 170
           when Threshold_Curr  between 180 and 189 then 180
           when Threshold_Curr  between 190 and 199 then 190
           when Threshold_Curr  = 200 then 200
         end as Threshold_Curr_Intervals
         , case
           when Threshold_Grad_v4 between 0 and 9 then 0
           when Threshold_Grad_v4 between 10 and 19 then 10
           when Threshold_Grad_v4 between 20 and 29 then 20
           when Threshold_Grad_v4 between 30 and 39 then 30
           when Threshold_Grad_v4 between 40 and 49 then 40
           when Threshold_Grad_v4 between 50 and 59 then 50
           when Threshold_Grad_v4 between 60 and 69 then 60
           when Threshold_Grad_v4  between 70 and 79 then 70
           when Threshold_Grad_v4  between 80 and 89 then 80
           when Threshold_Grad_v4  between 90 and 99 then 90
           when Threshold_Grad_v4  between 100 and 109 then 100
           when Threshold_Grad_v4  between 110 and 119 then 110
           when Threshold_Grad_v4  between 120 and 129 then 120
           when Threshold_Grad_v4  between 130 and 139 then 130
           when Threshold_Grad_v4  between 140 and 149 then 140
           when Threshold_Grad_v4  between 150 and 159 then 150
           when Threshold_Grad_v4  between 160 and 169 then 160
           when Threshold_Grad_v4  between 170 and 179 then 170
           when Threshold_Grad_v4  between 180 and 189 then 180
           when Threshold_Grad_v4  between 190 and 199 then 190
           when Threshold_Grad_v4  = 200 then 200
         end as Threshold_Grad_v4_Intervals
         into First_Comparison_v4
from     Current_Threshold_Seg1 as CTS1
inner join Gradient_Threshold_Seg1_v4 as GN1
on GN1.Viewing_Type_Detailed = CTS1.Viewing_Type_Detailed
and GN1.EVENT_START_DOW = CTS1.EVENT_START_DOW
and GN1.event_start_hour = CTS1.event_start_hour
--504 Row(s) affected


---Segment_2

select   CTS2.Viewing_Type_Detailed
        ,CTS2.EVENT_START_DOW
        ,CTS2.event_start_hour
        ,CTS2.pack_grp
        ,CTS2.genre_description
        ,CTS2.Threshold_Curr
        ,GN2.Threshold_Grad_V4
         ,case
           when Threshold_Curr  between -39 and -30 then -30
           when Threshold_Curr  between -29 and -20 then -20
           when Threshold_Curr  between -19 and -10 then -10
           when Threshold_Curr  between -9 and -1 then -1
           when Threshold_Curr  between 0 and 9 then 0
           when Threshold_Curr  between 10 and 19 then 10
           when Threshold_Curr  between 20 and 29 then 20
           when Threshold_Curr  between 30 and 39 then 30
           when Threshold_Curr  between 40 and 49 then 40
           when Threshold_Curr  between 50 and 59 then 50
           when Threshold_Curr  between 60 and 69 then 60
           when Threshold_Curr  between 70 and 79 then 70
           when Threshold_Curr  between 80 and 89 then 80
           when Threshold_Curr  between 90 and 99 then 90
           when Threshold_Curr  between 100 and 109 then 100
           when Threshold_Curr  between 110 and 119 then 110
           when Threshold_Curr  between 120 and 129 then 120
           when Threshold_Curr  between 130 and 139 then 130
           when Threshold_Curr  between 140 and 149 then 140
           when Threshold_Curr  between 150 and 159 then 150
           when Threshold_Curr  between 160 and 169 then 160
           when Threshold_Curr  between 170 and 179 then 170
           when Threshold_Curr  between 180 and 189 then 180
           when Threshold_Curr  between 190 and 199 then 190
           when Threshold_Curr  = 200 then 200
         end as Threshold_Curr_Intervals
         ,case
           when Threshold_Grad_v4 between 0 and 9 then 0
           when Threshold_Grad_v4 between 10 and 19 then 10
           when Threshold_Grad_v4 between 20 and 29 then 20
           when Threshold_Grad_v4 between 30 and 39 then 30
           when Threshold_Grad_v4 between 40 and 49 then 40
           when Threshold_Grad_v4 between 50 and 59 then 50
           when Threshold_Grad_v4 between 60 and 69 then 60
           when Threshold_Grad_v4  between 70 and 79 then 70
           when Threshold_Grad_v4  between 80 and 89 then 80
           when Threshold_Grad_v4  between 90 and 99 then 90
           when Threshold_Grad_v4  between 100 and 109 then 100
           when Threshold_Grad_v4  between 110 and 119 then 110
           when Threshold_Grad_v4  between 120 and 129 then 120
           when Threshold_Grad_v4  between 130 and 139 then 130
           when Threshold_Grad_v4  between 140 and 149 then 140
           when Threshold_Grad_v4  between 150 and 159 then 150
           when Threshold_Grad_v4  between 160 and 169 then 160
           when Threshold_Grad_v4  between 170 and 179 then 170
           when Threshold_Grad_v4  between 180 and 189 then 180
           when Threshold_Grad_v4  between 190 and 199 then 190
           when Threshold_Grad_v4  = 200 then 200
         end as Threshold_Grad_v4_Intervals
         into Second_Comparison_v4
from     Current_Threshold_Seg2 as CTS2
inner join Gradient_Threshold_Seg2_v4 as GN2
on GN2.Viewing_Type_Detailed = CTS2.Viewing_Type_Detailed
and GN2.EVENT_START_DOW = CTS2.EVENT_START_DOW
and GN2.event_start_hour = CTS2.event_start_hour
and coalesce(GN2.pack_grp, 'Empty')= coalesce(CTS2.pack_grp, 'Empty')
and coalesce(GN2.genre_description, 'Unknown') = coalesce(CTS2.genre_description, 'Unknown')
---1,590 Row(s) affected

---Segment_3

select   CTS3.Viewing_Type_Detailed
        ,CTS3.EVENT_START_DOW
        ,CTS3.event_start_hour
        ,CTS3.box_subscription
        ,CTS3.pack_grp
        ,CTS3.genre_description
        ,CTS3.Threshold_Curr
        ,GN3.Threshold_Grad_v4
         ,case
           when Threshold_Curr  between -39 and -30 then -30
           when Threshold_Curr  between -29 and -20 then -20
           when Threshold_Curr  between -19 and -10 then -10
           when Threshold_Curr  between -9 and -1 then -1
           when Threshold_Curr  between 0 and 9 then 0
           when Threshold_Curr  between 10 and 19 then 10
           when Threshold_Curr  between 20 and 29 then 20
           when Threshold_Curr  between 30 and 39 then 30
           when Threshold_Curr  between 40 and 49 then 40
           when Threshold_Curr  between 50 and 59 then 50
           when Threshold_Curr  between 60 and 69 then 60
           when Threshold_Curr  between 70 and 79 then 70
           when Threshold_Curr  between 80 and 89 then 80
           when Threshold_Curr  between 90 and 99 then 90
           when Threshold_Curr  between 100 and 109 then 100
           when Threshold_Curr  between 110 and 119 then 110
           when Threshold_Curr  between 120 and 129 then 120
           when Threshold_Curr  between 130 and 139 then 130
           when Threshold_Curr  between 140 and 149 then 140
           when Threshold_Curr  between 150 and 159 then 150
           when Threshold_Curr  between 160 and 169 then 160
           when Threshold_Curr  between 170 and 179 then 170
           when Threshold_Curr  between 180 and 189 then 180
           when Threshold_Curr  between 190 and 199 then 190
           when Threshold_Curr  = 200 then 200
         end as Threshold_Curr_Intervals
         ,case
           when Threshold_Grad_v4 between 0 and 9 then 0
           when Threshold_Grad_v4 between 10 and 19 then 10
           when Threshold_Grad_v4 between 20 and 29 then 20
           when Threshold_Grad_v4 between 30 and 39 then 30
           when Threshold_Grad_v4 between 40 and 49 then 40
           when Threshold_Grad_v4 between 50 and 59 then 50
           when Threshold_Grad_v4 between 60 and 69 then 60
           when Threshold_Grad_v4  between 70 and 79 then 70
           when Threshold_Grad_v4  between 80 and 89 then 80
           when Threshold_Grad_v4  between 90 and 99 then 90
           when Threshold_Grad_v4  between 100 and 109 then 100
           when Threshold_Grad_v4  between 110 and 119 then 110
           when Threshold_Grad_v4  between 120 and 129 then 120
           when Threshold_Grad_v4  between 130 and 139 then 130
           when Threshold_Grad_v4  between 140 and 149 then 140
           when Threshold_Grad_v4  between 150 and 159 then 150
           when Threshold_Grad_v4  between 160 and 169 then 160
           when Threshold_Grad_v4  between 170 and 179 then 170
           when Threshold_Grad_v4  between 180 and 189 then 180
           when Threshold_Grad_v4  between 190 and 199 then 190
           when Threshold_Grad_v4  = 200 then 200
          end as Threshold_Grad_v4_Intervals
         into Third_Comparison_v4
from     Current_Threshold_Seg3 as CTS3
inner join Gradient_Threshold_Seg3_v4 as GN3
on GN3.Viewing_Type_Detailed = CTS3.Viewing_Type_Detailed
and GN3.EVENT_START_DOW = CTS3.EVENT_START_DOW
and GN3.event_start_hour = CTS3.event_start_hour
and GN3.box_subscription = CTS3.box_subscription
and coalesce(GN3.pack_grp, 'Empty')= coalesce(CTS3.pack_grp, 'Empty')
and coalesce(GN3.genre_description, 'Unknown') = coalesce(CTS3.genre_description, 'Unknown')

---9,625 Row(s) affected




