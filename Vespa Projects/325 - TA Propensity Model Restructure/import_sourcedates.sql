    drop table sourcedates_text;
  create table sourcedates_text([Reference] varchar(100)
                               ,[Attachments_Table] varchar(100)
                               ,[Account_Numbers_ending_in] varchar(100)
                               ,[Snapshot_Date] varchar(100)
                               ,[2_Years_Prior] varchar(100)
                               ,[1_Year_Prior] varchar(100)
                               ,[10_Months_Prior] varchar(100)
                               ,[9_Months_Prior] varchar(100)
                               ,[6_Months_Prior] varchar(100)
                               ,[3_Months_Prior] varchar(100)
                               ,[1_Month_Prior] varchar(100)
                               ,[1_Month_Future] varchar(100)
                               ,[2_Months_Future] varchar(100)
                               ,[3_Months_Future] varchar(100)
                               ,[4_Months_Future] varchar(100)
                               ,[5_Months_Future] varchar(100)
                               ,[6_Months_Future] varchar(100)
);

    load table sourcedates_text([Reference]',',
                                Attachments_Table',',
                                Account_Numbers_ending_in',',
                                Snapshot_Date',',
                                [2_Years_Prior]',',
                                [1_Year_Prior]',',
                                [10_Months_Prior]',',
                                [9_Months_Prior]',',
                                [6_Months_Prior]',',
                                [3_Months_Prior]',',
                                [1_Month_Prior]',',
                                [1_Month_Future]',',
                                [2_Months_Future]',',
                                [3_Months_Future]',',
                                [4_Months_Future]',',
                                [5_Months_Future]',',
                                [6_Months_Future]'\n'
    )
    from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/sourcedates.csv' QUOTES OFF ESCAPES OFF SKIP 1
;

      drop table sourcedates;
    select * into sourcedates     from sourcedates_20130813 where 1=2;

    insert into sourcedates([Reference]
                           ,Attachments_Table
                           ,Account_Numbers_ending_in
                           ,Snapshot_Date
                           ,[2_Years_Prior]
                           ,[1_Year_Prior]
                           ,[10_Months_Prior]
                           ,[9_Months_Prior]
                           ,[6_Months_Prior]
                           ,[3_Months_Prior]
                           ,[1_Month_Prior]
                           ,[1_Month_Future]
                           ,[2_Months_Future]
                           ,[3_Months_Future]
                           ,[4_Months_Future]
                           ,[5_Months_Future]
                           ,[6_Months_Future]
    )
    select [Reference]
          ,Attachments_Table
          ,Account_Numbers_ending_in
          ,cast(right([Snapshot_Date],4) || '-' || substr([Snapshot_Date],4,2) || '-' || left([Snapshot_Date],2) as date)
          ,cast(right([2_Years_Prior],4) || '-' || substr([2_Years_Prior],4,2) || '-' || left([2_Years_Prior],2) as date)
          ,cast(right([1_Year_Prior],4) || '-' || substr([1_Year_Prior],4,2) || '-' || left([1_Year_Prior],2) as date)
          ,cast(right([10_Months_Prior],4) || '-' || substr([10_Months_Prior],4,2) || '-' || left([10_Months_Prior],2) as date)
          ,cast(right([9_Months_Prior],4) || '-' || substr([9_Months_Prior],4,2) || '-' || left([9_Months_Prior],2) as date)
          ,cast(right([6_Months_Prior],4) || '-' || substr([6_Months_Prior],4,2) || '-' || left([6_Months_Prior],2) as date)
          ,cast(right([3_Months_Prior],4) || '-' || substr([3_Months_Prior],4,2) || '-' || left([3_Months_Prior],2) as date)
          ,cast(right([1_Month_Prior],4) || '-' || substr([1_Month_Prior],4,2) || '-' || left([1_Month_Prior],2) as date)
          ,cast(right([1_Month_Future],4) || '-' || substr([1_Month_Future],4,2) || '-' || left([1_Month_Future],2) as date)
          ,cast(right([2_Months_Future],4) || '-' || substr([2_Months_Future],4,2) || '-' || left([2_Months_Future],2) as date)
          ,cast(right([3_Months_Future],4) || '-' || substr([3_Months_Future],4,2) || '-' || left([3_Months_Future],2) as date)
          ,cast(right([4_Months_Future],4) || '-' || substr([4_Months_Future],4,2) || '-' || left([4_Months_Future],2) as date)
          ,cast(right([5_Months_Future],4) || '-' || substr([5_Months_Future],4,2) || '-' || left([5_Months_Future],2) as date)
          ,cast(right([6_Months_Future],4) || '-' || substr([6_Months_Future],4,2) || '-' || left([6_Months_Future],2) as date)
      from sourcedates_text




