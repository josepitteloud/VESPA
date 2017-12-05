Attribute VB_Name = "Module1"
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'   Project Vespa; Automated Reporting Suite
'
' This code is not actually maintained within the Excel document, because Excel
' is terriblepants at version control etc. The delevoped version is held outside
' in the Git repository, and gets purged/loaded any time there are changes to be
' made. Bit of an extra management headache, but in return we get commit, branch,
' merge, diff, blame, rebase, all the things that you need to build responsibly.
'
' Note that code loading, purging / refreshing happens in two scripts which exist
' in the workbook proper and not in this script. There are buttons which fire
' those actions on the Settings tab, next to where you specify the location of
' the file you want to import.
'
' Also, refer to:
'   http://rtci/vespa1/Vespa%20report%20suite.aspx
' and also update that page if you're making significant changes to this code.
'
' Sections:
'
'      Part A: A01 -  Spreadshet management stuff
'              A02 -  Report independent DB connecting stuff
'
'      Part B: B01 -  Operational Dashboard (OpDash)
'              B02 -  Opt Out sheet
'              B03 -  Daily Summary sheet
'              B04 -  Weekly KPI sheet (external data)
'              B05 -  Weekly Enablement sheet
'              B06 -  Box Type & Premiums sheet
'              B07 -  Dates in title boxes (all sheets)
'              B08 -  Finishing stuff off
'
'              B21 -  Sky View panel version of Operational Dashboard (SVDash)
'              B23 -  Daily Summary sheet
'              B25 -  Weekly Enablement sheet
'              B26 -  Box Type & Premiums sheet
'              B27 -  Dates in title boxes (all sheets)
'              B28 -  Finishing stuff off
'
'      Part C: C01 -  Dialback Report (Dialback or DBR)
'              C03 -  The 6 basic tabs (30/7 day total/distinct/raw)
'              C04 -  Transition probabilities (DISCONTINUED)
'              C05 -  Overview page
'              C06 -  Number of events in logs
'              C07 -  Time of day of dialback
'              C08 -  Dates in title bars
'              C09 -  Tidying up, finishing off
'
'              C21 -  The Sky View Panel version of the DBR
'              C23 -  The 6 basic tabs (30/7 day total/distinct/raw)
'              C24 -  Transition probabilities (DISCONTINUED)
'              C25 -  Overview page
'              C26 -  Number of events in logs
'              C27 -  Time of day of dialback
'              C28 -  Dates in title bars
'              C29 -  Tidying up, finishing off
'
'      Part D: D01 -  Key Viewing Facts (KVF) - not in play (replaced by Share of Viewing?)
'
'      Part E: E01 -  Weekly Status Report (WeStat)
'              E02 -  Vespa sheet
'              E03 -  Sky View sheet (NYIP)
'              E04 -  Alternate panel sheets (NYIP)
'              E05 -  All the title bars
'              E06 -  Tidying up
'
'      Part F:  F01 -   Executive Dashboard (XDash ID)
'               F02 -   Refreshing Pivot tables
'               F03 -   Executive Dashboard (XDash ED)
'               F04 -   Refreshing Pivot tables
'
'      Part G: G01 -  The Panel Management report (PanMan)
'              G02 -  Summary sheet
'              G03 -  Traffic lights sheet
'              G04 -  Analysts by single variables (all 6 tabs worth)
'              G05 -  Over- and under- representation
'              G06 -  Box swing etc
'              G07 -  All the title bars
'              G08 -  Tidying up before we quit
'              G09 -  How are the migration lists being handled?
'
'      Part H:  H01 -   Viewing Consent Report (ViewCons)
'               H02 -   Refreshing Pivot tables
'
'
' Further features: actual features, not dev building stuff:
'  17/ So there's a button for Exit on the form, but the thing don't actually work.
'  18/ The whole thing might need to get rebuilt with the new branding identity thing.
'       Haven't got the docs on how that's done yet? So, yeah. Sometimes. All, the
'       report templates too, ha!
'  22/ The main report building query also wants to show any QA errors turning up from
'       the logger when each report completes.
'  30/ Some visibility of Failed or Bounced reports within the report suite? Currently
'       we only get that if we look on a subsequent page... and only if the report
'       returns error codes to the scheduler.
'
' Recently implemented things:
'  28/ PanMan: integration of changes resulting from Scaling 2
'  29/ New Universe breakdown on summary requested with scaling 2
'  29/ Panman: other minor cosmetic changes...
'  31/ Now with Excel 2010 compatibility via the ListBox object
'  32/ OpDash: aligned templates with current RTM lists
'
' Okay, so some of the funcitonality we pulled out into this file, but mostly
' we left stuff in there when we realised there could be scope bugs. Having
' experience with the sort of error messages that VBA produces, debugging the
' scope stuff is going to be awful. So we won't.
'
' There are some magic constants that will cause report failure if certain
' table pulls exceed 2500 rows. Just so you know. Probably won't encounter
' that for a good 5 years or so, report probably won't be in play by then.
' Unless you're building some kind of pivot...? Which happens fairly often,
' as it turns out, so, yeah.
'
' This guy still takes a few seconds to run, largely because of the Activate
' calls around the place. There's a way to do it without Activate, probably...
' Is it Select? apparently not. Not a huge deal though, fast enough considering.
'
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Option Explicit
Option Private Module

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''          A01:  Spreadsheet management: menus, exiting, etc            ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public Sub Initialise_Menu()
    'Okay, so now we're starting to outsource and then start versioning the code. This will
    'probably make debugging significantly more challenging... though... presumably it will
    'show me the code and be able to step through it after the initialise code method has
    'been called?

    'If the user has disabled macros then they'll see the warning sheet explaining how to use this file, otherwise
    'we'll get here so we need to show the menu sheet
    ThisWorkbook.Worksheets("menu").Activate
    
    Dim Y As Integer
    
    With ThisWorkbook.Worksheets("control")
        For Y = 2 To .Range("A1").End(xlDown).Row
            'Whether the report is ready to run or not is now managed via the report status vlookup...
            
            'Set the selected flag to FALSE for all the reports
            .Range("D" & Y) = False
        Next Y
    End With
    
    'Populate the report list
    RefreshReportList

    With ThisWorkbook.Worksheets("Menu").lstReports
        'We need a bit of resizing here in order to make the ListView fill its space
        .Width = 336
        
        'Adjust the column widths, this is easier to do wth code than to do it manually
       ' .ColumnHeaders(2).Width = 65
       ' .ColumnHeaders(3).Width = 90
        If .ListCount > 9 Then
            'If we've got more than 9 items then the vertical scroll bar appears and we need to make room for it in order to prevent
            'the horizontal scroll bar from appearing
'            .ColumnHeaders(1).Width = (.Width * (4 / 3)) - (.ColumnHeaders(2).Width + .ColumnHeaders(3).Width + 17)
        Else
            '.ColumnHeaders(1).Width = (.Width * (4 / 3)) - (.ColumnHeaders(2).Width + .ColumnHeaders(3).Width)
        End If
    End With
    
    'EnableView
    
    'Create the popup menus
    CreateMenu

End Sub


Public Sub AddListViewControls()
Dim lv As ListBox
    
    ' For some reason Excel 2010 doesn't seem to recognise a ListView control when it opens a file with one in.  The fix for this is to re-create the controls
    ' whenever the tool is opened.  What a ball-ache !!

    Application.ScreenUpdating = False

'ListViews no longer work, thanks to a Microsoft VB "Update", so we're now using a listbox (and text boxes for column headers), and these do get recognised when Excel is opened
'    RemoveListViewControls

    
    ' Add the two ListView Controls
'   Sheet1.OLEObjects.Add(ClassType:="forms.listbox.1", Link:=False, DisplayAsIcon:=False, Left:=12, Top:=130, Width:=380, Height:=141).Name = "lstReports"

    ' Configure the reports list


    With Sheet1.lstReports
'        .ColumnHeaders.Add , , "Available reports"
'        .ColumnHeaders.Add , , "Last run", 65
'        .ColumnHeaders.Add , , "Ready", 90
'        .ColumnHeaders.Add , , "Setup", 0
'        .ColumnHeaders.Add , , "RowID", 0
'        .View = lvwReport
'        .CheckBoxes = True
'        .LabelEdit = lvwManual
'        .Appearance = ccFlat
'        .AutoLoad = True
'        .FullRowSelect = True
'        .ColumnCount = 2
'        .ColumnHeads = True
'        .ListStyle = fmListStyleOption
        .Height = 120
        .Width = 336
    End With

    ' We need to do some faffing to get the listviews to repaint properly
    Sheet5.Activate
    Sheet1.Activate
    ActiveWindow.ScrollRow = 2
    ActiveWindow.ScrollRow = 1
    Application.ScreenUpdating = True

    ' For some reason the action of adding controls like this causes Excel to not handle running subsequent code directly very well, so we'll
    '   wait a moment and get Excel to kick things off rather than calling the proc directly.  Seems to work.
    Application.OnTime Now + (1 / 24 / 60 / 60), "ThisWorkbook.Initialise"
    
End Sub



Public Sub RemoveListViewControls()
Dim shp As Shape
Dim a As Integer

    For Each shp In Sheet1.Shapes
        If shp.Name = "lstReports" Or shp.Name Like "ListView*" Then
            shp.Delete
        End If
    Next shp
End Sub

Public Sub Check_Connection()
    If conn.ConnectionString = "" Then
        'If we're not connected then show the login form
        'MsgBox "You need to be connected to a database before you can run any reports.", vbOKOnly + vbInformation, "Sky reporting suite"
        frmLogin.txtOpenArg = "run"
        frmLogin.Show
    End If

End Sub

Public Sub Safe_Exit()

    'Just prevent the workbook from being saved
    ThisWorkbook.Saved = True

    'Remove the custom popup menus that we created when we opened the workbook
    RemoveMenu
    
    ' Remove the listViews, Excel doesn't seem to keep them when re-opening
'    RemoveListViewControls
    
    'Activate the alternate menu sheet ready for when the file is opened next time
    ThisWorkbook.Worksheets("alternate menu").Activate
    
    'Clean out the report status and flip check flags
    ThisWorkbook.Worksheets("ReportStatus").Range("A5:B5").Clear
    ThisWorkbook.Worksheets("ReportStatus").Range("A9:C109").Clear ' - Will we ever have more than 100 concurrently active reports? Probably not.
    
    'Um... don't we kind of have to save it now? Otherwise all the format preservation etc
    'counts for nothing because it didn't get saved?
    
    If MsgBox("Save Changes to Report Suite?", vbYesNo + vbQuestion, "Save on Exit") = vbYes Then
        ThisWorkbook.Save
    End If

End Sub


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''             A02: Generic report independent status stuff              ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

'Things that have to connect to the database to get the current state

Public Sub Purge_Transient_Tables()
    ' Check the DB conenction is in play
    Check_Connection

    ' Then execute the table purge script:
    conn.Execute ("execute vespa_analysts.Purge_all_transient_report_tables")

End Sub


Public Sub Mark_Opt_In_Flag()
    ' If we're not connected to the database yet then do that first
    Check_Connection

    ' Then run the opting in script
    conn.Execute (GetSQL(ActiveWorkbook.Worksheets("Settings").Range("B11") & "\Vespa core 06 register opt-in.sql"))
    
    ' Then check if the DB is ready for reporting
    Check_Flips
    
End Sub


Public Sub Check_Flips()
    ' Check if the various important DB tables and views are ready for this week (ie have
    ' been flipped with data as of last Thursday)
    
    ' If we're not connected to the database yet then do that first
    Check_Connection
    
    ' Okay, so we're no longer checking each table individually, but pulling the
    ' Opt In and Table Flip flags from the reporting scheduler that lives... in
    ' a completely different Git repository, as it turns out. That's okay, it'll
    ' eventually be aligned.
    ThisWorkbook.Worksheets("ReportStatus").Range("A5").CopyFromRecordset conn.Execute(GetSQL(ActiveWorkbook.Worksheets("Settings").Range("B11") & "\Vespa core 03 check flips and opt in.sql"))

    ' Okay, now update the main page based on those tags:
    ' Nope, that happens automatically based on formulas now.
    
End Sub


Public Sub View_Completed_Weekly_Reports()

    'Go to the database, pick up the list of things which have been completed
    'recently. Um, somewhere we also need to track the state of the data and
    'whether or not reports are ready to be scheduled this run. Even though...
    'the triggers won't be set here, that all takes place in a scheduler
    'somewhere else. But it'll still be good to see that the required tables
    'are updated as needed etc.

    ' If we're not connected to the database yet then do that first
    Check_Connection
    
    ' First off, Get the report status quotes into the spreadsheet
    ThisWorkbook.Worksheets("ReportStatus").Range("A9").CopyFromRecordset conn.Execute(GetSQL(ActiveWorkbook.Worksheets("Settings").Range("B11") & "\Vespa core 04 report readiness.sql"))
    
    ' Then update the report lookup thingy: well, that happens automatically now,
    RefreshReportList
    ' but we also want the thing about the view flips refreshing too:
    Check_Flips
    
End Sub

Public Sub Blank_Holding_page()
    ' This is used by a couple of reports to clear holding pages into which data gets
    ' dumped; clear all the items other than the headers on the active worksheet.
    ActiveSheet.Range("A2").Select
    Range(Selection, Selection.End(xlDown)).Select
    Range(Selection, Selection.End(xlToRight)).Clear
End Sub

Public Sub Format_Borders()
    ' This guy will apply the standard Sky IQ borders to a block of stuff; thick
    ' black borders around the selection and the grey dotted lines in between.
    
    Selection.Borders(xlDiagonalDown).LineStyle = xlNone
    Selection.Borders(xlDiagonalUp).LineStyle = xlNone
    With Selection.Borders(xlEdgeLeft)
        .LineStyle = xlContinuous
        .ColorIndex = 0
        .TintAndShade = 0
        .Weight = xlMedium
    End With
    With Selection.Borders(xlEdgeTop)
        .LineStyle = xlContinuous
        .ColorIndex = 0
        .TintAndShade = 0
        .Weight = xlMedium
    End With
    With Selection.Borders(xlEdgeBottom)
        .LineStyle = xlContinuous
        .ColorIndex = 0
        .TintAndShade = 0
        .Weight = xlMedium
    End With
    With Selection.Borders(xlEdgeRight)
        .LineStyle = xlContinuous
        .ColorIndex = 0
        .TintAndShade = 0
        .Weight = xlMedium
    End With
    Selection.Borders(xlInsideVertical).LineStyle = xlNone
    With Selection.Borders(xlInsideHorizontal)
        .LineStyle = xlContinuous
        .ThemeColor = 1
        .TintAndShade = -0.249946592608417
        .Weight = xlHairline
    End With
    
End Sub

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''              B01: OPERATIONAL DASHBOARD (OPDASH) REPORT               ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

' This is the Operational Dashboard, for which old run instructions (including
' formatting notes etc) can be located here:
'   http://rtci/vespa1/The%20Operational%20Dashboard.aspx
'
' The only tricky bit of this script are the cases where tables grow by an
' indeterminate number of record each week. We handle this by putting the data
' into a holding pen and inserting rows into the middle. This preserves the
' formats and also automatically updates and other formulae references which
' point to the last row. Then formulas are copied down, and because we're in
' the middle of the table we don't have any corner case / bottom row formats
' to deal with, so that's all fine.
'
' It's also not thread-safe since it pushes stuff through the clipboard, but
' there doesn't seem to be a way to do values only direct copy...
'

Public Sub GenerateReport_Vespa_OpDash()
    ' A bunch of variables we need...
    Dim records_start_at    As Integer      ' Position in the final report that records begin for some table
    Dim records_count       As Integer      ' Number of records the table has to be expanded to fit
    Dim prior_records_count As Integer      ' The number of records already in the report (from a prior build)
    Dim line_counter        As Integer
    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim externalloaddate    As String       ' The date bit of an external file we're loading (Weekly KPI page)
    Dim externalfilepath    As String       ' Full name of an external file (Weekly KPI page)
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim newsource           As Workbook     ' For loading the daily KPI file stuff
    Dim column_placement    As Integer      ' For trackign the column that the KPI stuff is going into
    Dim sht                 As Worksheet
    Dim holding             As Worksheet

    todaysdate = Date
    
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Vespa Operational Dashboard (TEMPLATE).xls", _
             ThisWorkbook.Path & "\Vespa Operational Dashboard " & Format(todaysdate, "yyyymmdd") & ".xls"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Vespa Operational Dashboard " & Format(todaysdate, "yyyymmdd") & ".xls")
    
    codepath = settings_sheet.Range("B11") & "\operational_dashboard\"
    
    '''''''''''''''''' B02: Opt Out Sheet: ''''''''''''''''''
    ' At this point we're just hoping that the number of RTM options doesn't change...
    wbk.Worksheets("Opt Out").Range("B10").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-01 opt out all accounts.sql"))
    wbk.Worksheets("Opt Out").Range("B50").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-02 opt out 26th May.sql"))
    wbk.Worksheets("Opt Out").Range("B70").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-03 opt out 28th April.sql"))
    wbk.Worksheets("Opt Out").Range("B90").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-04 opt out at activation.sql"))
    
    wbk.Worksheets.Add().Name = "Staging_Area"
   
    '''''''''''''''''' B03: Daily Summary Sheet: ''''''''''''''''''
    wbk.Worksheets("Daily Summary").Activate
    
    ' We do this guy before the Weekly KPI files because we want to know what the last
    ' date of data extraction is / was so we can look for the right daily files.
    
    ' This guy is query 10 because he was aded last, even though he gets used 4th. It's always 7 days worth of data...
    wbk.Worksheets("Staging_Area").Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-10 daily reporting proportions.sql"))
    
    ' But now it has to go in in a manner that dodges those other percentages in the middle;
    wbk.Worksheets("Staging_Area").Range("A1:K7").Copy
    wbk.Worksheets("Daily Summary").Range("B9").PasteSpecial xlPasteValues
    
    ' The chunk to the right of those three percentages:
    wbk.Worksheets("Staging_Area").Range("L1:Z7").Copy
    wbk.Worksheets("Daily Summary").Range("P9").PasteSpecial xlPasteValues
   
    wbk.Worksheets("Staging_Area").Cells.Clear
    
    prior_records_count = wbk.Worksheets("Daily Summary").Range("B21:B2500").Find("", , , xlWhole).Row ' Not quite the count, but the end of the previously filled range...
    wbk.Worksheets("Staging_Area").Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-05 daily summary.sql"))
    records_count = wbk.Worksheets("Staging_Area").Range("A1:A2500").Find("", , , xlWhole).Row + 21       ' New end of the content, with adjustment for vertical table offset
    ' So now the data is in and we know how many rows were before and after, insert
    ' rows as required until the table is the right size
    Rows(prior_records_count - 6 & ":" & prior_records_count - 6).Select
    line_counter = prior_records_count + 1
    Do While line_counter < records_count
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop
    ' And bring down those formulas too;
    Range("F" & prior_records_count - 7 & ":G" & prior_records_count - 7).Select
    Selection.AutoFill Destination:=Range("F" & prior_records_count - 7 & ":G" & records_count - 7), Type:=xlFillSeries
    
    ' The other newer formulas too (which act on the second chunk of data with the enablement numbers and P/S split
    Range("N" & prior_records_count - 7 & ":P" & prior_records_count - 7).Select
    Selection.AutoFill Destination:=Range("N" & prior_records_count - 7 & ":P" & records_count - 7), Type:=xlFillSeries
    
    ' Now the formats are ready, go and push the first block of data in
    wbk.Worksheets("Staging_Area").Range("A1:D" & records_count - 1).Copy
    wbk.Worksheets("Daily Summary").Range("B21").PasteSpecial xlPasteValues
    
    ' And then the second block which has the Enablement, the P/S split, and the anytime breakdown
    wbk.Worksheets("Staging_Area").Range("E1:J" & records_count - 1).Copy
    wbk.Worksheets("Daily Summary").Range("H21").PasteSpecial xlPasteValues

    ' Pull the most recent date out of the daily stuffs....
    
    '''''''''''''''''' B04: Import all the externally hosted Weekly KPI files ''''''''''''''''''
    
    ' This is the last thing to do! How we're going to handle this:
    '   1/ Get the latest date from the daily logs tab
    '   2/ Blank the weekly KPI block
    '   3/ Look (waiting for errors) for the 7 daily files up to and including the latest daily thing.
    '   4/ If the .csv exists:
    '       i. Open the csv
    '       ii. Paste the details from the csv to the workbook
    '       iii. Close the .csv
    '       iv. move it to the archive folder
    '   5/ If the .csv doesn't exist, put a little note about lack of data
    ' That way if we get daily things that are too new, we'll just leave them there until
    ' we need them next. Slight risk of badly named cruft building up there, but hey.
    
    ' OK, so the most recent date is always going to end up in C15 on the Daily summary tab; so
    ' pulling that into the weekly KPIs tab, we can now drive the whole thing via the dates in
    ' the table already. Awesomes.
    
    ' 2/ Blank the whole table
    wbk.Worksheets("Weekly KPI Measures").Range("D11:J61").ClearContents
    
    ' 3/ Look for the 7 daily files, in chronological order;
    column_placement = 4
    Do While column_placement <= 10
        
        ' Pull the date that we want out of the appropriate cell, for mthe expected filename
        externalloaddate = Format(wbk.Worksheets("Weekly KPI Measures").Cells(10, column_placement).Value, "yyyy-mm-dd")
        externalfilepath = settings_sheet.Range("B14").Value & "\Operational Dashboard\Dalies\vespa_dashboard_" & externalloaddate & ".csv"
        
        ' No longer trying to emulate safe exception handling behavior. Because, VBA wasn't catching
        ' the errors anyway, they still arrived on the desktop as a message box :-(
        If Dir(externalfilepath) <> "" Then
            
            ' OKay, so in this section where we're formatting today's date, we instead need to be
            ' reformatting the date from the column lookup using column_placement. Not difficult,
            ' maybe a bit annoying on how Excel is going to treat things.
            
            Set newsource = Workbooks.Open(externalfilepath)

            ' Now we've got the file open and figured out which column it's going into...
            newsource.Worksheets("vespa_dashboard_" & externalloaddate).Range("B4:B54").Copy
            wbk.Worksheets("Weekly KPI Measures").Cells(11, column_placement).PasteSpecial xlPasteValues
        
            newsource.Close SaveChanges:=False

            ' Cool, so now, move that file to the archives folder.
            Name settings_sheet.Range("B14").Value & "\Operational Dashboard\Dalies\vespa_dashboard_" & externalloaddate & ".csv" _
            As settings_sheet.Range("B14").Value & "\Operational Dashboard\Dalies\Archive\vespa_dashboard_" & externalloaddate & ".csv"
            ' Why isn't this the same call structure as copying? oh well. Consistency is too much for Excel apparently.
        
        Else
            ' Okay, now we're no longer tracking failure states at all, we're just hoping it
            ' works because VBA is generally pants at achieving anything responsibly...
            wbk.Worksheets("Weekly KPI Measures").Cells(11, column_placement).Value = "No data!"
            ' And some tracking stuff too:
            'wbk.Worksheets("Weekly KPI Measures").Cells(12,column_placement).Value = externalfilepath
            'wbk.Worksheets("Weekly KPI Measures").Cells(13,column_placement).Value =externalloaddate
        End If

        ' On to the next column / daily file
        column_placement = column_placement + 1
    Loop
    Application.DisplayAlerts = True
    
    '''''''''''''''''' B05: Weekly Enablement sheet: ''''''''''''''''''
    'a bunch of stuff forcing special treatment...
    wbk.Worksheets("Weekly Enablement").Activate
    
    Set holding = wbk.Worksheets("Staging_Area")
    Set sht = wbk.Worksheets("Weekly Enablement")
    
    holding.Cells.Clear
    holding.Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-06 summary by box.sql"))
    ' Figure out how many rows there are, and if any rows need to be inserted...
    records_start_at = 10                                                       ' First on is hardcoded, easy.
    records_count = holding.Range("A1:A2500").Find("", , , xlWhole).Row         'Number of non-blank items in the new pull
    prior_records_count = wbk.Worksheets("Weekly Enablement").Range("B1:B2500").Find("Box Enablement and data returned - By Account", , , xlWhole).Row - 2 - records_start_at
    ' The prior count is based on identifying the header of the next table and subtracting the
    ' offset from the end of the data, then adding on the offset to the beginning of the table.
    
    ' Reusing our trick to insert additional rows
    Rows(prior_records_count - 6 + records_start_at & ":" & prior_records_count - 6 + records_start_at).Select
    line_counter = prior_records_count + 1
    Do While line_counter < records_count
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop

    ' OK, now copy the formulas down...
    If records_count > prior_records_count Then
        Range("H" & prior_records_count - 7 + records_start_at & ":J" & prior_records_count - 7 + records_start_at).Select
        Selection.AutoFill Destination:=Range("H" & prior_records_count - 7 + records_start_at & ":J" & records_count - 7 + records_start_at), Type:=xlFillSeries
    End If
    
    ' Now push the right values into the right places...
    holding.Range("A1:F" & records_count - 1).Copy
    sht.Range("B" & records_start_at).PasteSpecial xlPasteValues
    
    holding.Range("J1:S" & records_count - 1).Copy
    sht.Range("K10").PasteSpecial xlPasteValues

    ' Now do the same with the subsequent reports...
    holding.Cells.Clear
    holding.Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-07 summary by accounts.sql"))

    records_start_at = wbk.Worksheets("Weekly Enablement").Range("B1:B2500").Find("Box Enablement and data returned - By Account", , , xlWhole).Row + 3
    records_count = holding.Range("A1:A2500").Find("", , , xlWhole).Row         'Number of non-blank items in the new pull
    prior_records_count = wbk.Worksheets("Weekly Enablement").Range("B1:B2500").Find("Returning Data - Box has returned any viewing since being enabled", , , xlWhole).Row - 2 - records_start_at
    ' The prior count is based on identifying a post-table note and subtracting the
    ' offset from the end of the data, then adding on the offset to the beginning of the table.
    
    ' Reusing our trick to insert additional rows
    Rows(prior_records_count - 6 + records_start_at & ":" & prior_records_count - 6 + records_start_at).Select
    line_counter = prior_records_count + 1
    Do While line_counter < records_count
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop

    ' OK, now copy the formulas down...
    If records_count > prior_records_count Then
        Range("L" & prior_records_count - 7 + records_start_at & ":N" & prior_records_count - 7 + records_start_at).Select
        Selection.AutoFill Destination:=Range("L" & prior_records_count - 7 + records_start_at & ":N" & records_count - 7 + records_start_at), Type:=xlFillSeries
    End If
    
    ' Now push the right values into the right places...
    holding.Range("A1:J" & records_count - 1).Copy
    sht.Range("B" & records_start_at).PasteSpecial xlPasteValues
    
    holding.Range("M1:AH" & records_count - 1).Copy
    sht.Range("O" & records_start_at).PasteSpecial xlPasteValues
    
    ' Also make sure the cumulative table gets updated: oh hey, the number
    ' of records (and previous records) should be static... need to refresh
    ' the start of the table though (cos it's a new table)
    records_start_at = wbk.Worksheets("Weekly Enablement").Range("B1:B2500").Find("Box Enablement and data returned - By Box (Cumulative)", , , xlWhole).Row + 3
    Rows(prior_records_count - 6 + records_start_at & ":" & prior_records_count - 6 + records_start_at).Select
    line_counter = prior_records_count + 1
    Do While line_counter < records_count
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop
    
        If records_count > prior_records_count Then
        Range("B" & prior_records_count - 7 + records_start_at & ":J" & prior_records_count - 7 + records_start_at).Select
        ' But this time pushing it all the way to the bottom, because the inserts messed
        ' with the references and they need to be updated...
        Selection.AutoFill Destination:=Range("B" & prior_records_count - 7 + records_start_at & ":J" & records_count + records_start_at - 2), Type:=xlFillSeries
    End If
    
    ' And now put the border back on the bottom...
    Range("B" & records_count + records_start_at - 2 & ":J" & records_count + records_start_at - 2).Select
    With Selection.Borders(xlEdgeBottom)
        .LineStyle = xlContinuous
        .ColorIndex = 0
        .TintAndShade = 0
        .Weight = xlMedium
    End With
    
    ' Now we're done, remove the working sheet
    Application.DisplayAlerts = False
    wbk.Worksheets("Staging_Area").Delete
    Application.DisplayAlerts = True
    ' So the recomended way to dodge the popup is to blanket deny all popups and
    ' then re-enable them afterwards? sure. Whatever
    
    '''''''''''''''''' B06: Box Type & Premiums sheet: ''''''''''''''''''
    wbk.Worksheets("Box Type and Premiums").Activate

    wbk.Worksheets("Box Type and Premiums").Range("B10").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-08 box type.sql"))
    wbk.Worksheets("Box Type and Premiums").Range("B30").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa OpDash 03-09 premiums.sql"))
    ' And that other stuff about recolouring the graph bars and updating the weights around the sides...

    ' So... just need to figure out what these text box names are...
    With wbk.Worksheets("Box Type and Premiums")
        .Shapes.Range(Array("Label 4212")).Select
        Selection.Characters.Text = .Range("D25").Value
        .Shapes.Range(Array("Label 4211")).Select
        Selection.Characters.Text = .Range("D24").Value
        .Shapes.Range(Array("Label 4210")).Select
        Selection.Characters.Text = .Range("D23").Value
        .Shapes.Range(Array("Label 4191")).Select
        Selection.Characters.Text = .Range("D22").Value
        .Shapes.Range(Array("FDB_label")).Select
        Selection.Characters.Text = .Range("D21").Value
        .Shapes.Range(Array("multiroom_label")).Select
        Selection.Characters.Text = .Range("D20").Value
        .Shapes.Range(Array("skyplus_label")).Select
        Selection.Characters.Text = .Range("D19").Value
        .Shapes.Range(Array("skypluscombi_label")).Select
        Selection.Characters.Text = .Range("D18").Value
        .Shapes.Range(Array("Label 3506")).Select
        Selection.Characters.Text = .Range("D17").Value
        .Shapes.Range(Array("Label 3507")).Select
        Selection.Characters.Text = .Range("D16").Value
        .Shapes.Range(Array("kdx1tb_label")).Select
        Selection.Characters.Text = .Range("D15").Value
        .Shapes.Range(Array("Label 3509")).Select
        Selection.Characters.Text = .Range("D14").Value
        .Shapes.Range(Array("Label 3510")).Select
        Selection.Characters.Text = .Range("D13").Value
        .Shapes.Range(Array("Label 3511")).Select
        Selection.Characters.Text = .Range("D12").Value
        .Shapes.Range(Array("Label 3512")).Select
        Selection.Characters.Text = .Range("D11").Value
        .Shapes.Range(Array("Label 3513")).Select
        Selection.Characters.Text = .Range("D10").Value
        .Shapes.Range(Array("Label 3515")).Select
        Selection.Characters.Text = .Range("D36").Value
        .Shapes.Range(Array("Label 3518")).Select
        Selection.Characters.Text = .Range("D35").Value
        .Shapes.Range(Array("Label 3519")).Select
        Selection.Characters.Text = .Range("D34").Value
        .Shapes.Range(Array("Label 3523")).Select
        Selection.Characters.Text = .Range("D33").Value
        .Shapes.Range(Array("Label 3520")).Select
        Selection.Characters.Text = .Range("D32").Value
        .Shapes.Range(Array("Label 3521")).Select
        Selection.Characters.Text = .Range("D31").Value
        .Shapes.Range(Array("Label 3522")).Select
        Selection.Characters.Text = .Range("D30").Value
        .Range("C8").Select
        ' Thought these had all been renamed but apparently they didn't take. Oh well, figured out
        ' their names by recording a macro, now I don't care to change them.
        ' And yeah, tried these by tacking 'Characters.Text' on the end of
        ' the Select but apparently that's a method which sets a global or
        ' something, can't figure out how to make those character settings
        ' explicit. There are other hacks we could try, but yeah, don't so
        ' much care about it.
    End With
    
    'To toop through the cells and colour the graph accordingly:
    line_counter = 1
    wbk.Worksheets("Box Type and Premiums").ChartObjects("Chart 21").Activate
    Do While line_counter <= 16 ' There are 16 box types
        ActiveChart.SeriesCollection(1).Points(line_counter).Select
        With Selection.Format.Fill
            .Visible = msoTrue
            .Transparency = 0
            .Solid
            If Range("H" & line_counter + 9).Value < Range("M9").Value Then
                    .ForeColor.RGB = RGB(255, 0, 0)
            ElseIf Range("H" & line_counter + 9).Value > Range("N9").Value Then
                    .ForeColor.RGB = RGB(146, 208, 80)
            Else
                    .ForeColor.RGB = RGB(255, 192, 0)
            End If
        End With
        
        line_counter = line_counter + 1
    Loop
    
    line_counter = 1
    wbk.Worksheets("Box Type and Premiums").ChartObjects("Chart 22").Activate
    Do While line_counter <= 7 ' There are 7 packages
        ActiveChart.SeriesCollection(1).Points(line_counter).Select
        With Selection.Format.Fill
            .Visible = msoTrue
            .Transparency = 0
            .Solid
            If Range("H" & line_counter + 25).Value < Range("M9").Value Then
                    .ForeColor.RGB = RGB(255, 0, 0)
            ElseIf Range("H" & line_counter + 25).Value > Range("N9").Value Then
                    .ForeColor.RGB = RGB(146, 208, 80)
            Else
                    .ForeColor.RGB = RGB(255, 192, 0)
            End If
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' B07: The dates across the top of each sheet... ''''''''''''''''''
    
    ' This section: ugly, messy, and also, we're resetting the cursors to somewhere
    ' nice instead of them defaulting to having a bunch of stuff selected.
    
    ' To put the report period into the headers, we also need to figure out what the
    ' report end date is... fortunately this comes off the Daily Summary tab:
    report_end_date = wbk.Worksheets("Daily Summary").Range("C15").Value
    
    Dim thesheetname As String
    Dim titlecaption As String
    
    line_counter = 1
    Do While line_counter <= 7 ' There are 7 tabs
        If line_counter = 1 Then
            thesheetname = "Summary"
        ElseIf line_counter = 2 Then
            thesheetname = "Weekly KPI Measures"
        ElseIf line_counter = 3 Then
            thesheetname = "Opt Out"
        ElseIf line_counter = 4 Then
            thesheetname = "Weekly Enablement"
        ElseIf line_counter = 5 Then
            thesheetname = "Daily Summary"
        ElseIf line_counter = 6 Then
            thesheetname = "Box Type and Premiums"
        ElseIf line_counter = 7 Then
            thesheetname = "Glossary"
        End If
        
        wbk.Worksheets(thesheetname).Activate
        Range("B7").Select
        
        If line_counter = 2 Then
            thesheetname = "Weekly KPI Measures - Day by Day"
        End If
        
        ActiveSheet.Shapes.Range(Array("shpTitle")).Select
        titlecaption = thesheetname & Chr(10) & "Report created on " & Format(todaysdate, "dd/mm/yyyy") & " covering period " & Format(DateAdd("d", -6, report_end_date), "dd/mm/yyyy") & " to " & Format(report_end_date, "dd/mm/yyyy") & "."
        Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = titlecaption
            
        'ActiveSheet.Range("L11").Value = thesheetname
        'ActiveSheet.Range("L12").Value = Len(thesheetname)
        'ActiveSheet.Range("L13").Value = Len(Selection.ShapeRange(1).TextFrame2.TextRange.Text)
            
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(titlecaption))
            .ParagraphFormat.TextDirection = msoTextDirectionLeftToRight
            .ParagraphFormat.FirstLineIndent = 0
            .ParagraphFormat.Alignment = msoAlignLeft
            .Font.NameComplexScript = "+mn-cs"
            .Font.NameFarEast = "+mn-ea"
            .Font.Fill.Visible = msoTrue
            .Font.Fill.ForeColor.RGB = RGB(255, 255, 255)
            .Font.Fill.Transparency = 0
            .Font.Fill.Solid
            .Font.Italic = msoFalse
            .Font.Name = "Sky InfoText Rg"
            .Font.Strike = msoNoStrike
            .Font.Bold = msoFalse
            .Font.Size = 9
        End With
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(thesheetname)).Font
            .Bold = msoTrue
            .Size = 14
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' B08: Finishing off: ''''''''''''''''''
    
    ' Hide all the sheet titles:
    ActiveWindow.DisplayWorkbookTabs = False
    
    ' To dodge that thing where it asks you if you want to preserve the clipboard;
    wbk.Worksheets("Box Type and Premiums").Range("E12").Copy
    
    ' Because we want to start on the summary tab
    wbk.Worksheets("Summary").Activate
    
    ' Okay, now save and close that workbook...
    wbk.Close SaveChanges:=True
    
End Sub

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''             B21: SKY VIEW OPERATIONAL DASHBOARD (SVDASH)              ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

' This guy is mostly based on the Vespa build, though significantly cut down as
' there are things we just don't know about the Sky View panel yet.

Public Sub GenerateReport_SkyView_Dashboard()
    ' A bunch of variables we need...
    Dim records_start_at    As Integer      ' Position in the final report that records begin for some table
    Dim records_count       As Integer      ' Number of records the table has to be expanded to fit
    Dim prior_records_count As Integer      ' The number of records already in the report (from a prior build)
    Dim line_counter        As Integer
    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim externalloaddate    As String       ' The date bit of an external file we're loading (Weekly KPI page)
    Dim externalfilepath    As String       ' Full name of an external file (Weekly KPI page)
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim newsource           As Workbook     ' For loading the daily KPI file stuff
    Dim column_placement    As Integer      ' For trackign the column that the KPI stuff is going into
    Dim sht                 As Worksheet
    Dim holding             As Worksheet

    todaysdate = Date
    
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Sky View Panel Dashboard (TEMPLATE).xls", _
             ThisWorkbook.Path & "\Sky View Dashboard " & Format(todaysdate, "yyyymmdd") & ".xls"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Sky View Dashboard " & Format(todaysdate, "yyyymmdd") & ".xls")
    
    codepath = settings_sheet.Range("B11") & "\sky_view_dashboard\"

    wbk.Worksheets.Add().Name = "Staging_Area"
    
    '''''''''''''''''' Opt Out Sheet: ''''''''''''''''''
    
    ' Opt out not produced in the Sky View panel build...
   
    '''''''''''''''''' B23: Daily Summary Sheet: ''''''''''''''''''
    wbk.Worksheets("Daily Summary").Activate
    
    ' We do this guy before the Weekly KPI files because we want to know what the last
    ' date of data extraction is / was so we can look for the right daily files.
    
    ' This guy is query 10 because he was aded last, even though he gets used 4th. It's always 7 days worth of data...
    wbk.Worksheets("Staging_Area").Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dash 03-10 daily reporting proportions.sql"))
    
    ' But now it has to go in in a manner that dodges those other percentages in the middle;
    wbk.Worksheets("Staging_Area").Range("A1:K7").Copy
    wbk.Worksheets("Daily Summary").Range("B9").PasteSpecial xlPasteValues
    
    ' The chunk to the right of those three percentages:
    wbk.Worksheets("Staging_Area").Range("L1:Z7").Copy
    wbk.Worksheets("Daily Summary").Range("P9").PasteSpecial xlPasteValues
   
    wbk.Worksheets("Staging_Area").Cells.Clear
    
    prior_records_count = wbk.Worksheets("Daily Summary").Range("B21:B2500").Find("", , , xlWhole).Row ' Not quite the count, but the end of the previously filled range...
    wbk.Worksheets("Staging_Area").Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dash 03-05 daily summary.sql"))
    records_count = wbk.Worksheets("Staging_Area").Range("A1:A2500").Find("", , , xlWhole).Row + 21       ' New end of the content, with adjustment for vertical table offset
    ' So now the data is in and we know how many rows were before and after, insert
    ' rows as required until the table is the right size
    Rows(prior_records_count - 6 & ":" & prior_records_count - 6).Select
    line_counter = prior_records_count + 1
    Do While line_counter < records_count
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop
    ' And bring down those formulas too;
    Range("F" & prior_records_count - 7 & ":G" & prior_records_count - 7).Select
    Selection.AutoFill Destination:=Range("F" & prior_records_count - 7 & ":G" & records_count - 7), Type:=xlFillSeries
    
    ' The other newer formulas too (which act on the second chunk of data with the enablement numbers and P/S split
    Range("N" & prior_records_count - 7 & ":P" & prior_records_count - 7).Select
    Selection.AutoFill Destination:=Range("N" & prior_records_count - 7 & ":P" & records_count - 7), Type:=xlFillSeries
    
    ' Now the formats are ready, go and push the first block of data in
    wbk.Worksheets("Staging_Area").Range("A1:D" & records_count - 1).Copy
    wbk.Worksheets("Daily Summary").Range("B21").PasteSpecial xlPasteValues
    
    ' And then the second block which has the Enablement, the P/S split, and the anytime breakdown
    wbk.Worksheets("Staging_Area").Range("E1:J" & records_count - 1).Copy
    wbk.Worksheets("Daily Summary").Range("H21").PasteSpecial xlPasteValues

    ' Pull the most recent date out of the daily stuffs....
    
    '''''''''''''''''' Import all the externally hosted Weekly KPI files ''''''''''''''''''
    
    ' Nope, no external file sources for the Sky View build of this guy.
    
    '''''''''''''''''' B25: Weekly Enablement sheet: ''''''''''''''''''
    
    ' Turns out there *is* some enablement statistics for the Sky View panel; but it's going to
    ' be degenerate, only one row of data, going to have to patch this manually probably...
    wbk.Worksheets("Weekly Enablement").Activate
    
    Set holding = wbk.Worksheets("Staging_Area")
    Set sht = wbk.Worksheets("Weekly Enablement")
    
    'OK so this section got some major commenting out because we only have one date of enablement
    ' for Sky View Panel, and it'll be that way for a while, so when we get the new data source
    ' we can look into how this stuff shuld be behaving. But 'till then, hardccde the 1 row result.
    
    holding.Cells.Clear
    holding.Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dash 03-06 summary by box.sql"))

    ' Now push the right values into the right places... hardcoded since we have only 1 enablement date.
    holding.Range("A1:F1").Copy
    sht.Range("B10").PasteSpecial xlPasteValues
    
    holding.Range("J1:S1").Copy
    sht.Range("K10").PasteSpecial xlPasteValues

    ' Now do the same with the subsequent reports...
    holding.Cells.Clear
    holding.Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dash 03-07 summary by accounts.sql"))

    ' Now push the right values into the right places...
    holding.Range("A1:J1").Copy
    sht.Range("B16").PasteSpecial xlPasteValues
    
    holding.Range("M1:AH1").Copy
    sht.Range("O16").PasteSpecial xlPasteValues
    
    ' Yup, the cumulative bot at the bottom dissappears too since it's gonna be 1 row, static, etc.
    
    ' Now we're done, remove the working sheet
    Application.DisplayAlerts = False
    wbk.Worksheets("Staging_Area").Delete
    Application.DisplayAlerts = True
    ' So the recomended way to dodge the popup is to blanket deny all popups and
    ' then re-enable them afterwards? sure. Whatever
    
    '''''''''''''''''' B26: Box Type & Premiums sheet: ''''''''''''''''''
    wbk.Worksheets("Box Type and Premiums").Activate

    wbk.Worksheets("Box Type and Premiums").Range("B10").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dash 03-08 box type.sql"))
    wbk.Worksheets("Box Type and Premiums").Range("B30").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dash 03-09 premiums.sql"))
    ' And that other stuff about recolouring the graph bars and updating the weights around the sides...

    ' So... just need to figure out what these text box names are...
    With wbk.Worksheets("Box Type and Premiums")
        .Shapes.Range(Array("D")).Select
        Selection.Characters.Text = .Range("D25").Value
        .Shapes.Range(Array("multi")).Select
        Selection.Characters.Text = .Range("D24").Value
        .Shapes.Range(Array("B")).Select
        Selection.Characters.Text = .Range("D23").Value
        .Shapes.Range(Array("A")).Select
        Selection.Characters.Text = .Range("D22").Value
        .Shapes.Range(Array("FDB_label")).Select
        Selection.Characters.Text = .Range("D21").Value
        .Shapes.Range(Array("multiroom_label")).Select
        Selection.Characters.Text = .Range("D20").Value
        .Shapes.Range(Array("skyplus_label")).Select
        Selection.Characters.Text = .Range("D19").Value
        .Shapes.Range(Array("skypluscombi_label")).Select
        Selection.Characters.Text = .Range("D18").Value
        .Shapes.Range(Array("Label 3506")).Select
        Selection.Characters.Text = .Range("D17").Value
        .Shapes.Range(Array("Label 3507")).Select
        Selection.Characters.Text = .Range("D16").Value
        .Shapes.Range(Array("kdx1tb_label")).Select
        Selection.Characters.Text = .Range("D15").Value
        .Shapes.Range(Array("Label 3509")).Select
        Selection.Characters.Text = .Range("D14").Value
        .Shapes.Range(Array("Label 3510")).Select
        Selection.Characters.Text = .Range("D13").Value
        .Shapes.Range(Array("Label 3511")).Select
        Selection.Characters.Text = .Range("D12").Value
        .Shapes.Range(Array("Label 3512")).Select
        Selection.Characters.Text = .Range("D11").Value
        .Shapes.Range(Array("Label 3513")).Select
        Selection.Characters.Text = .Range("D10").Value
        .Shapes.Range(Array("Label 3515")).Select
        Selection.Characters.Text = .Range("D36").Value
        .Shapes.Range(Array("Label 3518")).Select
        Selection.Characters.Text = .Range("D35").Value
        .Shapes.Range(Array("Label 3519")).Select
        Selection.Characters.Text = .Range("D34").Value
        .Shapes.Range(Array("Label 3523")).Select
        Selection.Characters.Text = .Range("D33").Value
        .Shapes.Range(Array("Label 3520")).Select
        Selection.Characters.Text = .Range("D32").Value
        .Shapes.Range(Array("Label 3521")).Select
        Selection.Characters.Text = .Range("D31").Value
        .Shapes.Range(Array("Label 3522")).Select
        Selection.Characters.Text = .Range("D30").Value
        .Range("C8").Select
        ' Thought these had all been renamed but apparently they didn't take. Oh well, figured out
        ' their names by recording a macro, now I don't care to change them.
        ' And yeah, tried these by tacking 'Characters.Text' on the end of
        ' the Select but apparently that's a method which sets a global or
        ' something, can't figure out how to make those character settings
        ' explicit. There are other hacks we could try, but yeah, don't so
        ' much care about it.
    End With
    
    'To toop through the cells and colour the graph accordingly:
    line_counter = 1
    wbk.Worksheets("Box Type and Premiums").ChartObjects("Chart 21").Activate
    Do While line_counter <= 16 ' There are 12 box types
        ActiveChart.SeriesCollection(1).Points(line_counter).Select
        With Selection.Format.Fill
            .Visible = msoTrue
            .Transparency = 0
            .Solid
            If Range("H" & line_counter + 9).Value < Range("M9").Value Then
                    .ForeColor.RGB = RGB(255, 0, 0)
            ElseIf Range("H" & line_counter + 9).Value > Range("N9").Value Then
                    .ForeColor.RGB = RGB(146, 208, 80)
            Else
                    .ForeColor.RGB = RGB(255, 192, 0)
            End If
        End With
        
        line_counter = line_counter + 1
    Loop
    
    line_counter = 1
    wbk.Worksheets("Box Type and Premiums").ChartObjects("Chart 22").Activate
    Do While line_counter <= 5 ' There are 5 packages in the Sky View version
        ActiveChart.SeriesCollection(1).Points(line_counter).Select
        With Selection.Format.Fill
            .Visible = msoTrue
            .Transparency = 0
            .Solid
            If Range("H" & line_counter + 25).Value < Range("M9").Value Then
                    .ForeColor.RGB = RGB(255, 0, 0)
            ElseIf Range("H" & line_counter + 25).Value > Range("N9").Value Then
                    .ForeColor.RGB = RGB(146, 208, 80)
            Else
                    .ForeColor.RGB = RGB(255, 192, 0)
            End If
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' B27: The dates across the top of each sheet... ''''''''''''''''''
    
    ' This section: ugly, messy, and also, we're resetting the cursors to somewhere
    ' nice instead of them defaulting to having a bunch of stuff selected.

    ' To put the report period into the headers, we also need to figure out what the
    ' report end date is... in this report, it's also in the daily summary tab:
    report_end_date = wbk.Worksheets("Daily Summary").Range("C15").Value
    
    Dim thesheetname As String
    Dim titlecaption As String
    
    line_counter = 1
    Do While line_counter <= 5 ' There are 5 tabs
        If line_counter = 1 Then
            wbk.Worksheets("Summary").Activate
            thesheetname = "Sky View Dashboard: Summary"
        ElseIf line_counter = 2 Then
            wbk.Worksheets("Weekly Enablement").Activate
            thesheetname = "Sky View Dashboard: Enablement"
        ElseIf line_counter = 3 Then
            wbk.Worksheets("Daily Summary").Activate
            thesheetname = "Sky View Dashboard: Daily Summary"
        ElseIf line_counter = 4 Then
            wbk.Worksheets("Box Type and Premiums").Activate
            thesheetname = "Sky View Dashboard: Box Type and Premiums"
        ElseIf line_counter = 5 Then
            wbk.Worksheets("Glossary").Activate
            thesheetname = "Sky View Dashboard: Glossary"
        End If

        Range("B7").Select
        
        ActiveSheet.Shapes.Range(Array("shpTitle")).Select
        titlecaption = thesheetname & Chr(10) & "Report created on " & Format(todaysdate, "dd/mm/yyyy") & " covering period " & Format(DateAdd("d", -6, report_end_date), "dd/mm/yyyy") & " to " & Format(report_end_date, "dd/mm/yyyy") & "."
        Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = titlecaption
            
        'ActiveSheet.Range("L11").Value = thesheetname
        'ActiveSheet.Range("L12").Value = Len(thesheetname)
        'ActiveSheet.Range("L13").Value = Len(Selection.ShapeRange(1).TextFrame2.TextRange.Text)
            
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(titlecaption))
            .ParagraphFormat.TextDirection = msoTextDirectionLeftToRight
            .ParagraphFormat.FirstLineIndent = 0
            .ParagraphFormat.Alignment = msoAlignLeft
            .Font.NameComplexScript = "+mn-cs"
            .Font.NameFarEast = "+mn-ea"
            .Font.Fill.Visible = msoTrue
            .Font.Fill.ForeColor.RGB = RGB(255, 255, 255)
            .Font.Fill.Transparency = 0
            .Font.Fill.Solid
            .Font.Italic = msoFalse
            .Font.Name = "Sky InfoText Rg"
            .Font.Strike = msoNoStrike
            .Font.Bold = msoFalse
            .Font.Size = 9
        End With
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(thesheetname)).Font
            .Bold = msoTrue
            .Size = 14
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' B28: Finishing off: ''''''''''''''''''
    
    ' Hide all the sheet titles:
    ActiveWindow.DisplayWorkbookTabs = False
    
    ' To dodge that thing where it asks you if you want to preserve the clipboard;
    wbk.Worksheets("Box Type and Premiums").Range("E12").Copy
    
    ' Because we want to start on the summary tab
    wbk.Worksheets("Summary").Activate
    
    ' Okay, now save and close that workbook...
    wbk.Close SaveChanges:=True
    
End Sub


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                      C01: DIALBACK REPORT (DBR)                       ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public Sub GenerateReport_Vespa_Dialback()
    ' Stuff we need, of course...
    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim sht                 As Worksheet

    todaysdate = Date
    
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Vespa Dialback Report (TEMPLATE).xlsx", _
             ThisWorkbook.Path & "\Vespa Dialback Report " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Vespa Dialback Report " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    codepath = settings_sheet.Range("B11") & "\dialback_report\"
    
    '''''''''''''''''' C02: There is no section C02 ''''''''''''''''''
    
    ' Well, C08 used to be C02 but it had to be moved to accomodate putting the reporting
    ' period in the title bars. This is just here to mmitigate concern section C02 is gone.
    
    '''''''''''''''''' C03: Basic reports of total logs, distinct days, interval length ''''''''''''''''''
    
    ' Think we can just go and paste these things in? but... we probably need to blank
    ' the things first so that we don't get old stuff spilling over...
   
    ' Yeah, awesome, doing this the fast hacky way with Activate again...
    With wbk
        wbk.Worksheets("30 day total raw").Activate
        Blank_Holding_page
        wbk.Worksheets("7 day total raw").Activate
        Blank_Holding_page
        wbk.Worksheets("30 day distinct raw").Activate
        Blank_Holding_page
        wbk.Worksheets("7 day distinct raw").Activate
        Blank_Holding_page
        wbk.Worksheets("30 day interval raw").Activate
        Blank_Holding_page
        wbk.Worksheets("7 day interval raw").Activate
        Blank_Holding_page
    End With
    ' We did have it with function calls passing in the worksheet as an object, but VBA didn't
    ' like it :-/ If the language doesn't give me usefull error messages, it's going to end up
    ' with hacky experimental ugly code, if that happens to work :(
   

    ' And now it's ready for the new data:
    wbk.Worksheets("30 day total raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-01 pivot pull 30d total.sql"))
    wbk.Worksheets("7 day total raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-02 pivot pull 7d total.sql"))
    wbk.Worksheets("30 day distinct raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-03 pivot pull 30d distinct.sql"))
    wbk.Worksheets("7 day distinct raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-04 pivot pull 7d distinct.sql"))
    wbk.Worksheets("30 day interval raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-05 pivot pull 30d intervals.sql"))
    wbk.Worksheets("7 day interval raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-06 pivot pull 7d intervals.sql"))

    
    ' Then refresh all the pivots... OK, that's now set up to happen on file opening? Awesome.
    
    '''''''''''''''''' C04: Then looking at the transition probability independence stuff ''''''''''''''''''
    ' This report has been pulled from the build. We're going to leave the bits in the template, but just not update it....
    'wbk.Worksheets("Independence raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-11 report independence.sql"))
    ' And that automatically goes into the graph on the visible tab...
    
    '''''''''''''''''' Are we doing the other graph based stuff? logs returned vs interval length? ''''''''''''''''''
    
    ' heh, not yet.
    
    '''''''''''''''''' C05: Getting the other enablement totals for the Overview page ''''''''''''''''''
    wbk.Worksheets("Overview").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-12 enablement 30d.sql"))
    wbk.Worksheets("Overview").Range("C13").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-13 enablement 7d.sql"))
    wbk.Worksheets("Overview").Range("C9").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-14 confirmed 30d.sql"))
    wbk.Worksheets("Overview").Range("C14").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-15 confirmed 7d.sql"))
    
    '''''''''''''''''' C06: Chart of number of events in received logs ''''''''''''''''''
    
    wbk.Worksheets("Event counts raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-17 events in logs.sql"))
    'Similarly, this auto-refreshes on file open.

    '''''''''''''''''' C07: Chart of time of day when boxes dial back ''''''''''''''''''
    
    wbk.Worksheets("Time of log return raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa Dialback 03-18 time of log reception.sql"))
    'And then it gets auto-refreshed into the pivot and chart when the file opens.

    '''''''''''''''''' C08: Re-date-ing the title bars on each active page ''''''''''''''''''
    
    ' OK, now at the end of the report because we need to have all the data in so we can say
    ' what the reporting period is for this run:
    report_end_date = wbk.Worksheets("Overview").Range("B27").Value
    ' Heh, summarising that value from the logs returned page in a kind of nasty silent hack.
    ' Maybe we should put those things not as white font on white background but on a separate
    ' for control totals or things? This build also quite unpleasantly assumes that there were
    ' logs returned on the last day of the reporting period...
    
    ' There are 9 visible tabs
    Dim sheetnames(1 To 11) As String
    Dim sheettitles(1 To 11) As String
    Dim titlecaption As String
    
    sheetnames(1) = "Overview"
    sheettitles(1) = "Vespa dialback report: Overview"
    sheetnames(2) = "30 day total logs"
    sheettitles(2) = "Vespa dialback report: Data returned over 30 days"
    sheetnames(3) = "30 day distinct"
    sheettitles(3) = "Vespa dialback report: Different days on which data is returned"
    sheetnames(4) = "30 day intervals"
    sheettitles(4) = "Vespa dialback report: Longest continuous reporting for each box"
    sheetnames(5) = "7 day total logs"
    sheettitles(5) = "Vespa dialback report: Data returned over 7 days"
    sheetnames(6) = "7 day distinct"
    sheettitles(6) = "Vespa dialback report: Different days on which data is returned"
    sheetnames(7) = "7 day intervals"
    sheettitles(7) = "Vespa dialback report: Longest continuous reporting for each box"
    sheetnames(8) = "Independence assumptions"                                                  ' No longer in play but sheet is hidden
    sheettitles(8) = "Vespa dialback report: Statistical independence of returning data"        ' No longer in play but sheet is hidden
    sheetnames(9) = "Glossary"
    sheettitles(9) = "Vespa dialback report: Glossary"
    sheetnames(10) = "Event counts"
    sheettitles(10) = "Vespa dialback report: Profiling the number of events in a log"
    sheetnames(11) = "Time of day of reporting"
    sheettitles(11) = "Vespa dialback report: Time of day when boxes dial back"
    
    Dim line_counter As Integer
    line_counter = 1
    
    Do While line_counter <= 11
        
        wbk.Worksheets(sheetnames(line_counter)).Activate
        Range("B7").Select
        
        ActiveSheet.Shapes.Range(Array("shpTitle")).Select
        titlecaption = sheettitles(line_counter) & Chr(10) & "Report created on " & Format(todaysdate, "dd/mm/yyyy") & " covering period " & Format(DateAdd("d", -29, report_end_date), "dd/mm/yyyy") & " to " & Format(report_end_date, "dd/mm/yyyy") & "."
        Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = titlecaption
            
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(titlecaption))
            .ParagraphFormat.TextDirection = msoTextDirectionLeftToRight
            .ParagraphFormat.FirstLineIndent = 0
            .ParagraphFormat.Alignment = msoAlignLeft
            .Font.NameComplexScript = "+mn-cs"
            .Font.NameFarEast = "+mn-ea"
            .Font.Fill.Visible = msoTrue
            .Font.Fill.ForeColor.RGB = RGB(255, 255, 255)
            .Font.Fill.Transparency = 0
            .Font.Fill.Solid
            .Font.Italic = msoFalse
            .Font.Name = "Sky InfoText Rg"
            .Font.Strike = msoNoStrike
            .Font.Bold = msoFalse
            .Font.Size = 9
        End With
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(sheettitles(line_counter))).Font
            .Bold = msoTrue
            .Size = 14
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' C09: Tidying stuff up ''''''''''''''''''
    
    ' Hide all the tab names
    ActiveWindow.DisplayWorkbookTabs = False
    
    ' To dodge that thing where it asks you if you want to preserve the clipboard;
    wbk.Worksheets("Overview").Range("A1").Copy
    
    'But then: moving the cursor to a navigable page
    wbk.Worksheets("Overview").Activate
    ActiveSheet.Range("A1").Select

    ' Okay, now save and close that workbook...
    wbk.Close SaveChanges:=True
    
End Sub


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                 C21: SKY VIEW DIALBACK REPORT (SVDB)                  ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

' So this guy is largely a slightly different cobbled together version of the
' Vespa Dialback report that was built up in C01. Starts with a copy-paste, and
' then some bits get changed. Kind of annoying to maintain them side by side,
' but hey, whatever.


Public Sub GenerateReport_SkyView_Dialback()
    ' Stuff we need, of course...
    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim sht                 As Worksheet

    todaysdate = Date
    
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Sky View Panel Dialback Report (TEMPLATE).xlsx", _
             ThisWorkbook.Path & "\Sky View Dialback Report " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Sky View Dialback Report " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    codepath = settings_sheet.Range("B11") & "\sky_view_dialback\"
    
    '''''''''''''''''' C22: There is no section C22 ''''''''''''''''''
    
    ' Well, C08 used to be C22 but it had to be moved to accomodate putting the reporting
    ' period in the title bars. This is just here to mmitigate concern section C02 is gone.
    
    '''''''''''''''''' C23: Basic reports of total logs, distinct days, interval length ''''''''''''''''''
    
    ' Think we can just go and paste these things in? but... we probably need to blank
    ' the things first so that we don't get old stuff spilling over...
   
    ' Yeah, awesome, doing this the fast hacky way with Activate again...
    With wbk
        wbk.Worksheets("30 day total raw").Activate
        Blank_Holding_page
        wbk.Worksheets("7 day total raw").Activate
        Blank_Holding_page
        wbk.Worksheets("30 day distinct raw").Activate
        Blank_Holding_page
        wbk.Worksheets("7 day distinct raw").Activate
        Blank_Holding_page
        wbk.Worksheets("30 day interval raw").Activate
        Blank_Holding_page
        wbk.Worksheets("7 day interval raw").Activate
        Blank_Holding_page
    End With
    ' We did have it with function calls passing in the worksheet as an object, but VBA didn't
    ' like it :-/ If the language doesn't give me usefull error messages, it's going to end up
    ' with hacky experimental ugly code, if that happens to work :(
   

    ' And now it's ready for the new data:
    wbk.Worksheets("30 day total raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-01 pivot pull 30d total.sql"))
    wbk.Worksheets("7 day total raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-02 pivot pull 7d total.sql"))
    wbk.Worksheets("30 day distinct raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-03 pivot pull 30d distinct.sql"))
    wbk.Worksheets("7 day distinct raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-04 pivot pull 7d distinct.sql"))
    wbk.Worksheets("30 day interval raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-05 pivot pull 30d intervals.sql"))
    wbk.Worksheets("7 day interval raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-06 pivot pull 7d intervals.sql"))

    
    ' Then refresh all the pivots... OK, that's now set up to happen on file opening? Awesome.
    
    '''''''''''''''''' C24: Then looking at the transition probability independence stuff ''''''''''''''''''
    ' This report has been pulled from the build. We're going to leave the bits in the template, but just not update it....
    'wbk.Worksheets("Independence raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-11 report independence.sql"))
    ' And that automatically goes into the graph on the visible tab...
    
    '''''''''''''''''' Are we doing the other graph based stuff? logs returned vs interval length? ''''''''''''''''''
    
    ' heh, not yet.
    
    '''''''''''''''''' C25: Getting the other enablement totals for the Overview page ''''''''''''''''''
    wbk.Worksheets("Overview").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-12 enablement 30d.sql"))
    wbk.Worksheets("Overview").Range("C13").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-13 enablement 7d.sql"))
    'No feed for selected returning boxes yet, leave that pull out
    'wbk.Worksheets("Overview").Range("C9").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-14 selected 30d.sql"))
    'wbk.Worksheets("Overview").Range("C14").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-15 selected 7d.sql"))
    
    '''''''''''''''''' C26: Chart of number of events in received logs ''''''''''''''''''
    
    wbk.Worksheets("Event counts raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-17 events in logs.sql"))
    'And then it gets auto-refreshed into the pivot and chart when the file opens.
    
    ' Hacky: when the pivot refreshes, it'll break the formatting around the table. But we
    ' have to refresh the pivot table first, otherwise the fix does nothing;
    Set sht = wbk.Worksheets("Event counts")
    sht.Activate
    ActiveSheet.PivotTables("PivotTable1").PivotCache.Refresh
    
    ' Get the row with the totals in it, so we know where the new borders go:
    Dim uglyhack As Integer
    uglyhack = sht.Range("B12").End(xlDown).Row
    
    ' Grab the left hand column (with the axis values) and apply the bordering
    sht.Range("B12:B" & uglyhack - 1).Select
    Format_Borders
    
    ' Grab the right hand column (with the totals) and apply the formatting
    sht.Range("E12:E" & uglyhack - 1).Select
    Format_Borders
    
    ' For some reason, the inner columns don't end up broken. Awesome. I love consistency.

    '''''''''''''''''' C27: Chart of time of day when boxes dial back ''''''''''''''''''
    
    wbk.Worksheets("Time of log return raw").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Sky View Dialback 03-18 time of log reception.sql"))
    'Similarly, this auto-refreshes on file open.
    
    '''''''''''''''''' C28: Re-date-ing the title bars on each active page ''''''''''''''''''
    
    ' OK, now at the end of the report because we need to have all the data in so we can say
    ' what the reporting period is for this run. Same ugly chack as the Vespa dialback:
    report_end_date = wbk.Worksheets("Overview").Range("B27").Value
    
    ' There are 9 visible tabs
    Dim sheetnames(1 To 11) As String
    Dim sheettitles(1 To 11) As String
    Dim titlecaption As String
    
    sheetnames(1) = "Overview"
    sheettitles(1) = "Sky View dialback report: Overview"
    sheetnames(2) = "30 day total logs"
    sheettitles(2) = "Sky View dialback report: Data returned over 30 days"
    sheetnames(3) = "30 day distinct"
    sheettitles(3) = "Sky View dialback report: Different days on which data is returned"
    sheetnames(4) = "30 day intervals"
    sheettitles(4) = "Sky View dialback report: Longest continuous reporting for each box"
    sheetnames(5) = "7 day total logs"
    sheettitles(5) = "Sky View dialback report: Data returned over 7 days"
    sheetnames(6) = "7 day distinct"
    sheettitles(6) = "Sky View dialback report: Different days on which data is returned"
    sheetnames(7) = "7 day intervals"
    sheettitles(7) = "Sky View dialback report: Longest continuous reporting for each box"
    sheetnames(8) = "Independence assumptions"                                                  ' No longer in play but sheet is hidden
    sheettitles(8) = "Sky View dialback report: Statistical independence of returning data"     ' No longer in play but sheet is hidden
    sheetnames(9) = "Glossary"
    sheettitles(9) = "Sky View dialback report: Glossary"
    sheetnames(10) = "Event counts"
    sheettitles(10) = "Sky View dialback report: Profiling the number of events in a log"
    sheetnames(11) = "Time of day of reporting"
    sheettitles(11) = "Sky View dialback report: Time of day when boxes dial back"
    
    Dim line_counter As Integer
    line_counter = 1
    
    Do While line_counter <= 11
        
        wbk.Worksheets(sheetnames(line_counter)).Activate
        Range("B7").Select
        
        ActiveSheet.Shapes.Range(Array("shpTitle")).Select
        titlecaption = sheettitles(line_counter) & Chr(10) & "Report created on " & Format(todaysdate, "dd/mm/yyyy") & " covering period " & Format(DateAdd("d", -29, report_end_date), "dd/mm/yyyy") & " to " & Format(report_end_date, "dd/mm/yyyy") & "."
        Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = titlecaption
            
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(titlecaption))
            .ParagraphFormat.TextDirection = msoTextDirectionLeftToRight
            .ParagraphFormat.FirstLineIndent = 0
            .ParagraphFormat.Alignment = msoAlignLeft
            .Font.NameComplexScript = "+mn-cs"
            .Font.NameFarEast = "+mn-ea"
            .Font.Fill.Visible = msoTrue
            .Font.Fill.ForeColor.RGB = RGB(255, 255, 255)
            .Font.Fill.Transparency = 0
            .Font.Fill.Solid
            .Font.Italic = msoFalse
            .Font.Name = "Sky InfoText Rg"
            .Font.Strike = msoNoStrike
            .Font.Bold = msoFalse
            .Font.Size = 9
        End With
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(sheettitles(line_counter))).Font
            .Bold = msoTrue
            .Size = 14
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' C29: Tidying stuff up ''''''''''''''''''
    
    ' Hide all the tab names
    ActiveWindow.DisplayWorkbookTabs = False
    
    ' To dodge that thing where it asks you if you want to preserve the clipboard;
    wbk.Worksheets("Overview").Range("A1").Copy
    
    'But then: moving the cursor to a navigable page
    wbk.Worksheets("Overview").Activate
    ActiveSheet.Range("A1").Select

    ' Okay, now save and close that workbook...
    wbk.Close SaveChanges:=True
    
End Sub


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                     D01: KEY VIEWING FACTS (KVF)                      ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


' Not yet in play. Might turn into the Share of Viewing report?


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                  E01: WEEKLY STATUS REPORT (WeStat)                   ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

' This guy is still in development, but we think mostly the structure will be
' the same from here on out.

Public Sub GenerateReport_WeStat()

    ' The same stuff we do at the beginning of all of these reports:
    Dim todaysdate          As String
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim sht                 As Worksheet
    Dim holding             As Worksheet
    Dim records_count       As Integer
    Dim line_counter        As Integer

    todaysdate = Date
    
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Vespa Weekly Status Report (TEMPLATE).xlsx", _
             ThisWorkbook.Path & "\Vespa Weekly Status Report " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Vespa Weekly Status Report " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    codepath = settings_sheet.Range("B11") & "\weekly_status_report\"

    '''''''''''''''''' E02: Vespa sheet ''''''''''''''''''
    
    ' Two things. Later the historical results table but right now the pivot that
    ' powers the main graph:
    
    wbk.Worksheets("Raw Vespa data").Range("A2").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa WeStat 03-01 - big for pivots.sql"))
    ' And then the graph refreshes automatically, right?
    
    ' Now with the same data inserting trick we used in the weekly enablement sheet
    ' of the Operational Dashboard: we'll put the raw data in a holding sheet, count
    ' the number of rows it has, and then insert new rows so that the formatting fits
    ' and shows the right form.
    
    Set holding = wbk.Worksheets("Staging_Area")
    holding.Cells.Clear
    
    ' Grab the data and figure out how many lines worth there is in it
    holding.Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa WeStat 03-02 - historical table.sql"))
    records_count = holding.Range("A1:A2500").Find("", , , xlWhole).Row         'Number of non-blank items in the new pull
    
    ' Row 36 is in the middle of the table, when we insert there it'll give us the formatting we want
    Rows("36:36").Select
    line_counter = 4 ' Template starts with 4 rows
    
    Do While line_counter < records_count
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop
    
    ' Now the formatting is ready, we can move in the data:
    holding.Range("A1:M" & records_count - 1).Copy
    wbk.Worksheets("State of Vespa panel").Range("B34").PasteSpecial xlPasteValues
    
    ' OK, and we're done here!

    '''''''''''''''''' E03: Sky View sheet (NYIP) ''''''''''''''''''
    
    ' NYIP stands for "Not Yet In Play"

    '''''''''''''''''' E04: Alternate panel sheets (NYIP) ''''''''''''''''''

    ' NYIP stands for "Not Yet In Play"

    '''''''''''''''''' E05: All the title bars ''''''''''''''''''

    ' There are 3 visible tabs... that might change later though.
    Dim sheetnames(1 To 3) As String
    Dim sheettitles(1 To 3) As String
    Dim titlecaption As String
    Dim report_end_date As Date
    
    sheetnames(1) = "State of Vespa panel"
    sheettitles(1) = "Vespa Panel: changes since last week"
    sheetnames(2) = "State of Sky View panel"
    sheettitles(2) = "Sky View Panel: changes since last week"
    sheetnames(3) = "Glossary"
    sheettitles(3) = "Panel Status Report: Glossary"

    line_counter = 1
    report_end_date = wbk.Worksheets("State of Vespa Panel").Range("B34").Value
    
    Do While line_counter <= 3
        
        wbk.Worksheets(sheetnames(line_counter)).Activate
        Range("B7").Select
        
        ActiveSheet.Shapes.Range(Array("shpTitle")).Select
        titlecaption = sheettitles(line_counter) & Chr(10) & "Report created on " & Format(todaysdate, "dd/mm/yyyy") & " covering period " & Format(DateAdd("d", -6, report_end_date), "dd/mm/yyyy") & " to " & Format(report_end_date, "dd/mm/yyyy") & "."
        Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = titlecaption
            
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(titlecaption))
            .ParagraphFormat.TextDirection = msoTextDirectionLeftToRight
            .ParagraphFormat.FirstLineIndent = 0
            .ParagraphFormat.Alignment = msoAlignLeft
            .Font.NameComplexScript = "+mn-cs"
            .Font.NameFarEast = "+mn-ea"
            .Font.Fill.Visible = msoTrue
            .Font.Fill.ForeColor.RGB = RGB(255, 255, 255)
            .Font.Fill.Transparency = 0
            .Font.Fill.Solid
            .Font.Italic = msoFalse
            .Font.Name = "Sky InfoText Rg"
            .Font.Strike = msoNoStrike
            .Font.Bold = msoFalse
            .Font.Size = 9
        End With
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(sheettitles(line_counter))).Font
            .Bold = msoTrue
            .Size = 14
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' E06: Tidying up ''''''''''''''''''
    
    ' Hide all the tab names
    ActiveWindow.DisplayWorkbookTabs = False
    
    ' To dodge that thing where it asks you if you want to preserve the clipboard;
    wbk.Worksheets("State of Vespa panel").Range("A1").Copy
    
    'But then: moving the cursor to a navigable page
    wbk.Worksheets("State of Vespa panel").Activate
    ActiveSheet.Range("A1").Select

    ' Okay, now save and close that workbook...
    wbk.Close SaveChanges:=True
    
End Sub



'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                 G01: PANEL MANAGEMENT REPORT (PanMan)                 ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

' Placeholder: still in dev, it'll be out eventually.
Public Sub GenerateReport_PanMan()

    '''''''''''''''''' Set the stuff up: preparing new version from the template etc ''''''''''''''''''
    
    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim holding             As Worksheet
    Dim records_count       As Integer
    Dim line_counter        As Integer
    
    todaysdate = Date
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Vespa Panel Management Report (TEMPLATE) +Scaling2.xlsx", _
             ThisWorkbook.Path & "\Vespa Panel Management Report " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Vespa Panel Management Report " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    codepath = settings_sheet.Range("B11") & "\panel_management\"
    
    '''''''''''''''''' G02: Summary tab of high level stuff ''''''''''''''''''
    
    ' So first we need the total number of accounts on the Sky Base...
    wbk.Worksheets("Summary").Range("E9").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-01 - Sky Base Account Count.sql"))
    
    ' We were thinking about transposing them, but we'll report into tables instead because there's
    ' also the other base totals and stuff which don't easily fit into one query.
    wbk.Worksheets("Summary").Range("F9").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-02 - Vespa live panel overall.sql"))
    wbk.Worksheets("Summary").Range("H9").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-03 - Alternate panel 6 overall.sql"))
    wbk.Worksheets("Summary").Range("J9").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-04 - Alternate panel 7 overall.sql"))
    
    ' And then the subsequent similar thing about the Vespa reporting quality split over universes
    wbk.Worksheets("Summary").Range("E17").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-01 - Sky Base Account Count.sql"))
    wbk.Worksheets("Summary").Range("F17").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-42 - Vespa panel single box HH reporting.sql"))
    wbk.Worksheets("Summary").Range("H17").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-44 - Vespa panel multi box HH reporting.sql"))
    
    ' Then the other part of the summary graph is the historical plot of the completeness metrics,
    ' and again the trick with putting the stuff on a holding tab and counting the new records...
    Set holding = wbk.Worksheets("Staging_Area")
    holding.Cells.Clear
    
    ' Grab the data and figure out how many lines worth there is in it
    holding.Range("A1").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-05 - Data completeness metrics.sql"))
    records_count = holding.Range("A1:A2500").Find("", , , xlWhole).Row         'Number of non-blank items in the new pull
    
    ' Row 21 is in the middle of the table, when we insert there it'll give us the formatting we want
    wbk.Worksheets("Summary").Select
    Rows("28:28").Select
    line_counter = 4 ' Template starts with 4 rows
    
    Do While line_counter < records_count - 1
        Selection.Insert Shift:=xlDown, CopyOrigin:=xlFormatFromLeftOrAbove
        line_counter = line_counter + 1
    Loop
    
    ' Now the formatting is ready, we can move in the data:
    holding.Range("A1:D" & records_count - 1).Copy
    wbk.Worksheets("Summary").Range("B26").PasteSpecial xlPasteValues
    
    '''''''''''''''''' G03: Traffic lights worksheet(s) ''''''''''''''''''
    
    ' So it's a simple data dump into the right place... (hopefully the formatting doesn't get overwritten)
    wbk.Worksheets("Traffic lights").Range("D11").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-09 - Traffic lights.sql"))
    
    '''''''''''''''''' G04: Single Variable analysts worksheet(s) ''''''''''''''''''

    ' There are six worksheets, but they're all set up to be simple data dumps...
    wbk.Worksheets("Vespa scaling variables").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-40 - Vespa panel by each variable.sql"))
    wbk.Worksheets("Vespa non-scaling variables").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-41 - Vespa panel by each non-scaling variable.sql"))

    ' Panels 6 and 7 not in play yet, don't bother pulling anything out

    wbk.Worksheets("Alternate 6 scaling").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-60 - Alternate panel 6 by each variable.sql"))
    wbk.Worksheets("Alternate 6 non-scaling").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-61 - Alternate panel 6 by each non-scaling variable.sql"))
    wbk.Worksheets("Alternate 7 scaling").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-70 - Alternate panel 7 for each variable.sql"))
    wbk.Worksheets("Alternate 7 non-scaling").Range("C8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-71 - Alternate panel 7 for each non-scaling variable.sql"))
    
    '''''''''''''''''' G05: Over- and under- representation of particular cells ''''''''''''''''''
    
    wbk.Worksheets("Top 30 under-represented").Range("B8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-06 - Empty segments.sql"))
    wbk.Worksheets("Top 30 over-represented").Range("B8").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-07 - Over-represented segments.sql"))
    
    '''''''''''''''''' G06: Box Swing-ish-ness things ''''''''''''''''''
    
    '' Just the one dump here, the rest is just automatic updates of some graph
    wbk.Worksheets("Weightings Raw").Range("B3").CopyFromRecordset conn.Execute(GetSQL(codepath & "Vespa PanMan 03-08 - Profile of Box Swing.sql"))

    '''''''''''''''''' G07: Setting the titles and the cell selection bit ''''''''''''''''''
    
    ' There are 13 visible tabs...
    Dim sheetnames(1 To 13) As String
    Dim sheettitles(1 To 13) As String
    Dim titlecaption As String
    
    sheetnames(1) = "Summary"
    sheettitles(1) = "Vespa Panel Management Report: Overview"
    sheetnames(2) = "Traffic lights"
    sheettitles(2) = "Vespa Panel Management Report: Traffic lights"
    sheetnames(3) = "Vespa scaling variables"
    sheettitles(3) = "Vespa Panel Management Report: Live Vespa panel by each scaling variable"
    sheetnames(4) = "Vespa non-scaling variables"
    sheettitles(4) = "Vespa Panel Management Report: Live Vespa panel by non-scaling variables"
    sheetnames(5) = "Alternate 6 scaling"
    sheettitles(5) = "Vespa Panel Management Report: Alternate panel 6 by each scaling variable"
    sheetnames(6) = "Alternate 6 non-scaling"
    sheettitles(6) = "Vespa Panel Management Report: Alternate panel 6 by non-scaling variables"
    sheetnames(7) = "Alternate 7 scaling"
    sheettitles(7) = "Vespa Panel Management Report: Alternate panel 7 by each scaling variable"
    sheetnames(8) = "Alternate 7 non-scaling"
    sheettitles(8) = "Vespa Panel Management Report: Alternate panel 7 by non-scaling variables"
    sheetnames(9) = "Top 30 under-represented"
    sheettitles(9) = "Vespa Panel Management Report: Top 30 under-represented segments in Vespa"
    sheetnames(10) = "Top 30 over-represented"
    sheettitles(10) = "Vespa Panel Management Report: Top 30 over-represented segments in Vespa "
    sheetnames(11) = "Weightings profile"
    sheettitles(11) = "Vespa Panel Management Report: Profile of scaling weights by household"
    sheetnames(12) = "Redundancy"
    sheettitles(12) = "Vespa Panel Management Report: Overview of panel redundancy"
    sheetnames(13) = "Glossary"
    sheettitles(13) = "Vespa Panel Management Report: Glossary"

    line_counter = 1
    
    report_end_date = wbk.Worksheets("Summary").Range("B26").Value
    
    Do While line_counter <= 13
        
        wbk.Worksheets(sheetnames(line_counter)).Activate
        Range("B7").Select
        
        ActiveSheet.Shapes.Range(Array("shpTitle")).Select
        titlecaption = sheettitles(line_counter) & Chr(10) & "Report created on " & Format(todaysdate, "dd/mm/yyyy") & " covering period " & Format(DateAdd("d", -6, report_end_date), "dd/mm/yyyy") & " to " & Format(report_end_date, "dd/mm/yyyy") & "."
        Selection.ShapeRange(1).TextFrame2.TextRange.Characters.Text = titlecaption
            
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(titlecaption))
            .ParagraphFormat.TextDirection = msoTextDirectionLeftToRight
            .ParagraphFormat.FirstLineIndent = 0
            .ParagraphFormat.Alignment = msoAlignLeft
            .Font.NameComplexScript = "+mn-cs"
            .Font.NameFarEast = "+mn-ea"
            .Font.Fill.Visible = msoTrue
            .Font.Fill.ForeColor.RGB = RGB(255, 255, 255)
            .Font.Fill.Transparency = 0
            .Font.Fill.Solid
            .Font.Italic = msoFalse
            .Font.Name = "Sky InfoText Rg"
            .Font.Strike = msoNoStrike
            .Font.Bold = msoFalse
            .Font.Size = 9
        End With
        With Selection.ShapeRange(1).TextFrame2.TextRange.Characters(1, Len(sheettitles(line_counter))).Font
            .Bold = msoTrue
            .Size = 14
        End With
        
        line_counter = line_counter + 1
    Loop
    
    '''''''''''''''''' G08: Tidying stuff up ''''''''''''''''''
    
    ' Hide all the tab names
    ActiveWindow.DisplayWorkbookTabs = False
    
    ' To dodge that thing where it asks you if you want to preserve the clipboard;
    wbk.Worksheets("Summary").Range("A1").Copy
    
    'But then: moving the cursor to a navigable page
    wbk.Worksheets("Summary").Activate
    ActiveSheet.Range("A1").Select

    ' Okay, now save and close that workbook...
    wbk.Close SaveChanges:=True
    
    '''''''''''''''''' G09: Maybe something about the migration lists? ''''''''''''''''''
    
    ' How are we handling those migration lists? Are they often enough to automate?
    
End Sub



'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                 F01:   Executive Dashboard (XDash ID)                    ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public Sub GenerateReport_Xdash_ID()

    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim holding             As Worksheet
    Dim records_count       As Integer
    Dim line_counter        As Integer
    Dim w                   As Worksheet
    Dim p                   As PivotTable
    
    todaysdate = Date
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Executive Dashboard Report ID (TEMPLATE).xlsx", _
             ThisWorkbook.Path & "\Executive Dashboard Report ID " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Executive Dashboard Report ID " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    
    ' F02 -   Refreshing all Pivot tables

    For Each w In wbk.Worksheets
      For Each p In w.PivotTables
        p.RefreshTable
        p.Update
      Next
    Next
    
    ' we're done now, so lets close and save...
    wbk.Close SaveChanges:=True

End Sub


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                 F01:   Executive Dashboard (XDash ED)                 ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public Sub GenerateReport_Xdash_ED()

    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim holding             As Worksheet
    Dim records_count       As Integer
    Dim line_counter        As Integer
    Dim w                   As Worksheet
    Dim p                   As PivotTable
    
    todaysdate = Date
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Executive Dashboard Report ED (TEMPLATE).xlsx", _
             ThisWorkbook.Path & "\Executive Dashboard Report ED " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Executive Dashboard Report ED " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    
    ' F02 -   Refreshing all Pivot tables

    For Each w In wbk.Worksheets
      For Each p In w.PivotTables
        p.RefreshTable
        p.Update
      Next
    Next
    
    ' we're done now, so lets close and save...
    wbk.Close SaveChanges:=True

End Sub



'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''                 H01:   Viewing Consent Report (ViewCons)              ''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Public Sub GenerateReport_ViewConsent()

    Dim todaysdate          As Date
    Dim report_end_date     As Date
    Dim codepath            As String
    Dim settings_sheet      As Worksheet
    Dim wbk                 As Workbook
    Dim holding             As Worksheet
    Dim records_count       As Integer
    Dim line_counter        As Integer
    Dim w                   As Worksheet
    Dim p                   As PivotTable
    
    todaysdate = Date
    Set settings_sheet = ActiveWorkbook.Worksheets("Settings")
    
    ' Use the existing report as a template we build off...
    FileCopy ThisWorkbook.Worksheets("Settings").Range("B13") & "\Vespa Viewing Consent Report (TEMPLATE).xlsx", _
             ThisWorkbook.Path & "\Vespa Viewing Consent Report " & Format(todaysdate, "yyyymmdd") & ".xlsx"
    
    ' Now open the new report:
    Set wbk = Workbooks.Open(ThisWorkbook.Path & "\Vespa Viewing Consent Report " & Format(todaysdate, "yyyymmdd") & ".xlsx")
    
    
    ' H02 -   Refreshing all Pivot tables

    For Each w In wbk.Worksheets
      For Each p In w.PivotTables
        p.RefreshTable
        p.Update
      Next
    Next
    
    ' we're done now, so lets close and save...
    wbk.Close SaveChanges:=True
    
End Sub
