The Logging System
 
What is a Logger?
A logger allows the automated recording of events during code execution.
 
There are two main reasons to do this: 
•	You want to record the progress of a scripts execution during development or in case it terminates unexpectedly.
•	You want to perform QA tests throughout your code and view the results
Using the logger is made simple through the use of stored procedures which means the snippets of logger code can be quickly added to any piece of code.
How is the Logger Set Up?
The Logger has :
•	Runs - These are script executions.  Every time a script is executed a new run record is set up
•	Events - these are the bits of information you would like to record
•	Levels - Each event has a level which determines the events importance 
Event Levels
The level of an event will determine its importance to the viewer.  
1.	FATAL - The most serious.  Usually indicates the script is terminating as there is a problem it cannot recover from. 
2.	WARNING - Here something is wrong and warrants attention
3.	INFORMATION - Usually a stage within the script you would ordinarily want to see reported on or it could be the result of a QA test showing everything is OK
4.	DEBUG - This is information that is trivial and usually only looked at when you are developing a script or you are tryng to fix it!
Usually you will want to only see events less than or equal to a certain level.  eg if you are checking your log for QA issues then you would look at all events for a run <= level 2 as info and debug information will not be of interest to you.  
 
The Run 
The Run Records the meta data for the logging run: 
•	The Unique Run ID which groups all the events together
•	The Job Number - the reference for the job being run - usually the SK number 
•	Run Description - Usually the Job Name 
Events
Events will have the following: 
•	A Run ID (see above) 
•	A timestamp to show when it happened
•	A Level (Fatal / Warning / Info / Debug) 
•	A description - saying what happened 
•	An optional integer value to record the result of a QA test
Adding Logger Commands into your Code
The Logger stored procedures are created locally but have permission granted to public.
 
First Set Up a Run and get the RunID 
Change the Job Number and Job Description and these to the top of your Script :  
 
CREATE VARIABLE @RunID bigint; 
-- the Run_ID to be used for events

EXECUTE logger_create_run 'SK9999','Test Run',@RunID output; 
-- This sets up the run AND gets the RunID
 
The logger_create_run proceedure has to been written with multiple users in mind so if two run setup requests are made at the same time each will get the correct run_id.  
 
Next Add your Events 
Adding an event is just a simple command logger_add_event where you insert the following
•	The Run ID (which you got earlier) 
•	The event level (See above) 
•	The Event Name (A string so you need quotes)
•	The Value - An OPTIONAL value 
EXECUTE logger_add_event @RunID , 3,'First Event';        
-- A Normal Info Event 

EXECUTE logger_add_event @RunID , 3,'QA Result',100;      
-- An Info event with an optional Value  

EXECUTE logger_add_event @RunID , 2,'Something is Wrong'; 
-- A Warning event with a level 2 
 
It really is a simple as that!
 
 
Listing Logger Runs and Events
There is nothing to stop you writing your own queries to list run and event information from the logger tables (see below).  However a few stored procedures have been written to make this easier.  Note that the DB servers are internally set to BST (which is GMT+1), so at certain times of the year events might be logged an hour ahead of local time. 

Listing events for a RunID - logger_list_events_for_run
If you know the run ID then you can list the events for that run.  Use these parameters 
•	The Run ID 
•	The maximum event level (Defaults to - 2 Warning) 
EXECUTE logger_list_events_for_run 1234;     
-- Will list just the Warning and Fatal Events 

EXECUTE logger_list_events_for_run 1234, 4;  
-- Will list ALL the events including the debug ones 
 
 
Listing events for the last run of a Job - logger_get_latest_job_events
Most of the time you will not know the run_id.  To save you looking it up you can use the logger_get_latest_job_events procedure to look up the last run for a specific job and list those events.  Use these parameters 
•	The Job Number
•	The maximum event level (Defaults to - 2 Warning) 
EXECUTE logger_get_latest_job_events 'SK1234';    
-- Will list just the Warning and Fatal Events
 
EXECUTE logger_get_latest_job_events 'SK1234',4;  
-- Will list ALL the events including the debug ones 
 
 
Listing the Latest Runs for a job - logger_list_latest_runs  
If you want to see the last 50 runs for a particular job then use this procedure with the Parameter:
•	The Job Number
EXECUTE logger_list_latest_runs 'SK1234' 
 
Logger Tables 
The Logger tables are set up with all permissions granted to public.
 
The Run Table - z_logger_runs
Field	Type	Description
run_id	bigint	The Unique RunID (The Identity field) 
run_time	datetime	When the run was set up 
job_number	varchar(20)	The Job Number reference 
run_description	varchar(50)	The Description of the Run 
 
 
The Event Table - z_logger_events
Field	Type	Description
event_id	bigint	The Unique ID of the Event(The Identity field) 
run_id	bigint	Foriegn Key to the Run Table 
event_time	datetime	Time of the Event 
event_level	tinyint	The Event Level (see above)
event_description	varchar(200)	The Description of the event
value	integer	the optional number for use in QA checks 
 
The Script for the Logger is in the repository in the Trunk area.

