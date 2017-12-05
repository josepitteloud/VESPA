=======================================================
V306 - Capping calibration automation
\Vespa\Vespa Projects\306 - Capping Calibration Automation\InProgress
=======================================================

This folder contains a work-in-progress version of the Capping calibration algorithm.

Script are named from 01 to 06 and must be executed in order:
01 creates tables
02 is just a check query and can be skipped at this stage
03 creates the view: it is important to change the date inside the script: variable @varStartDate
04 creates a table named CP2_BOX_LOOKUP (with information on primary/secondary boxes), used in script 05
05 contains the main algorithm for capping
06 calculates BARB minute-by-minute
07 calculates VESPA minute-by-minute
08 makes cleaning of transient tables

Currently, the same date to be run for capping, must be specified in the following files: 
03 - variable @varStartDate (for example set @varStartDate = '2015-01-30';)
04 - variable @profiling_thursday (for example set  @profiling_thursday='2015-01-30';)
05 - variable @target_date (for example set @target_date='2015-01-30';)


==========
14/04/2015
==========
Version taking parameters from the metadata table
