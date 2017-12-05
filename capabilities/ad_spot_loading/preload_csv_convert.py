"""Preprocess DCL spot files for Sybase import

We receive DCL formatted spot files, which are a fairly ugly fixed length
kludge. We build a header file with the MD5 has of the thing we loaded, we
test the stated control totals, and also add a primary key within each
file that will eventually serve as part of a composite promary key.

Refer to the wiki for other run instructions:
        http://rtci/vespa1/Advertising%20data.aspx

We're generally pretty loose about exception handling. We don't have unit
test scripts or negative path testing, but we are tracking progress and
collecting control totals at each stage. We could pay more attention to
the flags at the start of each spot line, but hey, it'll just bail if the
source file is out of spec, and operators are still expected to be devs
at this point. Oh, also: this script bails horribly if anyone has the
"New" folder open in an explorer window while you try to run it, because
it clears the "New" folder by deleting and then remaking it :-/ Generally
the script isn't so stable, but it does what it needs to usually without
complaining. And you can just go into p5x1 and execute the thing, and
away it goes.

Things to do:
5/ Some decent exception catching would help, since we're not always
        properly closing the files that we should... are we? The failures
        are coming from system excepts coming up through our calls, so,
        yeah.
6/ Add a check to just automatically stop in the case where the raw folder
        is empty. Otherwise we're just generating files of size zero and
        incrementing the last run date, and that's kind of annoying. Refer
        to the promo version as well, since it might get handled over there
        instead, that's seeing more active Dev at the moment.
7/ Delete only the contents of the "New" folder. This will be a lot more
        robust than nuking the entire folder and trying to put it back.
8/ Maybe cut the load up into a number of files with a maxiumum size? Yes,
        it'll make it a pain to import into Sybase, but it's better than
        Sybase just choking on one big file :-(
9/ Turns out that the extraction filename is inserted into the header line
        of the raw files, so that identical logs extracted on different
        days are getting different hashes even though all the data is the
        same. Instead, we should hash the resulting .csv file which will
        only contain the resulting data. Except, lols, we never write that
        file to disk, it always goes into the combined .csv :/
10/ And if the hash of the .csv is recognised as a duplicate, shal we set
        that aside in a different loadfile? or just drop it from the
        processing? Maybe we do calculate the has at the beginnig, but
        figure out how to make the hashing thing skip the first line of
        the raw file...
11/ Additional feature requests marked inline as always by ##

"""

# The folder which contains all the stuff (maybe in subfolders too)
SourceFolder = r'\\tsclient\G\RTCI\Sky Projects\Vespa\Spot logs\New'
ProcessedFolder = r'\\tsclient\G\RTCI\Sky Projects\Vespa\Spot logs\Processed'
ParsedFolder = r'\\tsclient\G\RTCI\Sky Projects\Vespa\Spot logs\Parsed'
StoreFolder  = r'D:\Vespa spot loading'
# p5x1 has to go back through the client to get to the networked
# drives, which is why you have to share them through the RDP
# session. New logs should be in SourceFolder (in any arbitrary
# subfolder arrangement). After the batch, they'll automatically
# be moved to ProcessedFolder, and the parsed files will appear in
# ParsedFolder. Local copies will also exist in StoreFolder because
# loading into Sybase over the shared network drive is super slow.
# Hopefully you don't really have to change these.

#------------ Other modules we need: ------------------#

# To iterate over the various files
import os
# To copy files around the between network and working areas
import shutil
# Hashing files to check if they're changed
import hashlib
#To write the resulting files
import csv
#For logging:
from datetime import datetime

#------------ The El Cheapo logger: ------------------#

class MultiLogger:
    """ Something to write the same line to the screen and the log file in one call."""
    
    def __init__(self, logwriter) :
        self.logthing = logwriter
    
    def write(self, lineoftext) :
        self.logthing.write(lineoftext)
        # I guess we could even bundle this into some kind of flag or check so
        # that it outputs to terminal if you want it to or otherwise just to
        # log file or whatever...
        print(lineoftext)
        
    def close(self) :
        self.logthing.close()

#------------ Functions which do things: ------------------#

# To hash the files and check that any repeats have / have not changed
# Doesn't need to be cryptographically secure, don't care about things
# like preimage attack weaknesses, we'er just checking if a file has
# changed.
def get_file_hash (fullpath) :
    """Return the MD5 hash of the specified file"""    

    # This bit was sourced from Stack Overflow via Google, specifically:
    #   http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python

    md5 = hashlib.md5()
    with open(fullpath,'rb') as f: 
        for chunk in iter(lambda: f.read(512*md5.block_size), ''): 
             md5.update(chunk)
    # Hexdigest is the safe varchar(32) style output
    return md5.hexdigest()

def do_DCL_line_parse (DCL_line, line_number, filehash) :
    """Parse a single line of DCL data into elements and write it to CSV"""
    result = [filehash,
              line_number,
              DCL_line[0:2].strip(),
              DCL_line[2:10].strip(),
              DCL_line[10:15].strip(),
              DCL_line[15:17].strip(),
              DCL_line[17:19].strip(),
              DCL_line[19:25].strip(),
              DCL_line[25:30].strip().lstrip('0'), # break_total_duration
              DCL_line[30:32].strip(),
              DCL_line[32:44].strip().lstrip('0'), # broadcasters_break_id
              DCL_line[44:46].strip(),
              DCL_line[46:58].strip().lstrip('0'), # broadcasters_spot_number
              DCL_line[58:63].strip(),
              DCL_line[63:65].strip(),
              DCL_line[65:67].strip(),
              DCL_line[67:69].strip(),
              DCL_line[69:75].strip(),
              DCL_line[75:80].strip().lstrip('0'), # spot_duration,
              DCL_line[80:95].strip(),
              DCL_line[95:130].strip(),
              DCL_line[130:170].strip(),
              DCL_line[170:210].strip(),
              DCL_line[210:215].strip().lstrip('0'), # sales_house_identifier
              DCL_line[215:225].strip().lstrip('0'), # campaign_approval_id
              DCL_line[225:230].strip().lstrip('0'), # campaign_approval_id_version_number
              DCL_line[230:232].strip(),
              DCL_line[232:256].strip()
              ]
    return result
    # Having the line number passed in is ugly, but kind of works :/
    # Having all the field extraction explicit is kind of ugly too...

def convert_DCL_2_cvs (writersettings, dirname, filename, hashlist=[]):
    """Covert a single DCL file specified by FULLPATH into a .csv file for Sybase

The logsandheaders should be a structure containing first an
open reference to the header file that control totals are being
written into, and secondly an open reference to the log file.

"""
    # Build the full path for the file and get the MD5 hash
    fullpath = '{0}\\{1}'.format(dirname, filename)
    filehash = get_file_hash (fullpath)
    
    # Check if the file has is one we've already seen (ie, duplicate raw file)
    if filehash in hashlist :
        writersettings[2].write('{1} : Skipping due to MD5 collision: "{0}"\n'.format(fullpath, filehash))
        return
    else :
        hashlist.append(filehash)
        # Turns out this usage, with the hashlist in the parameter default,
        # means that the same hashlist persists between function calls. Cool!

    # Assuming it's a new file: logging details (including the file hash)
    writersettings[2].write('{1} : From "{0}"\n'.format(fullpath, filehash))
    # We're also printing so that you see progress if you're
    # running it in a terminal window or something.

    # For counting how many data lines we process
    t = 0
    datalines = 0
    # Yeah, because we're adding the PK ourself, we kind of need to loop manually :/
    controllines = ''
    # Yes the control total here is a number, but it gets pulled out of the string
    # as a string and we're not checking it mathes against t until the database import

    loadhandle = open(fullpath, 'r')

    for nextline in loadhandle :
        if t == 0 :
            # The first line doesn't contain data, just some header stuff
            pass
        
        elif len(nextline.strip()) == 0 :
            # Must be one of the crappy trailing lines, do nothing
            pass
        elif len(nextline.strip()) < 250 :
            # Then it must be the final line; get the control total and log it
            controllines = nextline[2:9]
        else :
            # An actual data line! Parse that.
            datalines += 1
            lumps = do_DCL_line_parse(nextline, datalines, filehash)
            # Write as comma delimited to it's own file
            writersettings[0].writerow(lumps);
            
        t += 1
        ## Also, we could add a bunch of state checking for stability; lines in the
        # file should always go in the order of header -> data*N -> trailer -> blanks
        # but we're not actually checking that it does yet. What if it does? Just go
        # and fail I guess, as it doesn't meet the spec.
        ## Further, the first two characters of the line tell you which type of line
        # it is, so we can also check that it's what we expect (instead of basing it
        # entirely on the data string length)
        
    # Close the specific files
    loadhandle.close()

    # Prepare the load header line
    writersettings[1].writerow([filehash, datalines, controllines, fullpath])
    
    # We were going to also log the number of lines successfully written to .csv,
    # but that would imply we were responsibly handling the .csv module calls, or
    # indeed any of the interface calls, when in fact, there's not a single TRY in
    # this whole file yet.

# Um... there's a more concise way to do this with MAP and FILTER?
def process_DCL_directory(writersettings, dirname, filenames) :
    for eachfile in filenames :
        if eachfile[-4:] == '.DCL' :
            convert_DCL_2_cvs (writersettings, dirname, eachfile)

#------------ Preparing other variables ------------#

# So that midnight won't kick our ass in nightly runs:
kickoffflag = datetime.now().strftime('%Y%m%d_%H%M')

# Other setup of stuff:
LogFile      = StoreFolder + '\\DCL_parse_logs.txt'
HeaderFile   = StoreFolder + '\\DCL_parse_headers.csv'
ParsedFile   = StoreFolder + '\\DCL_parse_dump.csv'
# Okay, so these filenames are all the same because Sybase can't
# handle loading from a dynamic filename using INPUT INTO, and
# also because the other paths (bcp, isql) didn't work. So the
# loading filenames are static, and we now have to record the
# last time the script was run, then we rename them on the next
# iteration of the script. And we also have to rename them when
# we copy them up to the external drive too. But it's now a lot
# more automated, you don't have to change any of the references
# in the SQL loading script, so that's good.

WorkFolder   = StoreFolder + '\\working'

# If the header or data files are present, they must be from a
# prior run so we'll rename them to whatever the prior run date
# was. We just hope that there's only one run per day.

try :
    with open(StoreFolder + '\\lastrun.txt', 'r') as p :
        priorkickoffflag = p.read()
except :
    priorkickoffflag = None

# Archive the last bunch of stuff, if it's still sitting around
if priorkickoffflag is not None :
        # This will fail if these files already exist, but the flags
        # go down to the minute, so it should be fine unless you run
        # this script many times within a minute of each other.
        os.rename(HeaderFile, StoreFolder + '\\Archived\\DCL_parse_headers_{0}.csv'.format(priorkickoffflag))
        os.rename(ParsedFile, StoreFolder + '\\Archived\\DCL_parse_dump_{0}.csv'.format(priorkickoffflag))
        # We're just splicing in the datestamp before the .csv suffix
        
        # Also kill the lastrun.txt so if we bail midway through, we
        # don't have this fake broken reference sitting around
        os.remove(StoreFolder + '\\lastrun.txt')
        # Also, if a load breaks midway through, we'll no longer attempt
        # to archive off the broken files on the next run because the
        # timestamping file won't be there.

# We're not resetting the logs though, we just append to them

#------------ File handling and writing objects ------------#

# Open the header & log files
logwriter = MultiLogger(open(LogFile, 'a'))

# The header file with control totals for each raw file
headerhandle = open(HeaderFile, 'wb')
# In binary mode because the parsing is handled by the .csv engine:
headerwriter = csv.writer(headerhandle,
                          delimiter = ',',
                          quotechar = '"',
                          quoting   = csv.QUOTE_ALL)

# The file that will receive all the parsed components
parsedhandle = open(ParsedFile, 'wb')
parsedwriter = csv.writer(parsedhandle,
                          delimiter = ',',
                          quotechar = '"',
                          quoting   = csv.QUOTE_ALL)

# Wrap all references to each in a structure we can pass
writersettings = [parsedwriter, headerwriter, logwriter]

#------------ Main actions ------------#

# Logging start of main loop process
logwriter.write('\nNew log parsing batch!\n{0}: Starting with file copying.\n'.format(str(datetime.now())))

# Copy all the current data to the local working area
if os.path.exists(WorkFolder) :
    shutil.rmtree(WorkFolder)
shutil.copytree(SourceFolder, WorkFolder)

# Logging start of main loop process
logwriter.write('{0}: Now doing DCL file scanning.\n'.format(str(datetime.now())))

# Do the walking
os.path.walk(WorkFolder, process_DCL_directory, writersettings)

# Close parsing & header files
headerhandle.close()
parsedhandle.close()

# Save the timestamp to date so it can eventually be appended
# to the filenames when the script is rerun
with open(StoreFolder + '\\lastrun.txt', 'w') as p :
        p.write(kickoffflag)
# For the moment the filenames have to be static because that's
# the way tht Sybase likes to load it. But when we revisit the
# load, we have to archive off the most recently run files, so
# we need to keep that most recent timestamp around.

# Log end of looping process
logwriter.write('{0}: Completed DCL file scanning.\n'.format(str(datetime.now())))

# Move the parsed files to the network share (even though
# the automated load goes from the local copies on p5x1)
shutil.copy(HeaderFile, ParsedFolder + '\\DCL_parse_headers_{0}.csv'.format(kickoffflag))
shutil.copy(ParsedFile, ParsedFolder + '\\DCL_parse_dump_{0}.csv'.format(kickoffflag))

# Clear out the working space
shutil.rmtree(WorkFolder)

# Move files about on network store
shutil.copytree(SourceFolder, ProcessedFolder + '\\' + kickoffflag)
shutil.rmtree(SourceFolder)
os.mkdir(SourceFolder)
## Maybe sometime we'll get around to just removing all the contained
# files rather than killing and ressurecting the entire directory, but
# whatever. Also, being a copy rather than a move, it's super slow.
# When there are lots of logs and everything is going over a network,
# it turns into a major bottleneck :/ ##FeatureRequest!##

# Log completion of parsing!
logwriter.write('{0}: Completed file shuffling. All done!\n'.format(str(datetime.now())))

# Close log file
logwriter.close()

# And we're done!
raw_input('Processing complete! Press <Enter> to exit, then go run the Spot loading SQL script.')
