"""Preprocess raw promo files for Sybase import

Similar to the spot load import, we've now also got Promo stuff. It's
still fixed length. It still comes in a huge number of different source
files. So we go through much of the same process and parse and concatenate
them all. Build a header file, define some hashes, stuff like that.

We're still pretty loose about exception handling; even looser than the
spot load process. We're also probably doing less QA and stability stuff
on the SQL side, but that's OK, we're more hurried. This script still
bails horribly if anyone has the "New" folder open in an explorer window
while you try to run it, because it clears the "New" folder by deleting
and then remaking it :-/ Generally the script isn't so stable, but it
does what it needs to usually without complaining. And you can just go
into p5x1 and execute the thing, and away it goes.

Things to do: (a lot of these might also apply to the spot logs)
5/ Some decent exception catching would help, since we're not always
        properly closing the files that we should... are we? The failures
        are coming from system excepts coming up through our calls, so,
        yeah.
6/ Same request as spot logs: slightly more graceful handling of empty
        raw file directory. Yeah, not sure of an awesome way to handle
        this. What about the case where only a subdirectory exists, but
        contains no relevant files? do we really want to os.walk again
        before we actually parse? Arguably yes, since we don't even want
        to create the dump & header files if there's nothing to parse in.
        But ideally we'd still archive off the old ones... at which point
        the last run time doesn't matter? Maybe that changes the whole
        iteration structure. First walk, build a list of files, then if
        that list is not empty, step through it. Also: the presence of
        the prior timestamp file indicates whether or not we archive
        the things. And it's written raw each time, so yeah, we can just
        kill it if we're not creating the dumpfiles. Hmmm. Bit of an
        overhaul though. This also superceeded 4, since if we check if
        stuff is empty, we'd never have those files of size zero sitting
        around anyway.
7/ Delete only the contents of the "New" folder. This will be a lot more
        robust than nuking the entire folder and trying to put it back.

"""

# The folder which contains all the stuff (maybe in subfolders too)
SourceFolder = r'\\tsclient\G\RTCI\Sky Projects\Vespa\Promo data\New'
ProcessedFolder = r'\\tsclient\G\RTCI\Sky Projects\Vespa\Promo data\Processed'
ParsedFolder = r'\\tsclient\G\RTCI\Sky Projects\Vespa\Promo data\Parsed'
StoreFolder  = r'D:\Vespa promo loading'
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

# The function that parses each line of the spot files
def do_Promo_line_parse (Promo_line, line_number, filehash) :
    """Decompose a single line of raw promo data into data fields"""
    result = [filehash,
              line_number,
              Promo_line[0:8].strip(),
              Promo_line[9:13].strip(),
              Promo_line[14:19].strip(),
              Promo_line[20:26].strip().lstrip('0'), # Spec indicates numerical field
              Promo_line[27:30].strip().lstrip('0'), # Spec indicates numerical field
              Promo_line[31:40].strip(),
              Promo_line[41:49].strip(),
              Promo_line[50:].strip()
              ]
    return result
    # Having the line number passed in is ugly, but kind of works :/
    # Having all the field extraction explicit is kind of ugly too...
    # We're using the hash here to link? Yeah, that's because Python
    # doesn't know what the autonumbered load table is up to in the
    # DB when it starts to coalesce the raw files together.

# The function which parses each spot file 
def convert_Promo_2_cvs (writersettings, dirname, filename, hashlist=[]):
    """Covert a single Promo file specified by FULLPATH into a .csv file for Sybase

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

    # For counting how many data lines we process
    t = 1
    # Yeah, because we're adding the PK ourself, we kind of need to loop manually :/

    loadhandle = open(fullpath, 'r')

    for nextline in loadhandle :
        if len(nextline.strip()) == 0 :
            # Must be one of the crappy trailing lines, do nothing
            pass
        elif len((nextline[8] + nextline[13] + nextline[19] + nextline[26] + nextline[30] + nextline[40] + nextline[49]).strip()) == 0 :
            # An actual data line! Parse that.
            lumps = do_Promo_line_parse(nextline, t, filehash)
            # Write as comma delimited to it's own file
            writersettings[0].writerow(lumps);
        else :
            # If there aren't blanks in the places we expected, it's not a type
            # of line we expect and that's getting called an error.
            writersettings[2].write('Bad Promo data format in line {0}!\n'.format(t))

        t += 1
        
    # Close the specific files
    loadhandle.close()

    # Prepare the load header line
    writersettings[1].writerow([filehash, t, fullpath])
    
    # We were going to also log the number of lines successfully written to .csv,
    # but that would imply we were responsibly handling the .csv module calls, or
    # indeed any of the interface calls, when in fact, there's not a single TRY in
    # this whole file yet.

# Um... there's a more concise way to do this with MAP and FILTER?
def process_Promo_directory(writersettings, dirname, filenames) :
    for eachfile in filenames :
        if eachfile[-4:] == '.txt' :
            convert_Promo_2_cvs (writersettings, dirname, eachfile)

#------------ Preparing other variables ------------#

# So that midnight won't kick our ass in nightly runs:
kickoffflag = datetime.now().strftime('%Y%m%d_%H%M')

# Other setup of stuff:
LogFile      = StoreFolder + '\\Promo_parse_logs.txt'
HeaderFile   = StoreFolder + '\\Promo_parse_headers.csv'
ParsedFile   = StoreFolder + '\\Promo_parse_dump.csv'
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
        os.rename(HeaderFile, StoreFolder + '\\Archived\\Promo_parse_headers_{0}.csv'.format(priorkickoffflag))
        os.rename(ParsedFile, StoreFolder + '\\Archived\\Promo_parse_dump_{0}.csv'.format(priorkickoffflag))
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
logwriter.write('{0}: Now doing Promo file scanning.\n'.format(str(datetime.now())))

# Do the walking
os.path.walk(WorkFolder, process_Promo_directory, writersettings)

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
logwriter.write('{0}: Completed Promo file scanning.\n'.format(str(datetime.now())))

# Move the parsed files to the network share (even though
# the automated load goes from the local copies on p5x1)
shutil.copy(HeaderFile, ParsedFolder + '\\Promo_parse_headers_{0}.csv'.format(kickoffflag))
shutil.copy(ParsedFile, ParsedFolder + '\\Promo_parse_dump_{0}.csv'.format(kickoffflag))

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
raw_input('Processing complete! Press <Enter> to exit, then go run the Promo loading SQL script.')
