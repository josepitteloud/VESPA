
hadoop fs -ls <folder> 
# Accessing an specific folder within Hadoop
# I.E.: hadoop fs -ls / 
# with this we access the hadoop root directory and lists the files within, is like 'ls' or 'll' in unix

hadoop fs -cat <file> 
# reads the whole file (i.e. can be very very big if you do that with a bit file)
# I.E.: hadoop fs -cat /datasets/ethan/dimensions/trial/trial.txt

git commit -am "and type here the message"
#This is how you commit in git from putty

git push
#This is how you push stuff into git server from putty

git pull
#This is how you pull stuff from git server into the local branch

. ./<file>.sh
#This is how you execute a shell script