#!/usr/bin/env python
import os,sys,optparse,logging,glob,subprocess,datetime,json
from dateutil import parser as dateutilparser
logger = logging.getLogger(__name__)

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-g','--glob',dest='glob',help='glob to grab pilot log files to parse')
   parser.add_option('-o','--output',dest='output',help='output file name',default='pilotdata.json')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'glob',
                  ]

   for man in manditory_args:
      if options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)
   
   logfiles = glob.glob(options.glob)
   
   data = {}
   for logfile in logfiles:
      print logfile
      rank_num = int(logfile.split('.')[1])
      datetimes = []
      starttime = None
      athena_starttime = None
      athena_endtime = datetime.datetime.now()
      endtime = datetime.datetime.now()
      jobid = 0
      ncores = 0

      maxevents = 0

      for line in open(logfile):
         
         if line.startswith('2017') and starttime is None and 'PanDA Pilot, version PICARD' in line:
            starttime = parse_datetime(line)
         
         # find athena command
         # Experiment.p| Executing command
         if 'Experiment.p| Subprocess is running' in line and athena_starttime is None:
            athena_starttime = parse_datetime(line)

         # get panda id
         if 'jobid=' in line and jobid == 0:
            for part in line.split():
               if 'jobid=' in part:
                  jobid = int(part.split('=')[1][:-3])
         
         if 'This job ended with (trf,pilot) exit code of' in line:
            athena_endtime = parse_datetime(line)
         if '!!FAILED!!3000!! Job failed: Non-zero failed job return code' in line:
            athena_endtime = parse_datetime(line)
         
         # 2017-03-14 23:23:20| 18301|pUtil.py    | Done, using system exit to quit
         if 'Done, using system exit to quit' in line:
            endtime = parse_datetime(line)

         # get maxEvents
         index = line.find('--maxEvents=') 
         if index > 0 and maxevents == 0:
            endindex = line.find(' ',index)
            #print line,index,endindex
            maxevents = int(line[index + len('--maxEvents='):endindex])

         index = line.find('nCores=')
         if index > 0 and ncores == 0:
            endindex = line.find(')')
            ncores = int(line[index+len('nCores='):endindex])

      
      print starttime, athena_starttime,athena_endtime,endtime,jobid
      data[jobid] = {'start':str(starttime),'athena_start':str(athena_starttime),'athena_end':str(athena_endtime),'end':str(endtime),'rank':rank_num,'maxevents':maxevents,'ncores':ncores}

   json.dump(data,open(options.output,'w'))


def parse_datetime(line):
   date_string = line.split('|')[0].split('.')[0]

   #date = datetime.datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=7)
   try:
      date = dateutilparser.parse(date_string) - datetime.timedelta(hours=7)
   except:
      logger.error(' failed to parse "%s" from %s',date_string,line)
      raise
   return date
            


if __name__ == "__main__":
   main()
