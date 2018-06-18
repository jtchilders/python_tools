#!/usr/bin/env python
import os,sys,optparse,logging,glob,subprocess,CalcMean
logger = logging.getLogger(__name__)

search_string = 'evtloop_time'

def main():
   ''' loop over athena log file to extract the time per event. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-g','--glob',dest='glob',help='A string in double quotes to capture all the log files to process. Can use wildcards, for example "*/log.generate"')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'glob',
                  ]

   for man in manditory_args:
      if options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)

   filelist = glob.glob(options.glob)

   logger.info('processing %s files',len(filelist))

   mean_wall_per_event = CalcMean.CalcMean()

   failed = 0

   for filename in filelist:
      
      cmd = 'grep %s %s' % (search_string,filename)

      p = subprocess.Popen(cmd.split(),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      stdout,stderr = p.communicate()

      if p.returncode != 0:
         logger.error('error in grep of filename %s \n %s \n %s',filename,stdout,stderr)
         failed += 1
         continue
      
      # parse line like this:
      # 12:17:26 PMonSD [---] 1000    64696    64909        -        - evtloop_time
      parts = stdout.split()
      
      nevts = int(parts[3])
      cpu_seconds_in_eventloop = int(parts[4])
      wall_seconds_in_eventloop = int(parts[5])

      wall_per_event = float(wall_seconds_in_eventloop) / float(nevts)

      mean_wall_per_event.add_value(wall_per_event)

   logger.info('mean wall time per event is %s',mean_wall_per_event.get_string())
   if failed > 0:
      logger.info(' %s of %s failed',failed,len(filelist))

      
   


if __name__ == "__main__":
   main()
