#!/usr/bin/env python
import os,sys,optparse,logging,glob,subprocess,urlparse
logger = logging.getLogger(__name__)

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-g','--glob',dest='glob',help='input glob to grab log files in which to search for log tarballs, which will then be untarred to the local directory')
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

   for logfile in logfiles:
      cmd = 'grep "Executing command:" ' + logfile + ' | grep log.tgz'
      p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
      stdout,stderr = p.communicate()

      parts = stdout.split('\n')[0].split()
      
      try:
         url = parts[-2].replace(',','')
      except IndexError:
         logger.error('failed to parse: ' + logfile)
         
         # try to see if logs are still around:
         filename_number = logfile.split('.')[1]

         
         job_dir = glob.glob(filename_number + '/Panda_Pilot*/PandaJob*')[0]
         
         os.system('ln -s ' + job_dir + ' tarball_' + filename_number)
         
      else:
         up = urlparse.urlparse(url)

         print logfile + ': ' + url
         #print stdout

         cmd = 'tar xf ' + up.path

         p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
         stdout,stderr = p.communicate()





      




if __name__ == "__main__":
   main()
