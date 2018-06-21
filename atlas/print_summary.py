#!/usr/bin/env python
import os,sys,optparse,logging,glob,json
logger = logging.getLogger(__name__)

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-g','--globin',dest='globin',help='glob string to capture folders to loop over and print')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'globin',
                  ]

   for man in manditory_args:
      if man not in options.__dict__ or options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)


   folder_list = sorted(glob.glob(options.globin))
   logger.info('found %s folders to print',len(folder_list))

   for foldername in folder_list:
      jobid = os.path.basename(foldername)

      line = ''

      settings = json.load(open(foldername + '/settings.txt'))
      stats = json.load(open(foldername + '/jobstats.json'))

      line += '%s\t' % jobid
      line += '%5d\t' % settings['num_ranks']
      line += '%5d\t' % int(settings['use_container'])
      line += '%10d\t' % stats['summary']['runtime']
      line += '%10d\t' % stats['summary']['evts_processed']
      line += '%10.2f\t' % stats['summary']['evt_proc_time_mean']
      line += '%10.2f\t' % stats['summary']['evt_proc_time_sigma']
      line += '%10.2f\t' % stats['summary']['evt_proc_by_worker_mean']
      line += '%10.2f\t' % stats['summary']['evt_proc_by_worker_sigma']
      line += '%10.2f\t' % stats['summary']['occupancy_mean']
      line += '%10.2f' % stats['summary']['occupancy_sigma']

      print(line)


   


if __name__ == "__main__":
   main()
