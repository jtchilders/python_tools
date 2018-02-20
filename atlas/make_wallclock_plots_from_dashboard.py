#!/usr/bin/env python
import os,sys,optparse,logging,json,datetime
logger = logging.getLogger(__name__)

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-i','--input',dest='input',help='input file in json format from dashboard plot.')
   parser.add_option('-o','--output',dest='output',help='output file in csv format for data')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'input',
                     'output',
                  ]

   for man in manditory_args:
      if man not in options.__dict__ or options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)
   


   input_data = json.load(open(options.input))

   output_data = {}

   dates = []
   queues = []
   sites = {'OLCF':['ORNL_Titan_MCORE','Titan_long_MCORE','Titan_Harvester_MCORE',
                    'Titan_Harvester_ES','Titan_Harvester_test_MCORE'],
            'ALCF':['ALCF_Theta','ALCF_Theta_ES','ALCF_Cooley'],
            'NERSC':['NERSC_Cori_p2_mcore','NERSC_Cori_p1_mcore','NERSC_Edison_mcore',
                     'NERSC_Cori_p2_ES','NERSC_Cori_p1_ES','NERSC_Edison_ES',
                     'NERSC_Edison_2','NERSC_Edison','NERSC_Cori','NERSC_Cori_2']
            }

   for entry in input_data['jobs']:
      if entry['S_DATE'] in output_data:
         output_data[entry['S_DATE']][entry['S_SITE']] = entry['SUM']
      else:
         output_data[entry['S_DATE']] = {entry['S_SITE']:entry['SUM']}
      
      dates.append(entry['S_DATE'])
      queues.append(entry['S_SITE'])

   # remove duplicate dates
   dates = sorted(list(set(dates)))
   queues = sorted(list(set(queues)))

   # convert date strings to datetime objects
   newdates = []
   for date in dates:
      newdates.append(datetime.datetime.strptime(date,'%d-%b-%y %H:%M:%S'))
   dates = sorted(newdates)

   outfile = open(options.output.replace('.csv','_byqueue.csv'),'w')
   header = 'date,%s\n' % ','.join(x for x in queues) 
   outfile.write(header)
   for date in dates:
      entry = output_data[date.strftime('%d-%b-%y %H:%M:%S')]
      for queue in queues:
         if queue not in entry:
            entry[queue] = 0

      # convert date 01-Dec-17 00:00:00
      tmpdate = date.strftime('%m-%d-%Y')
      line = tmpdate + ',' + ','.join(str(entry[x]) for x in queues) + '\n'
      outfile.write(line)


   outfile = open(options.output.replace('.csv','_bysite.csv'),'w')
   header = 'date,%s\n' % ','.join(x for x in sites.keys()) 
   outfile.write(header)
   for date in dates:
      
      entry = output_data[date.strftime('%d-%b-%y %H:%M:%S')]
      
      new_sums = {}
      for site in sites.keys():
         new_sums[site] = 0
      
      for sitequeue,sum in entry.iteritems():
         
         found = False
         for site,sitequeues in sites.iteritems():
            if sitequeue in sitequeues:
               new_sums[site] += sum
               found = True
               break
         if not found:
            logger.error('sitequeue not recognized: %s',sitequeue)
      # convert date 01-Dec-17 00:00:00
      tmpdate = date.strftime('%m-%d-%Y')
      line = tmpdate + ',' + ','.join(str(new_sums[x]) for x in sites) + '\n'
      outfile.write(line)







if __name__ == "__main__":
   main()
