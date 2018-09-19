#!/usr/bin/env python
import os,sys,optparse,logging,json,datetime
logger = logging.getLogger(__name__)


def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-e','--events',dest='events',help='file containing processed events')
   parser.add_option('-w','--walltime',dest='walltime',help='file containing the wall time')
   parser.add_option('-j','--jobs',dest='jobs',help='file containing the processed jobs')
   parser.add_option('-o','--output',dest='output',default='output.txt',help='output file in tab column format for data')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'events',
                     'walltime',
                     'jobs',
                     'output',
                  ]

   for man in manditory_args:
      if man not in options.__dict__ or options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)
   
   site_data   = {}  # [date][site][descr]
   queue_data  = {}  # [queue][date][descr]

   dates = []
   sites = {'OLCF':['ORNL_Titan_MCORE','Titan_long_MCORE','Titan_Harvester_MCORE',
                    'Titan_Harvester_ES','Titan_Harvester_test_MCORE','Titan_Harvester_MCORE_ES'],
            'ALCF':['ALCF_Theta','ALCF_Theta_ES','ALCF_Cooley'],
            'NERSC':['NERSC_Cori_p2_mcore','NERSC_Cori_p1_mcore','NERSC_Edison_mcore',
                     'NERSC_Cori_p2_ES','NERSC_Cori_p1_ES','NERSC_Edison_ES',
                     'NERSC_Edison_2','NERSC_Edison','NERSC_Cori','NERSC_Cori_2']
            }

   parse_input_by_queue(options.events,'events',queue_data,dates,1.e6)
   parse_input_by_queue(options.walltime,'walltime',queue_data,dates,1. / 60. / 60.)
   parse_input_by_queue(options.jobs,'jobs',queue_data,dates)

   # need to process 'ORNL_Titan_MCORE' to convert job # to events process
   # since their numbers are supposedly different
   # assuming 50 events per job
   for date in queue_data['ORNL_Titan_MCORE'].keys():
      data = queue_data['ORNL_Titan_MCORE'][date]
      # old = data['events']
      new = 50. * data['jobs']
      # logger.info('%s diff = %10d (%6.2f%%)   new = %10d  old = %10d' % (date,new - old,(new - old)/old*100.,new,old))
      data['events'] = new
      # logger.info(' %s %s',data['events'],queue_data['ORNL_Titan_MCORE'][date]['events'])

   json.dump(queue_data,open('queue_data.json','w'),indent=3,sort_keys=True)

   site_data = sum_by_site(queue_data,sites)

   # convert date strings to datetime objects
   newdates = []
   for date in dates:
      newdates.append(datetime.datetime.strptime(date,'%m-%d-%Y'))
   dates = sorted(newdates)

   outfile = open(options.output,'w')
   outfile.write('\tALCF\t\t\tNERSC\t\t\tOLCF\n')
   outfile.write('date\tevents\twallclock\tjobs\tevents\twallclock\tjobs\tevents\twallclock\tjobs\n')
   
   logger.info('-> %s',site_data['01-01-2018']['OLCF']['events'])

   for date in dates:
      # get the entry in the output data for this date
      datestr = date.strftime('%m-%d-%Y')
      entry = site_data[datestr]

      # add date to output line
      output_line = datestr

      # parse each site
      for site in ['ALCF','NERSC','OLCF']:
         if site in entry:
            for descr in ['events','walltime','jobs']:
               if descr in entry[site]:
                  output_line += '\t%f' % entry[site][descr]
               else:
                  output_line += '\t0.'
         else:
            output_line += '\t0.\t0.\t0.'

      output_line += '\n'
      outfile.write(output_line)

   logger.info('-> %s',site_data['01-01-2018']['OLCF']['events'])

   json.dump(site_data,open('site_data.json','w'),indent=3,sort_keys=True)
   

def parse_input_by_queue(filename,descr,queue_data,dates,multiplier=1.):

   data = json.load(open(filename))

   for entry in data['jobs']:
      queue = entry['S_SITE']
      date = datetime.datetime.strptime(entry['S_DATE'],'%d-%b-%y %H:%M:%S')
      date = date.strftime('%m-%d-%Y')
      value = float(entry['SUM']) * multiplier

      if queue in queue_data:
         if date in queue_data[queue]:
            if descr in queue_data[queue][date]:
               queue_data[queue][date][descr] += value
            else:
               queue_data[queue][date][descr] = value
         else:
            queue_data[queue][date] = {descr:value}
      else:
         queue_data[queue] = {date:{descr:value}}
      
      if date not in dates:
         dates.append(date)


def parse_input_by_site(filename,descr,output_data,dates,sites,multiplier=1.):

   data = json.load(open(filename))

   for entry in data['jobs']:

      sitename = get_sitename(entry['S_SITE'],sites)

      if entry['S_DATE'] in output_data:
         if sitename in output_data[entry['S_DATE']]:
            if descr in output_data[entry['S_DATE']][sitename]:
               output_data[entry['S_DATE']][sitename][descr] += float(entry['SUM']) * multiplierw
            else:
               output_data[entry['S_DATE']][sitename][descr] = float(entry['SUM']) * multiplier
         else:
            output_data[entry['S_DATE']][sitename] = {descr:float(entry['SUM']) * multiplier}
      else:
         output_data[entry['S_DATE']] = {sitename:{descr:float(entry['SUM']) * multiplier}}
      
      if entry['S_DATE'] not in dates:
         dates.append(entry['S_DATE'])


def sum_by_site(queue_data,sites):

   site_data = {}  # [date][site][descr]

   for queue in queue_data.keys():

      # get site name
      sitename = get_sitename(queue,sites)
      if sitename is None:
         raise Exception('sitename queue %s has no site' % queue)

      for date in queue_data[queue]:

         if date not in site_data:
            site_data[date] = {sitename:{}}
         elif sitename not in site_data[date]:
            site_data[date][sitename] = {}

         for descr in queue_data[queue][date].keys():
            if descr in site_data[date][sitename]:
               site_data[date][sitename][descr] += queue_data[queue][date][descr]
            else:
               site_data[date][sitename][descr] = queue_data[queue][date][descr]

            if 'OLCF' in sitename and '01-01-2018' in date and 'event' in descr:
               logger.info('%10s %20s %10s %10s %10s %10s',date, queue, sitename, descr,site_data[date][sitename][descr],queue_data[queue][date][descr]) 

   logger.info('-> %s',site_data['01-01-2018']['OLCF']['events'])

   return site_data


def get_sitename(queue_name,sites):
   for site in sites.keys():
      if queue_name in sites[site]:
         return site

   return None



if __name__ == "__main__":
   main()
