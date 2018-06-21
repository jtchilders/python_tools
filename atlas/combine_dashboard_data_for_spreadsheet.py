#!/usr/bin/env python
import os,sys,optparse,logging,json,datetime,copy,numpy
logger = logging.getLogger(__name__)

olcf_date_change = datetime.datetime.strptime('04-Apr-17 00:00:00','%d-%b-%y %H:%M:%S')

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-c','--completedJobs',dest='completedJobs',help='input file in json format from dashboard plot.')
   parser.add_option('-p','--processedEvents',dest='processedEvents',help='input file in json format from dashboard plot.')
   parser.add_option('-w','--wallclock',dest='wallclock',help='input file in json format from dashboard plot.')
   parser.add_option('-o','--output',dest='output',help='output file in csv format for data')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'completedJobs',
                     'processedEvents',
                     'wallclock',
                     'output',
                  ]

   for man in manditory_args:
      if man not in options.__dict__ or options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)
   


   completedJobs = json.load(open(options.completedJobs))
   processedEvents = json.load(open(options.processedEvents))
   wallclock = json.load(open(options.wallclock))

   output_data = {}

   dates = []
   queues = []
   sites = {'OLCF':['ORNL_Titan_MCORE','Titan_long_MCORE','Titan_Harvester_MCORE',
                    'Titan_Harvester_ES','Titan_Harvester_test_MCORE'],
            'ALCF':['ALCF_Theta','ALCF_Theta_ES','ARGO_Mira','ARGO_ANLASC','ARGO_Edison'],
            'NERSC':['NERSC_Cori_p2_mcore','NERSC_Cori_p1_mcore','NERSC_Edison_mcore',
                     'NERSC_Cori_p2_ES','NERSC_Cori_p1_ES','NERSC_Edison_ES',
                     'NERSC_Edison_2','NERSC_Edison','NERSC_Cori','NERSC_Cori_2']
            }


   # get a list of all dates and queues
   for entry in completedJobs['jobs'] + processedEvents['jobs'] + wallclock['jobs']:
      # extract data
      value    = int(entry['SUM'])
      date     = parse_date(entry['S_DATE'])
      datestr  = date_to_string(date)
      queue    = entry['S_SITE']

      dates.append(datestr)
      queues.append(queue)

   # remove duplicates
   dates    = list(set(dates))
   queues   = list(set(queues))

   # convert dates to datetime objects so we can sort by time order
   datetimes = []
   for date in dates:
      datetimes.append(parse_date2(date))

   # sort dates
   datetimes = sorted(datetimes)

   # convert dates back to string
   dates = []
   for datetime in datetimes:
      dates.append(date_to_string(datetime))

   # create basic empty entry:
   date_entry_template = {}
   for queue in queues:
      date_entry_template[queue] = {'processedEvents':0,'wallclock':0,'completedJobs':0}


   # sort data into date rows ordered by queue
   databydate = {}

   # first loop over completed jobs
   for entry in completedJobs['jobs']:
      # extract data
      value    = int(entry['SUM'])
      date     = parse_date(entry['S_DATE'])
      datestr  = date_to_string(date)
      queue    = entry['S_SITE']

      if datestr not in databydate:
         databydate[datestr] = copy.deepcopy(date_entry_template)

      databydate[datestr][queue]['completedJobs'] = value
      
   # loop over the processed events
   for entry in processedEvents['jobs']:
      # extract data
      value    = float(entry['SUM']) * 1000000. # convert from millions
      date     = parse_date(entry['S_DATE'])
      datestr  = date_to_string(date)
      queue    = entry['S_SITE']

      if datestr not in databydate:
         databydate[datestr] = copy.deepcopy(date_entry_template)

      databydate[datestr][queue]['processedEvents'] = value

   # loop over the wall clock
   for entry in wallclock['jobs']:
      # extract data
      value    = float(entry['SUM']) / 60. / 60. # convert from core-seconds
      date     = parse_date(entry['S_DATE'])
      datestr  = date_to_string(date)
      queue    = entry['S_SITE']

      if datestr not in databydate:
         databydate[datestr] = copy.deepcopy(date_entry_template)

      databydate[datestr][queue]['wallclock'] = value

   # Now I have data organized by [date][queue][dataType]

   # I need to correct the ORNL_Titan_MCORE processed events number
   for date in dates:
      if parse_date2(date) < olcf_date_change:
         databydate[date]['ORNL_Titan_MCORE']['processedEvents'] = 100. * databydate[date]['ORNL_Titan_MCORE']['completedJobs']
      else:
         databydate[date]['ORNL_Titan_MCORE']['processedEvents'] = 50. * databydate[date]['ORNL_Titan_MCORE']['completedJobs']


   json.dump(databydate,open('tmp.json','w'))
   #return

   # group queues by site
   groupddatabydate = {}

   # create basic empty entry:
   grouped_entry_template = {}
   for site in sites.keys():
      grouped_entry_template[site] = {'processedEvents':0,'wallclock':0,'completedJobs':0}

   for date in dates:
      #print 'date',date
      if date not in groupddatabydate:
         groupddatabydate[date] = copy.deepcopy(grouped_entry_template)

      databydate_entry = databydate[date]
      #print 'entry',databydate_entry
      # loop over the queues in the data
      for queue in databydate_entry:
         #print 'queue',queue
         # loop over the possible sites
         for site in sites:
            # if this queue is part of this site, then add its statistics to the sites
            if queue in sites[site]:
               #print 'queue %s is part of site %s' % (queue,site)
               # loop over all the stats and add them
               for stat in groupddatabydate[date][site]:
                  groupddatabydate[date][site][stat] += databydate[date][queue][stat]
               #print 'grouped',groupddatabydate[date][site]

   # now I have data organized by [date][site][dataType]

   # want to now sum by month
   summeddata = {}

   for date in dates:
      tmpdate = parse_date2(date)




   #print groupddatabydate

   # output csv

   output = ',ALCF,,,NERSC,,,OLCF\n'
   output += 'date,processedEvents,wallclock,completedJobs,processedEvents,wallclock,completedJobs,processedEvents,wallclock,completedJobs\n'

   for date in dates:

      output += '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
         date,
         groupddatabydate[date]['ALCF']['processedEvents'],
         groupddatabydate[date]['ALCF']['wallclock'],
         groupddatabydate[date]['ALCF']['completedJobs'],
         groupddatabydate[date]['NERSC']['processedEvents'],
         groupddatabydate[date]['NERSC']['wallclock'],
         groupddatabydate[date]['NERSC']['completedJobs'],
         groupddatabydate[date]['OLCF']['processedEvents'],
         groupddatabydate[date]['OLCF']['wallclock'],
         groupddatabydate[date]['OLCF']['completedJobs'],
         )

   open(options.output,'w').write(output)


   # print the events per wallclock per queue
   evt_per_wlclk = {}
   for queue in queues:
      evt_per_wlclk[queue] = []

   # create list of these values organized by queue
   for date,queuedata in databydate.iteritems():
      for queue,stats in queuedata.iteritems():
         if stats['wallclock'] > 0:
            evt_per_wlclk[queue].append(stats['processedEvents']/stats['wallclock'])

   # print the averages and stddev per queue
   print 'queue,events per core-hour'
   for queue,datalist in evt_per_wlclk.iteritems():
      if len(datalist) > 0:
         print '%s,%s,%s' % (queue,numpy.mean(datalist),numpy.std(datalist))









def parse_date(date):
   return datetime.datetime.strptime(date,'%d-%b-%y %H:%M:%S')


def parse_date2(date):
   return datetime.datetime.strptime(date,'%m-%d-%Y')

def date_to_string(date):
   return date.strftime('%m-%d-%Y')




if __name__ == "__main__":
   main()
