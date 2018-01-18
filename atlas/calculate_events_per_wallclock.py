#!/usr/bin/env python
import os,sys,optparse,logging,json,datetime,numpy
logger = logging.getLogger(__name__)

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-p','--processedEvents',dest='processedEvents',help='input file in json format from dashboard plot "NEvents Processed in MEvents"')
   parser.add_option('-c','--consumptions',dest='consumptions',help='input file in json format from dashboard plot "Wall Clock consumption Good Jobs"')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'processedEvents',
                     'consumptions',
                  ]

   for man in manditory_args:
      if man not in options.__dict__ or options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)
   


   procEvntsInput = json.load(open(options.processedEvents))
   wallTimeInput = json.load(open(options.consumptions))

   input_by_site = {}

   for data in procEvntsInput['jobs']:
      # data
      #print input_by_site
      # extract data
      value = float(data['SUM']) * 1000000. # convert from millions
      date  = parse_date(data['S_DATE'])
      datestr = date_to_string(date)
      site  = data['S_SITE']

      if site in input_by_site:
         if datestr in input_by_site[site]:
            input_by_site[site][datestr]['processedEvents'] = value
         else:
            input_by_site[site][datestr] = {'processedEvents':value}
      else:
         input_by_site[site] = {datestr: {'processedEvents':value}}

   #print input_by_site

   for data in wallTimeInput['jobs']:
      # extract data
      value = float(data['SUM']) / 60. / 60. # convert to core-hours
      date  = parse_date(data['S_DATE'])
      datestr = date_to_string(date)
      site  = data['S_SITE']

      if site in input_by_site:
         if datestr in input_by_site[site]:
            input_by_site[site][datestr]['wallTime'] = value
         else:
            input_by_site[site][datestr] = {'wallTime':value}
      else:
         tmp = {datestr: {'wallTime':value}}
         input_by_site[site] = tmp

   
   for site,data in input_by_site.iteritems():

      events_per_corehour = []
      for datestr,stats in data.iteritems():
         if 'processedEvents' in stats and 'wallTime' in stats:
            events_per_corehour.append(stats['processedEvents']/stats['wallTime'])

      if len(events_per_corehour) > 0:
         print site,':',numpy.mean(events_per_corehour),'+/-',numpy.std(events_per_corehour)


#parse date "01-Feb-17 00:00:00"
def parse_date(date):
   return datetime.datetime.strptime(date,'%d-%b-%y %H:%M:%S')

def date_to_string(date):
   return date.strftime('%m-%d-%Y')



if __name__ == "__main__":
   main()
