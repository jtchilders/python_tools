#!/usr/bin/env python
import os,sys,optparse,logging,glob,subprocess,datetime,json
logger = logging.getLogger(__name__)

def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-g','--glob',dest='glob',help='glob to grab each path where athenaMP was run. Use double quotes to use wildcards, for example "tarball_*". This script will look for athena_stdout.txt and athenaMP-workers.../worker_*/AthenaMP.log in each directory matching this glob.')
   parser.add_option('-e','--event-service',dest='event_service',help='flag to treat job data like event service jobs',default=False,action='store_true')
   parser.add_option('-t','--trf-log',dest='trf_log',help='the name of the transform starting log',default='athena_stdout.txt')
   parser.add_option('-o','--output',dest='output',help='output filename for the json data extracted from these logs',default='athenadata.json')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'glob',
                  ]

   for man in manditory_args:
      if options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)
   
   athenadirs = glob.glob(options.glob )
   
   data = {}
   jobid = 0
   for dir in athenadirs:
      logger.info('parsing dir: ' + dir)
      
      jobid += 1
      try:
         # tarball_PandaJob_3283617220_NERSC_Edison_2
         basedir = os.path.basename(dir)
         parts = basedir.split('_')
         jobid = int(parts[2])
      except: pass

      data[jobid] = {}
      
      athenalog = glob.glob(os.path.join(dir,options.trf_log))[0]
      if not os.path.exists(athenalog):
         logger.error(athenalog + ' does not exist')
         continue


      trf_starttime = None 
      start_EVNTtoHITS = datetime.datetime.now()
      end_EVNTtoHITS = datetime.datetime.now()
      start_HITSMerge = datetime.datetime.now()
      end_HITSMerge = datetime.datetime.now()

      trf_endtime = datetime.datetime.now() - datetime.timedelta(days=6000)
      runtime = 0
      
      for line in open(athenalog):
         
         try:
            current_line_date = parse_date_A(line)
            #print current_line_date
            if current_line_date > trf_endtime:
               trf_endtime = current_line_date
         except:
            continue
            
         # 0 running asetup Tue Mar 14 15:14:50 PDT 2017
         if 'running asetup' in line and trf_starttime is None:
            parts = line[:-1].split()
            datestring = ' '.join(parts[4:7]) + ' ' + parts[8]
            date = datetime.datetime.strptime(datestring,'%b %d %H:%M:%S %Y') - datetime.timedelta(hours=7)
            if trf_starttime is None or trf_starttime > date:
               trf_starttime = date
         # PyJobTransforms.<module> 2017-06-25 08:48:34,711 INFO logging set in
         elif 'PyJobTransforms.<module>' in line and trf_starttime is None:
            trf_starttime = parse_date_A(line)
            

         # PyJobTransforms.<module> 2017-03-14 15:19:34,190 INFO logging set in
         if 'PyJobTransforms.<module>' in line and 'INFO logging set in' in line:
            trf_starttime = parse_date_A(line)

         # PyJobTransforms.trfExe.execute 2017-03-14 15:22:11,659 INFO Starting execution of EVNTtoHITS (['./runwrapper.EVNTtoHITS.sh']) 
         if 'Starting execution of EVNTtoHITS' in line:
            start_EVNTtoHITS = parse_date_A(line)
            print str(start_EVNTtoHITS)
         
         # PyJobTransforms.trfExe.execute 2017-03-14 16:12:58,371 INFO EVNTtoHITS executor returns 0
         if 'INFO EVNTtoHITS executor returns' in line:
            end_EVNTtoHITS = parse_date_A(line)

         if 'Non-zero return code from EVNTtoHITS' in line:
            start_HITSMerge = parse_date_A(line)
            end_HITSMerge = start_HITSMerge

         # PyJobTransforms.trfExe.execute 2017-03-14 16:12:59,762 INFO Starting execution of HITSMergeAthenaMP0 (['./runwrapper.HITSMergeAthenaMP0.sh'])
         if 'Starting execution of HITSMergeAthenaMP0' in line:
            start_HITSMerge = parse_date_A(line)

         # PyJobTransforms.trfExe.execute 2017-03-14 16:18:47,112 INFO HITSMergeAthenaMP0 executor returns 0
         if 'INFO HITSMergeAthenaMP0 executor returns' in line:
            end_HITSMerge = parse_date_A(line)

         # 3843 done with asetup Tue Mar 14 16:18:53 PDT 2017
         if 'done with asetup' in line:
            parts = line[:-1].split()
            datestring = ' '.join(parts[5:8]) + ' ' + parts[9]
            trf_endtime = datetime.datetime.strptime(datestring,'%b %d %H:%M:%S %Y')
            runtime = int(parts[0])
      

      data[jobid]['startasetup'] = str(trf_starttime)
      data[jobid]['startTrf'] = str(trf_starttime)
      data[jobid]['start_EVNTtoHITS'] = str(start_EVNTtoHITS)
      data[jobid]['end_EVNTtoHITS'] = str(end_EVNTtoHITS)
      data[jobid]['start_HITSMerge'] = str(start_HITSMerge)
      data[jobid]['end_HITSMerge'] = str(end_HITSMerge)
      data[jobid]['endTrf'] = str(trf_endtime)
      data[jobid]['runtime'] = timedelta_total_seconds(trf_endtime - trf_starttime)

      logger.info(' trf start - end: %s --- %s',trf_starttime,trf_endtime)


      athenaMPworkerdir = os.path.join(dir,'athenaMP-workers-EVNTtoHITS-sim')
      if not os.path.exists(athenaMPworkerdir):
         logger.warning(athenaMPworkerdir + ' does not exist')
         continue

      data[jobid]['workerdata'] = {}

      athenamplogs = glob.glob(os.path.join(athenaMPworkerdir,'worker_*/AthenaMP.log'))

      for athenamplog in athenamplogs:
         worker_num = get_worker_num(athenamplog)
         worker_starttime = None
         
         worker_endtime = trf_endtime
         if not options.event_service:
            worker_endtime = None
         
         eventdata = {}
         for line in open(athenamplog):

            try:
               current_line_date = parse_date_B(line)
               #print current_line_date,'-----',trf_endtime
               if options.event_service and trf_endtime < current_line_date:
                  trf_endtime = current_line_date
            except:
               continue
               current_line_date = None


            
            # 2017-03-14 15:42:25,700 AthMpEvtLoopMgr...   INFO Logs redirected in the AthenaMP event worker PID=20442
            if worker_starttime is None:
               worker_starttime = current_line_date
            
            # 2017-03-14 15:47:17,242 AthenaEventLoopMgr   INFO   ===>>>  start processing event #172104, run #284500 0 events processed so far  <<<===
            if 'INFO   ===>>>  start processing event' in line:
               eventid = int(line.split()[8][1:-1])
               eventdata[eventid] = {'start':str(current_line_date)}
            
            # 2017-03-14 15:53:53,811 AthenaEventLoopMgr   INFO   ===>>>  done processing event #172104, run #284500 1 events processed so far  <<<===
            if 'INFO   ===>>>  done processing event' in line:
               eventid = int(line.split()[8][1:-1])
               eventdata[eventid]['end'] = str(current_line_date)
            
            # 2017-03-14 16:12:46,694 ApplicationMgr       INFO Application Manager Finalized successfully
            if 'INFO Application Manager Finalized successfully' in line:
               worker_endtime = current_line_date

         if options.event_service:
            # loop over event and catch those that did not have an end time and add the global end time
            for id,event in eventdata.iteritems():
               if 'end' not in event:
                  event['end'] = str(trf_endtime)
                  event['lost_event'] = True
            # replace worker endtime with trf_endtime
            worker_endtime = trf_endtime

         data[jobid]['workerdata'][worker_num] = {'eventdata':eventdata,'starttime':str(worker_starttime),'endtime':str(worker_endtime)}


      #logger.info(' trf start - end: %s --- %s',trf_starttime,trf_endtime)

      if options.event_service:
         data[jobid]['end_EVNTtoHITS'] = str(trf_endtime)
         data[jobid]['start_HITSMerge'] = None
         data[jobid]['end_HITSMerge'] = None
         data[jobid]['endTrf'] = str(trf_endtime)
         data[jobid]['runtime'] = timedelta_total_seconds(trf_endtime - trf_starttime)


   #print data



      
      

   json.dump(data,open(options.output,'w'))

# PyJobTransforms.trfExe.execute 2017-03-14 16:18:47,112 
def parse_date_A(line):
   parts = line.split()
   datestring = ' '.join(parts[1:3]).split(',')[0].split('.')[0]
   return datetime.datetime.strptime(datestring,'%Y-%m-%d %H:%M:%S')

# 2017-03-14 15:42:25,700 AthMpEvtLoopMgr...   INFO Logs redirected in the AthenaMP event worker PID=20442
def parse_date_B(line):
   parts = line.split()
   datestring = ' '.join(parts[0:2]).split(',')[0].split('.')[0]
   return datetime.datetime.strptime(datestring,'%Y-%m-%d %H:%M:%S')


def get_worker_num(dir):
   i = dir.find('worker_') + len('worker_')
   f = dir.find('/',i)
   if f == -1:
      return int(dir[i:])
   
   return int(dir[i:f])

def timedelta_total_seconds(timedelta):
   return (timedelta.microseconds + 0.0 + (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

if __name__ == "__main__":
   main()
