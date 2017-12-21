#!/usr/bin/env python
import os,sys,optparse,logging,json,ROOT,datetime,subprocess,array
from dateutil import parser as dateutilparser
logger = logging.getLogger(__name__)
sys.path.append('/global/homes/p/parton/python_tools')
from analysis import CalcMean

#ROOT.gROOT.SetBatch()
ROOT.gStyle.SetOptStat(0)
colors = [ROOT.kYellow,ROOT.kBlue,ROOT.kGreen,ROOT.kRed]
ROOT.gStyle.SetPalette(len(colors),array.array('i',colors))

def get_job_runtime(jobid=None):
   cmd = 'sacct -j %s.batch -o Elapsed --noheader'
   
   if jobid is None:
      cwd = os.getcwd()
      base = os.path.basename(cwd)
      jobid = base.split('-')[1]
   
   p = subprocess.Popen(cmd % jobid,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
   stdout,stderr = p.communicate()
   lines = stdout.split('\n')

   if p.returncode != 0:
      raise Exception('sacct cmd failed: ' + stdout + '\n' + stderr)
   #print cmd % jobid
   
   # time format: DD-HH:MM:SS
   fulltimeformat = lines[0]
   parts = fulltimeformat.split('-')
   days = 0
   hours = 0
   minutes = 0
   seconds = 0
   if len(parts) == 1:
      # no day
      time = parts[0]
      tparts = parts[0].split(':')
      if len(tparts) == 2:
         # no hours
         minutes = int(tparts[0])
         seconds = int(tparts[1])
      else:
         hours = int(tparts[0])
         minutes = int(tparts[1])
         seconds = int(tparts[2])
   else:
      days = int(parts[0])
      time = parts[1]
      tparts = time.split(':')
      if len(tparts) == 2:
         # no hours
         minutes = int(tparts[0])
         seconds = int(tparts[1])
      else:
         hours = int(tparts[0])
         minutes = int(tparts[1])
         seconds = int(tparts[2])

   return datetime.timedelta(days=days,hours=hours,minutes=minutes,seconds=seconds)


def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-a','--athena',dest='athena_input',help='the data from the athena log files')
   parser.add_option('-p','--pilot',dest='pilot_input',help='the data from the pilot log files',default=None)
   parser.add_option('-e','--event-service',dest='event_service',help='flag to treat job data like event service jobs',default=False,action='store_true')
   parser.add_option('-j','--jobid',dest='jobid',help='batch job id, if not provided, will try to extract from path name',default=None)
   options,args = parser.parse_args()

   
   manditory_args = [
                     'athena_input',
                  ]

   for man in manditory_args:
      if options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)

   
   events_processed = 0
   coreminutes = 0
   coreminmean = CalcMean.CalcMean()

   runtime_mean = CalcMean.CalcMean()

   eventtimemean = CalcMean.CalcMean()

   average_worker_events_processed = CalcMean.CalcMean()

   pilotdata = None
   if options.pilot_input:
      pilotdata = json.load(open(options.pilot_input))
   athenadata = json.load(open(options.athena_input))
   
   can = ROOT.TCanvas('can','can',0,0,800,600)

   earliest_starttime = datetime.datetime.now()
   latest_endtime = datetime.datetime.now() - datetime.timedelta(days=10)
   
   occupancy = ROOT.TH1D('occupancy',';occupancy as a fraction of node lifetime',100,0,1)

   for jobid in athenadata.keys():

      try:
         adata = athenadata[jobid]
         job_events_processed = 0

         if options.event_service:
            jobstart = get_datetime_A(adata['startTrf'])
            jobend   = get_datetime_A(adata['endTrf'])
         else:
         
            try:
               astart = get_datetime_A(adata['startasetup'])
            except:
               logger.error('failed to parse startasetup: %s',adata['startasetup'])
            if astart < earliest_starttime: earliest_starttime = astart
            
            try:
               aend   = get_datetime_A(adata['end_HITSMerge'])
            except:
               logger.error('failed to parse end_HITSMerge: %s',adata['end_HITSMerge'])
               
            if aend > latest_endtime: latest_endtime = aend
            jobstart = astart
            jobend   = aend

         logger.info(' job start %s stop %s',jobstart,jobend)

         runtime = jobend - jobstart
         runtime_sec = runtime.total_seconds()
         runtime_min = int(runtime_sec/60.)
         
         start = jobstart
         end   = jobend
         
         pdata = None
         if pilotdata is not None:
            pdata = pilotdata[jobid]
            pilotstart = get_datetime_A(pdata['start']) + datetime.timedelta(hours=7)
            if pilotstart < earliest_starttime: earliest_starttime = pilotstart
            jobstart = get_datetime_A(pdata['athena_start']) + datetime.timedelta(hours=7)
            jobend = get_datetime_A(pdata['athena_end']) + datetime.timedelta(hours=7)
            pilotend   = get_datetime_A(pdata['end']) + datetime.timedelta(hours=7)
            if pilotend > latest_endtime: latest_endtime = pilotend
         
         
         
            runtime = pilotend - pilotstart
            runtime_sec = timedelta_total_seconds(runtime)
            runtime_min = int(runtime_sec/60.)

            logger.info('pilotstart %s pilotend %s',pilotstart,pilotend)
            logger.info(' job start %s stop %s',jobstart,jobend)
            
            events_processed += pdata['maxevents']
            coreminutes += pdata['ncores']*runtime_min

            start = pilotstart
            end   = pilotend
         
         #logger.info('jobstart   %s jobend   %s',jobstart,jobend)
         logger.info('runtime %s',runtime)

         runtime_seconds = timedelta_total_seconds(runtime)
         runtime_minutes = runtime_seconds/60.
         runtime_mean.add_value(runtime_minutes)

         transition_gap = max(int(runtime_minutes*0.01),1)
         
         ybins = len(adata['workerdata']) + 1
         if pilotdata:
            ybins += 1

         timeline = ROOT.TH2D('timeline','1=running,2=eventloop,3=lostevent;time since start (minutes); processes',runtime_min,0,runtime_min,ybins,0,ybins)
         timeline.SetMaximum(4)
         timeline.SetMinimum(0)
         # set contour
         levels = [0,1,2,3,4]
         timeline.SetContour(len(levels),array.array('d',levels))

         # fill panda job running time
         jobstart_min = int(timedelta_total_seconds(jobstart - start) / 60.)
         jobend_min = int(timedelta_total_seconds(jobend - start) / 60.)

         for xbin in xrange(jobstart_min,jobend_min):
            timeline.Fill(xbin,0.5,0.5)

         # fill time for asetup
         asetup_start_min = int(timedelta_total_seconds(get_datetime_A(adata['startasetup']) - start) / 60.)
         asetup_end_min = int(timedelta_total_seconds(get_datetime_A(adata['startTrf']) - start) / 60.)
         
         if asetup_start_min != asetup_end_min:
            logger.info(' asetup start min: %s stop min: %s',asetup_start_min,asetup_end_min)

            for xbin in xrange(asetup_start_min,asetup_end_min):
               timeline.Fill(xbin,1.5,1.5)
         
         # fill log.EVNTtoHITS time
         if ( timedelta_total_seconds(get_datetime_A(adata['start_EVNTtoHITS']) - start) != 0 and 
              timedelta_total_seconds(get_datetime_A(adata['end_EVNTtoHITS']) - end) != 0): 
            startEH_min = int(timedelta_total_seconds(get_datetime_A(adata['start_EVNTtoHITS']) - start) / 60.)
            endEH_min = int(timedelta_total_seconds(get_datetime_A(adata['end_EVNTtoHITS']) - start) / 60.)

            logger.info(' start EH: %s end: %s',startEH_min,endEH_min)
            

            for xbin in xrange(startEH_min,endEH_min):
               timeline.Fill(xbin,1.5,2.5)

         # fill log.HITSMerge time
         if not options.event_service:
            startHM_min = int(timedelta_total_seconds(get_datetime_A(adata['start_HITSMerge']) - start) / 60.)
            endHM_min = int(timedelta_total_seconds(get_datetime_A(adata['end_HITSMerge']) - start) / 60.)

            logger.debug(' start HM: %s end: %s',startHM_min,endHM_min)
                  
            for xbin in xrange(startHM_min,endHM_min):
               timeline.Fill(xbin,1.5,3.5)

         # fill workers event times
         for worker_num,workerdata in adata['workerdata'].iteritems():
            worker_events_processed = 0
            ybin = int(worker_num) + 2.5

            logger.debug('worker num %s',worker_num)
            
            try:
               worker_start = int(timedelta_total_seconds(get_datetime_A(workerdata['starttime']) - start) / 60.)
            except:
               logger.error('failed to parse workerdata: %s',workerdata)

            try:
               worker_end = int(timedelta_total_seconds(get_datetime_A(workerdata['endtime']) - start) / 60.)
            except:
               worker_end = int( timedelta_total_seconds(end - start) / 60.)

            logger.debug('   worker start %s end %s',worker_start,worker_end)

            for xbin in xrange(worker_start,worker_end):
               timeline.Fill(xbin,ybin,1.5)

            for evntid,evntdata in workerdata['eventdata'].iteritems():
               evnt_start = int(timedelta_total_seconds(get_datetime_A(evntdata['start']) - start) / 60.)
               evnt_end = int(timedelta_total_seconds(get_datetime_A(evntdata['end']) - start) / 60. - transition_gap)

               eventtimemean.add_value(evnt_end-evnt_start)

               if 'lost_event' not in evntdata:
                  worker_events_processed += 1

                  for xbin in xrange(evnt_start,evnt_end):
                     timeline.Fill(xbin,ybin,1)
               else:
                  evnt_end = int(timedelta_total_seconds(get_datetime_A(evntdata['end']) - start) / 60.)
                  for xbin in xrange(evnt_start,evnt_end):
                     timeline.Fill(xbin,ybin,2)

               

            average_worker_events_processed.add_value(worker_events_processed)
            job_events_processed += worker_events_processed
         
         nbins = timeline.GetNbinsX() * timeline.GetNbinsY()
         filledbins = 0
         for xbin in xrange(timeline.GetNbinsX()):
            for ybin in xrange(timeline.GetNbinsY()):
               if timeline.GetBinContent(timeline.FindBin(xbin+1,ybin+1)) == 2.5:
                  filledbins += 1
         print nbins,filledbins
         occupancy.Fill(float(filledbins)/float(nbins))
         timeline.SetTitle('%s' % jobid)
         timeline.Draw('colz')
         rank = 0
         if pilotdata:
            try:
               rank = pdata['rank']
            except: pass

         can.SaveAs(('%05i' % rank) + '_' + str(jobid) + '.timeline.ps')
      except:
         logger.exception('')

      logger.info('events processed by job: %s',job_events_processed)

   occupancy.Draw()
   can.SaveAs('occupancy.ps')

   #logger.info('slurm runtime:          ' + str(runtime))
   #logger.info('slurm runtime(min):     ' + str(timedelta_total_seconds(runtime)/60.))
   logger.info('longest  runtime:  ' + str(latest_endtime - earliest_starttime))
   try:
      logger.info('core-minutes per event: %.2f' % (coreminutes/events_processed))
   except: pass
   logger.info('average pilot runtime:  %s',runtime_mean.get_string())
   logger.info('average event processing time: %s',eventtimemean.get_string()) 
   logger.info('average number of events processed by each worker: %s',average_worker_events_processed.get_string())

# 2017-03-14 23:19:10
def get_datetime_A(date_string):
   try:
      return dateutilparser.parse(date_string)
      #return datetime.datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S')
   except:
      logger.error('failed to parse %s',date_string)
      raise

def timedelta_total_seconds(timedelta):
    return (timedelta.microseconds + 0.0 + (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

if __name__ == "__main__":
   main()