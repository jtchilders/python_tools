#!/usr/bin/env python
import os,sys,optparse,logging,json,ROOT,datetime,subprocess,array,glob
from dateutil import parser as dateutilparser
from analysis import CalcMean
logger = logging.getLogger(__name__)
sys.path.append('/global/homes/p/parton/python_tools')

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
   parser.add_option('-o','--outfile',dest='outfile',help='name of the file where json stats will be stored',default='jobstats.json')
   parser.add_option('-b','--batchmode',dest='batchmode',help='turn off plots appearing on screen',default=False,action='store_true')
   options,args = parser.parse_args()

   
   manditory_args = [
                     'athena_input',
                  ]

   for man in manditory_args:
      if options.__dict__[man] is None:
         logger.error('Must specify option: ' + man)
         parser.print_help()
         sys.exit(-1)

   if options.batchmode:
      ROOT.gROOT.SetBatch()
   
   events_processed = 0
   coreminutes = 0
   # coreminmean = CalcMean.CalcMean()

   runtime_mean = CalcMean.CalcMean()

   eventtimemean = CalcMean.CalcMean()

   average_worker_events_processed = CalcMean.CalcMean()

   pilotdata = None
   if options.pilot_input:
      pilotdata = json.load(open(options.pilot_input))
   athenadata = json.load(open(options.athena_input))
   
   can = ROOT.TCanvas('can','can',0,0,800,600)

   earliest_starttime = datetime.datetime.now()
   latest_endtime     = datetime.datetime.now() - datetime.timedelta(weeks=500)
   
   occupancy    = ROOT.TH1D('occupancy',';occupancy as a fraction of node lifetime',100,0,1)
   evttime      = ROOT.TH1D('evttime',';time to process one event (min)',100,0,100)
   evttime_lost = ROOT.TH1D('evttime_lost',';time to process one event (min)',100,0,100)
   init_time    = ROOT.TH1D('init_time',';time first event starts (min)',100,0,200)

   output_data = {'summary':{},'job_data':{}}

   all_events = 0
   # loop over yoda ranks or individual nodes running pilots
   # so jobid will be yoda rank number or just node number
   for jobid in sorted(athenadata.keys()):
      logger.info('processing jobid %s',jobid)
      output_data['job_data'][jobid] = {}
      try:
         adata = athenadata[jobid]
         job_events_processed = 0

         if options.event_service:
            jobstart = get_datetime_A(adata['earliest_time'])
            jobend   = get_datetime_A(adata['latest_time'])
            if jobstart < earliest_starttime:
               earliest_starttime = jobstart
            if jobend > latest_endtime:
               latest_endtime = jobend
         else:
         
            try:
               astart = get_datetime_A(adata['startasetup'])
            except Exception:
               logger.error('failed to parse startasetup: %s',adata['startasetup'])
            if astart < earliest_starttime:
               earliest_starttime = astart
            
            try:
               aend   = get_datetime_A(adata['end_HITSMerge'])
            except Exception:
               logger.error('failed to parse end_HITSMerge: %s',adata['end_HITSMerge'])
               
            if aend > latest_endtime:
               latest_endtime = aend
            jobstart = astart
            jobend   = aend

         logger.info(' job start %s stop %s',jobstart,jobend)

         runtime = jobend - jobstart
         
         start = jobstart
         end   = jobend

         
         pdata = None
         if pilotdata is not None:
            pdata = pilotdata[jobid]
            pilotstart = get_datetime_A(pdata['start']) + datetime.timedelta(hours=7)
            if pilotstart < earliest_starttime:
               earliest_starttime = pilotstart
            jobstart = get_datetime_A(pdata['athena_start']) + datetime.timedelta(hours=7)
            jobend = get_datetime_A(pdata['athena_end']) + datetime.timedelta(hours=7)
            pilotend   = get_datetime_A(pdata['end']) + datetime.timedelta(hours=7)
            if pilotend > latest_endtime:
               latest_endtime = pilotend
         
            runtime = pilotend - pilotstart
            runtime_seconds = timedelta_total_seconds(runtime)
            runtime_minutes = runtime_seconds / 60.

            logger.info('pilotstart %s pilotend %s',pilotstart,pilotend)
            logger.info(' job start %s stop %s',jobstart,jobend)
            
            events_processed += pdata['maxevents']
            coreminutes += pdata['ncores'] * runtime_minutes

            start = pilotstart
            end   = pilotend
         
         logger.info('runtime %s',runtime)

         runtime_seconds = timedelta_total_seconds(runtime)
         runtime_minutes = int(runtime_seconds / 60.)
         runtime_mean.add_value(runtime_minutes)

         output_data['job_data'][jobid]['runtime'] = runtime_seconds

         transition_gap = max(int(runtime_minutes * 0.01),1)
         
         ybins = len(adata['workerdata']) + 1
         if pilotdata:
            ybins += 1

         timeline = ROOT.TH2D('%05d_timeline' % int(jobid),'1=running,2=eventloop,3=lostevent;time since start (minutes); processes',runtime_minutes,0,runtime_minutes,ybins,0,ybins)
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
            logger.debug(' asetup start min: %s stop min: %s',asetup_start_min,asetup_end_min)

            for xbin in xrange(asetup_start_min,asetup_end_min):
               timeline.Fill(xbin,1.5,1.5)
         
         # fill log.EVNTtoHITS time
         if (timedelta_total_seconds(get_datetime_A(adata['start_EVNTtoHITS']) - start) != 0 and
              timedelta_total_seconds(get_datetime_A(adata['end_EVNTtoHITS']) - end) != 0):
            startEH_min = int(timedelta_total_seconds(get_datetime_A(adata['start_EVNTtoHITS']) - start) / 60.)
            endEH_min = int(timedelta_total_seconds(get_datetime_A(adata['end_EVNTtoHITS']) - start) / 60.)

            logger.debug(' start EH: %s end: %s',startEH_min,endEH_min)
            

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
         ordered_workers = sorted(adata['workerdata'].keys())
         for worker_num in ordered_workers:
            workerdata = adata['workerdata'][worker_num]
            worker_events_processed = 0
            ybin = int(worker_num) + 2.5

            logger.debug('worker num %s',worker_num)
            
            try:
               worker_start = int(timedelta_total_seconds(get_datetime_A(workerdata['starttime']) - start) / 60.)
            except Exception:
               logger.error('failed to parse workerdata: %s',workerdata)

            try:
               worker_end = int(timedelta_total_seconds(get_datetime_A(workerdata['endtime']) - start) / 60.)
            except Exception:
               worker_end = int(timedelta_total_seconds(end - start) / 60.)

            logger.debug('   worker start %s end %s',worker_start,worker_end)

            for xbin in xrange(worker_start,worker_end):
               timeline.Fill(xbin,ybin,1.5)

            earliest_event_time = datetime.now()
            for evntid,evntdata in workerdata['eventdata'].iteritems():
               evnt_start = int(timedelta_total_seconds(get_datetime_A(evntdata['start']) - start) / 60.)
               evnt_end = int(timedelta_total_seconds(get_datetime_A(evntdata['end']) - start) / 60. - transition_gap)

               if evnt_start < earliest_event_time:
                  earliest_event_time = evnt_start

               eventtimemean.add_value(evnt_end - evnt_start)

               if 'lost_event' not in evntdata:
                  worker_events_processed += 1
                  job_events_processed += 1
                  evttime.Fill(evnt_end - evnt_start)
                  for xbin in xrange(evnt_start,evnt_end):
                     timeline.Fill(xbin,ybin,1)
               else:
                  evnt_end = int(timedelta_total_seconds(get_datetime_A(evntdata['end']) - start) / 60.)
                  evttime_lost.Fill(evnt_end - evnt_start)
                  for xbin in xrange(evnt_start,evnt_end):
                     timeline.Fill(xbin,ybin,2)

            init_time.Fill(int(timedelta_total_seconds(earliest_event_time - start) / 60.))

            average_worker_events_processed.add_value(worker_events_processed)
         
         nbins = timeline.GetNbinsX() * timeline.GetNbinsY()
         filledbins = 0
         for xbin in xrange(timeline.GetNbinsX()):
            for ybin in xrange(timeline.GetNbinsY()):
               if timeline.GetBinContent(timeline.FindBin(xbin + 1,ybin + 1)) == 2.5:
                  filledbins += 1
         occupancy.Fill(float(filledbins) / float(nbins))
         timeline.SetTitle('%s' % jobid)
         timeline.Draw('colz')
         rank = 0
         if pilotdata:
            try:
               rank = pdata['rank']
            except Exception:
               pass

         can.SaveAs(('%05i' % rank) + '_' + str(jobid) + '.timeline.ps')


         output_data['job_data'][jobid]['events_processed'] = job_events_processed

         output_data['job_data'][jobid]['occupancy'] = float(filledbins) / float(nbins)
      except Exception:
         logger.exception('')

   
      logger.info('events processed by job %s: %s',jobid,job_events_processed)
      all_events += job_events_processed

   logger.info('total events processed: %s',all_events)
   output_data['summary']['evts_processed'] = all_events
   output_data['summary']['occupancy_mean'] = occupancy.GetMean()
   output_data['summary']['occupancy_sigma'] = occupancy.GetStdDev()

   occupancy.Draw()
   can.SaveAs('occupancy.ps')
   evttime.Draw()
   evttime_lost.SetLineColor(ROOT.kRed)
   evttime_lost.Draw('same')
   can.SaveAs('evttime.ps')

   init_time.Draw()
   can.SaveAs('init_time.ps')

   logger.info('occupancy: %6.4f +/- %6.4f',occupancy.GetMean(),occupancy.GetStdDev())

   output_data['summary']['runtime'] = timedelta_total_seconds(latest_endtime - earliest_starttime)
   logger.info('longest  runtime:  ' + str(latest_endtime - earliest_starttime))
   try:
      logger.info('core-minutes per event: %.2f' % (coreminutes / events_processed))
      output_data['summary']['core_min_per_evt'] = (coreminutes / events_processed)
   except Exception:
      pass
   try:
      logger.info('average pilot runtime:  %s',runtime_mean.get_string())
      output_data['summary']['pilot_runtime_mean'] = runtime_mean.calc_mean()
      output_data['summary']['pilot_runtime_sigma'] = runtime_mean.calc_sigma()
   except Exception:
      pass
   logger.info('average event processing time: %s',eventtimemean.get_string())
   output_data['summary']['evt_proc_time_mean'] = eventtimemean.calc_mean()
   output_data['summary']['evt_proc_time_sigma'] = eventtimemean.calc_sigma()
   logger.info('average number of events processed by each worker: %s',average_worker_events_processed.get_string())
   output_data['summary']['evt_proc_by_worker_mean'] = average_worker_events_processed.calc_mean()
   output_data['summary']['evt_proc_by_worker_sigma'] = average_worker_events_processed.calc_sigma()

   json.dump(output_data,open(options.outfile,'w'),indent=4, sort_keys=True)


   #### YODA Plots #####
   mpi_er_wait_time = CalcMean.CalcMean()
   h_er_wait_time = ROOT.TH1I('er_wait_time','Droid waiting time for Yoda to send event range;time (s);droids',100,0,5000)
   mpi_jr_wait_time = CalcMean.CalcMean()
   h_jr_wait_time = ROOT.TH1I('jr_wait_time','Droid waiting time for Yoda to send job def;time (s);droids',100,0,500)
   mpi_send_wait_time = CalcMean.CalcMean()
   h_send_wait_time = ROOT.TH1I('send_wait_time','Droid waiting time for MPI to complete send;time (s);droids',200,0,1800)
   if options.event_service:
      yodalogs = sorted(glob.glob('yoda_droid_*.log'))

      logger.info('found %s yoda_droid logs',len(yodalogs))

      # plots including all droids
      for i in range(1,len(yodalogs)):  # exclude rank 0
         ####  plot time between request job/ER and received
         yodalog = yodalogs[i]
         logger.info('processing %s',yodalog)
         # first job request

         cmd = 'grep "MPIService|.*REQUEST_JOB" ' + yodalog
         req_job_out,err = run_cmd(cmd)
         req_job_out = req_job_out.split('\n')

         cmd = 'grep "JobComm|received job definition" ' + yodalog
         new_job_out,err = run_cmd(cmd)
         new_job_out = new_job_out.split('\n')

         if len(req_job_out) > 0 and len(new_job_out) > 0:
            req_time = get_datetime_A(req_job_out[0].split('|')[0])
            new_time = get_datetime_A(new_job_out[0].split('|')[0])
            wait_time = timedelta_total_seconds(new_time - req_time)
            mpi_jr_wait_time.add_value(wait_time)
            h_jr_wait_time.Fill(wait_time)

         # then event range requests

         cmd = 'grep "MPIService|.*REQUEST_EVENT_RANGES" ' + yodalog
         req_er_out,err = run_cmd(cmd)

         req_er = []
         for line in req_er_out.split('\n'):
            req_er.append(get_datetime_A(line.split('|')[0]))

         cmd = 'grep "MPIService|.*NEW_EVENT_RANGES" ' + yodalog
         new_er_out,err = run_cmd(cmd)

         new_er = []
         for line in new_er_out.split('\n'):
            new_er.append(get_datetime_A(line.split('|')[0]))

         
         for i in range(len(req_er)):
            request_time = req_er[i]
            if i < len(new_er):
               received_time = new_er[i]
               wait_time = timedelta_total_seconds(received_time - request_time)
               mpi_er_wait_time.add_value(wait_time)
               h_er_wait_time.Fill(wait_time)

         #### Now plot the send waiting time for MPI 'non-blocking' send

         cmd = 'grep "MPIService|wait for send to complete" ' + yodalog
         out,err = run_cmd(cmd)
         start_waits = []
         for line in out.split('\n'):
            start_waits.append(get_datetime_A(line.split('|')[0]))

         cmd = 'grep "MPIService|send complete" ' + yodalog
         out,err = run_cmd(cmd)
         end_waits = []
         for line in out.split('\n'):
            end_waits.append(get_datetime_A(line.split('|')[0]))

         for i in range(len(start_waits)):
            start = start_waits[i]
            if i < len(end_waits):
               wait = timedelta_total_seconds(end_waits[i] - start)
               mpi_send_wait_time.add_value(wait)
               h_send_wait_time.Fill(wait)



      logger.info('average mpi_er_wait_time: %s',mpi_er_wait_time.get_string())
      output_data['summary']['mpi_er_wait_time_mean']    = mpi_er_wait_time.calc_mean()
      output_data['summary']['mpi_er_wait_time_sigma']   = mpi_er_wait_time.calc_sigma()

      logger.info('average mpi_jr_wait_time: %s',mpi_jr_wait_time.get_string())
      output_data['summary']['mpi_jr_wait_time_mean']    = mpi_jr_wait_time.calc_mean()
      output_data['summary']['mpi_jr_wait_time_sigma']   = mpi_jr_wait_time.calc_sigma()
      
      logger.info('average mpi_send_wait_time: %s',mpi_send_wait_time.get_string())
      output_data['summary']['mpi_send_wait_time_mean']    = mpi_send_wait_time.calc_mean()
      output_data['summary']['mpi_send_wait_time_sigma']   = mpi_send_wait_time.calc_sigma()

      # save plots
      h_er_wait_time.Draw()
      can.SaveAs('er_wait_time.ps')
      h_jr_wait_time.Draw()
      can.SaveAs('jr_wait_time.ps')
      can.SetLogy(True)
      h_send_wait_time.Draw()
      can.SaveAs('send_wait_time.ps')

   # dump all the json data
   json.dump(output_data,open(options.outfile,'w'),indent=4, sort_keys=True)



def run_cmd(cmd):
   p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
   return p.communicate()


# 2017-03-14 23:19:10
def get_datetime_A(date_string):
   try:
      return dateutilparser.parse(date_string)
   except Exception:
      logger.error('failed to parse %s',date_string)
      raise


def timedelta_total_seconds(timedelta):
    return (timedelta.microseconds + 0.0 + (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6


if __name__ == "__main__":
   main()
