#!/usr/bin/env python
import os,sys,optparse,logging,glob,subprocess,datetime,ROOT
logger = logging.getLogger(__name__)

ROOT.gStyle.SetOptStat('mr')
ROOT.gROOT.SetBatch()

def get_batch_id(logfilename):
   startindex = logfilename.find('slurm-') + len('slurm-')
   endindex = logfilename.find('/',startindex)
   jobid = logfilename[startindex:endindex]
   return jobid

def get_job_state(logfilename):
   cmd = 'sacct -j %s.batch -o State --noheader'
   
   jobid = get_batch_id(logfilename)

   p = subprocess.Popen(cmd % jobid,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
   stdout,stderr = p.communicate()
   lines = stdout.split('\n')

   if p.returncode != 0:
      raise Exception('sacct cmd failed: ' + stdout + '\n' + stderr)
   #print cmd % jobid

   return lines[0]

def get_job_timelimit(logfilename):
   cmd = 'sacct -j %s -o TimeLimit --noheader'

   jobid = get_batch_id(logfilename)

   p = subprocess.Popen(cmd % jobid,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
   stdout,stderr = p.communicate()
   lines = stdout.split('\n')

   if p.returncode != 0:
      raise Exception('sacct cmd failed: ' + stdout + '\n' + stderr)
   #print cmd % jobid

   timestr = lines[0]
   parts = timestr.split(':')
   hours = int(parts[0])
   minutes = int(parts[1])
   seconds = int(parts[2])
   td = datetime.timedelta(hours=hours,minutes=minutes,seconds=seconds)

   return td


def get_job_runtime(logfilename):
   cmd = 'sacct -j %s.batch -o Elapsed --noheader'

   jobid = get_batch_id(logfilename)

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

   return jobid,datetime.timedelta(days=days,hours=hours,minutes=minutes,seconds=seconds)




def main():
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s:%(name)s:%(message)s')

   parser = optparse.OptionParser(description='')
   parser.add_option('-g','--glob',dest='glob',help='glob to grab pilot log files to parse')
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
   
   can = ROOT.TCanvas('can','can',0,0,800,600)

   runtime = ROOT.TH1F('runtime','; pilot runtime over batch job runtime; number of jobs',100,0,1) 
   bj_runtimes = {}
   bj_plots = {}
   
   for logfile in logfiles:
      print logfile
      rank_num = int(logfile.split('.')[1])
      datetimes = []
      starttime = None
      endtime = datetime.datetime.now()
      batchid = get_batch_id(logfile)
      try:
         batch_job_time = bj_runtimes[batchid]
      except KeyError:
         try:
            batchid,batch_job_time = get_job_runtime(logfile)
         except:
            continue
         bj_runtimes[batchid] = batch_job_time
         bj_plots[batchid] = ROOT.TH1F('jobruntime_%s' % batchid,'batch id %s; pilot runtime over batch job runtime; number of jobs' % batchid,100,0,1) 
      except ValueError:
         continue
      

      for line in open(logfile):
         
         if line.startswith('2017') and starttime is None:
            starttime = parse_datetime(line)
         
         # 2017-03-14 23:23:20| 18301|pUtil.py    | Done, using system exit to quit
         if 'Done, using system exit to quit' in line:
            endtime = parse_datetime(line)

      
      batch_runtime = timedelta_total_seconds(bj_runtime[batchid])
      pilot_runtime = timedelta_total_seconds(endtime-starttime)

      runtime.Fill(pilot_runtime*1./batch_runtime)
      bj_plots[batchid].Fill(pilot_runtime*1./batch_runtime)

      #print '  ',batch_job_time, (endtime-starttime)

   runtime.Draw('hist')
   can.SaveAs('runtime.png')
   #raw_input('...')
   
   runtime_vs_job = ROOT.TH2F('runtime_vs_job','; pilot runtime over batch job runtime; batch job id',100,0,1,len(bj_plots),0,len(bj_plots))
   
   keys = sorted(bj_plots.keys())

   for i in xrange(len(keys)):
      id = keys[i]
      plot = bj_plots[id]
      #plot.Draw('hist')
      #can.SaveAs('runtime_%s.png'%id)
      if plot.Integral() > 1:
         plot.Scale(1./plot.Integral())
         for binnum in xrange(1,plot.GetNbinsX()+1):
            runtime_vs_job.SetBinContent(binnum,i+1,plot.GetBinContent(binnum))

   ROOT.gStyle.SetOptStat(0)
   runtime_vs_job.Draw('colz')
   can.SaveAs('runtime_vs_job.png')


def parse_datetime(line):
   date_string = line.split('|')[0].split('.')[0]
   date = datetime.datetime.strptime(date_string,'%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=7)
   return date
            
def timedelta_total_seconds(timedelta):
    return (timedelta.microseconds + 0.0 + (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6

if __name__ == "__main__":
   main()
