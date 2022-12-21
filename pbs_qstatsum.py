import subprocess as sp
import pandas as pd
import json
import argparse

def get_integers(stringtime):
   hr,min,sec = stringtime.split(':')
   hr,min,sec = int(hr),int(min),int(sec)
   return hr,min,sec

def string_time_to_seconds(stringtime):
   hr,min,sec = get_integers(stringtime)
   return hr * 60 * 60 + min * 60 + sec


def string_time_to_minutes(stringtime):
   hr,min,sec = get_integers(stringtime)
   return hr * 60 + min

def string_time_to_hours(stringtime):
   hr,min,sec = get_integers(stringtime)
   return float(hr) + float(min)/60. + float(sec/60./60.)

def walltime_to_hours(walltime):
   return string_time_to_hours(walltime)

def job_sort_formula(jobdata):
   rl = jobdata['Resource_List']
   base_score        = float(rl['base_score'])
   score_boost       = float(rl['score_boost'])
   enable_wfp        = float(rl['enable_wfp'])
   wfp_factor        = float(rl['wfp_factor'])
   eligible_time     = float(string_time_to_seconds(jobdata['eligible_time']))
   walltime          = float(string_time_to_seconds(rl['walltime']))
   project_priority  = float(rl['project_priority'])
   nodect            = float(rl['nodect'])
   total_cpus        = float(rl['total_cpus'])
   enable_backfill   = float(rl['enable_backfill'])
   backfill_max      = float(rl['backfill_max'])
   backfill_factor   = float(rl['backfill_factor'])
   enable_fifo       = float(rl['enable_fifo'])
   fifo_factor       = float(rl['fifo_factor'])
   return ( base_score +
            score_boost + 
            (enable_wfp * wfp_factor * (eligible_time ** 2 / min(max(walltime,21600.0),43200.0) ** 3  * project_priority * nodect / total_cpus)) + 
            (enable_backfill * min(backfill_max, eligible_time / backfill_factor)) + 
            (enable_fifo * eligible_time / fifo_factor) )


def main():
   
   parser = argparse.ArgumentParser(description='extract monitoring information from qstat')
   parser.add_argument('-r','--running',help='summarize running jobs',action='store_true')
   parser.add_argument('-q','--queued',help='summarize queued jobs',action='store_true')
   parser.add_argument('-s','--score',help='summarize top jobs in each queue by score',action='store_true')
   parser.add_argument('-c','--countdown',help='list running jobs with smallest walltime remaining',action='store_true')

   parser.add_argument('-t','--topn',help='how many jobs to print',default=5,type=int)

   args = parser.parse_args()

   # run qstat to get json output
   completed_process = sp.run('/opt/pbs/bin/qstat -f -F json'.split(),capture_output=True)

   assert completed_process.returncode == 0

   qdata = json.loads(completed_process.stdout)

   running_by_queue = {}
   queued_by_queue = {}

   running_jobs = 0
   running_nodes = 0
   running_debug = 0
   total_nodes = 560
   total_prod_nodes = 496

   queued_jobs = 0
   queued_nodes = 0
   queued_debug = 0

   queue_node_breakdown = {}

   job_data = []

   # check running jobs
   for jobname,jobdata in qdata['Jobs'].items():
      jobid = int(jobname.split('.')[0].replace('[]',''))
      state = jobdata['job_state']
      qname = jobdata['queue']
      nnodes = jobdata['Resource_List']['nodect']
      project = jobdata['project']
      walltime = walltime_to_hours(jobdata['Resource_List']['walltime'])
      nodehours = nnodes * walltime
      username = jobdata['Variable_List']['PBS_O_LOGNAME']

      try:
         remaining_runtime_min = string_time_to_minutes(jobdata['Resource_List']['walltime']) - string_time_to_minutes(jobdata['resources_used']['walltime'])
      except:
         remaining_runtime_min = string_time_to_minutes(jobdata['Resource_List']['walltime'])

      job_data.append({
         'id':jobid,
         'state':state,
         'queue':qname,
         'nodes':int(nnodes),
         'project':project,
         'wallhours':int(walltime),
         'nodehours':int(nodehours),
         'username':username,
         'score':job_sort_formula(jobdata),
         'runtime_left':remaining_runtime_min
      })

   df = pd.DataFrame(job_data)

   if(args.running):
      print('\nRunning:')
      dfr = df[df['state']=='R'][['queue','nodes','nodehours']]
      print(dfr.groupby(['queue']).sum())
   if(args.queued):
      print('\nQueued:')
      dfq = df[df['state']=='Q'][['queue','nodes','nodehours']]
      print(dfq.groupby(['queue']).sum())

   if(args.score):
      topn=args.topn
      print('\nTop %d Queued Large Jobs:' % topn)
      print(df[(df['state']=='Q') & (df['queue'] == 'large')].nlargest(topn,'score').to_string(index=False))
      print('\nTop %d Queued Medium Jobs:' % topn)
      print(df[(df['state']=='Q') & (df['queue'] == 'medium')].nlargest(topn,'score').to_string(index=False))
      print('\nTop %d Queued Small Jobs:' % topn)
      print(df[(df['state']=='Q') & (df['queue'] == 'small')].nlargest(topn,'score').to_string(index=False))

   if(args.countdown):
      print('\n Jobs about to Finish:')
      print(df[df['state']=='R'].nsmallest(args.topn,'runtime_left').to_string(index=False))


if __name__ == "__main__":
   main()