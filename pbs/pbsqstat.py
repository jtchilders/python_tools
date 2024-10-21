import subprocess as sp
import pandas as pd
import dateutil,datetime
import json
import logging
from tabulate import tabulate
from .pbs_states import get_full_state_name,get_state_code
logger = logging.getLogger(__name__)


# This command returns JSON formated as follows:
# {
#     "timestamp":1699637534,
#     "pbs_version":"2022.1.1.20220926110806",
#     "pbs_server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#     "Queue":{
#         "run_next":{
#             "queue_type":"Execution",
#             "Priority":100,
#             "total_jobs":0,
#             "state_count":"Transit:0 Queued:0 Held:0 Waiting:0 Running:0 Exiting:0 Begun:0 ",
#             "acl_user_enable":"True",
#             "acl_users":"+jenkinsat@*",
#             "enabled":"False",
#             "started":"False"
#         },
#         "debug":{
#             "queue_type":"Execution",
#             "Priority":100,
#             "total_jobs":32,
#             "state_count":"Transit:0 Queued:17 Held:8 Waiting:0 Running:7 Exiting:0 Begun:0 ",
#             "resources_max":{
#                 "nodect":2,
#                 "walltime":"01:00:00"
#             },
#             "resources_min":{
#                 "nodect":1,
#                 "walltime":"00:05:00"
#             },
#             "resources_default":{
#                 "base_score":51,
#                 "enable_fifo":1,
#                 "mig_avail":"True",
#                 "start_xserver":"False"
#             },
#             "default_chunk":{
#                 "build":"False",
#                 "demand":"False",
#                 "ss11":"False",
#                 "system":"polaris"
#             },
#             "resources_assigned":{
#                 "mem":"0gb",
#                 "mpiprocs":0,
#                 "ncpus":448,
#                 "nodect":7
#             },
#             "max_run":"[u:PBS_GENERIC=1]",
#             "max_run_res":{
#                 "nodect":"[o:PBS_ALL=8]"
#             },
#             "enabled":"True",
#             "started":"True",
#             "queued_jobs_threshold":"[u:PBS_GENERIC=1]"
#         },
#         "prod":{
#             "queue_type":"Route",
#             "total_jobs":1,
#             "state_count":"Transit:0 Queued:0 Held:1 Waiting:0 Running:0 Exiting:0 Begun:0 ",
#             "max_queued":"[p:PBS_GENERIC=100]",
#             "resources_max":{
#                 "nodect":496,
#                 "walltime":"24:00:00"
#             },
#             "resources_min":{
#                 "nodect":10,
#                 "walltime":"00:05:00"
#             },
#             "route_destinations":"small,medium,large,backfill-small,backfill-medium,backfill-large",
#             "enabled":"True",
#             "started":"True"
#         },
#         "small":{
#             "queue_type":"Execution",
#             "Priority":150,
#             "total_jobs":18,
#             "state_count":"Transit:0 Queued:8 Held:10 Waiting:0 Running:0 Exiting:0 Begun:0 ",
#             "max_queued":"[p:PBS_GENERIC=10]",
#             "from_route_only":"True",
#             "resources_max":{
#                 "nodect":24,
#                 "overburn":"False",
#                 "route_backfill":"False",
#                 "walltime":"03:00:00"
#             },
#             "resources_min":{
#                 "nodect":10,
#                 "overburn":"False",
#                 "route_backfill":"False",
#                 "walltime":"00:05:00"
#             },
#             "resources_default":{
#                 "base_score":51,
#                 "enable_wfp":1
#             },
#             "default_chunk":{
#                 "build":"False",
#                 "debug":"False",
#                 "demand":"False",
#                 "ss11":"False",
#                 "system":"polaris"
#             },
#             "resources_assigned":{
#                 "mpiprocs":0,
#                 "ncpus":0,
#                 "nodect":0
#             },
#             "enabled":"True",
#             "started":"True"
#         },
#     }
# }

def qstat_queues(exec: str = '/opt/pbs/bin/qstat',
                 args: list = ['-Q','-f','-F','json']) -> dict:
   cmd = exec + ' ' + ' '.join(args)
   completed_process = sp.run(cmd.split(' '),stdout=sp.PIPE,stderr=sp.PIPE)
   if completed_process.returncode != 0:
      raise Exception(completed_process.stderr.decode('utf-8'))
   return json.loads(completed_process.stdout.decode('utf-8'))

def get_queued_jobs_states(queue_data):
   queues = queue_data['Queue']
   output = {}
   for queue in queues:
      output[queue] = {}
      if 'state_count' in queues[queue]:
         for state_count in queues[queue]['state_count'].split():
            state,state_count = state_count.split(':')
            state_count = int(state_count)
            output[queue][state] = state_count
   
   return output


def get_job_node_hours(job):
   walltime = job['Resource_List'].get('walltime', '00:00:00')
   nodes = job['Resource_List'].get('nodect', 0)
   return int(string_time_to_hours(walltime) * nodes)

# a function that takes the job data dictionary and returns the total number of 
# node-hours in each queue and state
def get_node_hours(jobs_data):
   jobs = jobs_data['Jobs']
   output = {} # will be {'queue_name': {'stateA': hours, 'stateB': hours}}
   
   for job_id,job_data in jobs.items():
      job_state = job_data['job_state']
      job_node_hours = get_job_node_hours(job_data)
      job_queue = job_data['queue']

      if job_queue not in output:
         output[job_queue] = {}
      if job_state not in output[job_queue]:
         output[job_queue][job_state] = 0
      output[job_queue][job_state] += job_node_hours

   return output


def print_queued_jobs_states(job_data: dict, summarize: bool = False):
   job_df = convert_jobs_to_dataframe(job_data, qstat_server())

   if summarize:
      job_df = job_df[job_df['state'].isin(['Q', 'R'])]  # Filter only Running and Queued states
      job_df['state'] = job_df['state'].replace({'Q': 'Queued', 'R': 'Running'})
      summary_df = job_df.groupby(['queue', 'state']).agg({'jobid': 'count', 'node_hours': 'sum', 'nodes': 'sum'}).unstack(fill_value=0)
      
      summary_df.columns = ['Queued Count', 'Running Count', 'Queued Node Hours',  'Running Node Hours',  'Queued Nodes',  'Running Nodes']
   else:
      summary_df = job_df.pivot_table(index='queue', columns='state', values='jobid', aggfunc='count', fill_value=0).join(
         job_df.pivot_table(index='queue', columns='state', values='node_hours', aggfunc='sum', fill_value=0),
         rsuffix=' Node Hours'
      )

   summary_df = summary_df.reset_index()
   summary_df.columns.name = None  # Remove the column index name

   # Calculate totals
   totals = summary_df.sum(numeric_only=True)
   totals['queue'] = 'Totals'

   # Append totals row to the DataFrame
   summary_df = pd.concat([summary_df, totals.to_frame().T], ignore_index=True)

   table = tabulate(summary_df.to_records(index=False), headers='keys', tablefmt='pretty',colalign=['left'] + ['right'] * (summary_df.shape[1] - 1))
   logger.info("\n" + table)
   # print(table)
   

def print_queued_jobs_states2(queue_data: dict, job_data: dict, summarize: bool = False):
   # for the various queues, get a count of jobs in each state on each queue
   #  in the format {"queue_name": {"state": count}}
   data = get_queued_jobs_states(queue_data)

   # get the total number of node-hours in each queue
   # in the format {"queue_name": {"Q": hours, "H": hours, ...}}
   node_hours = get_node_hours(job_data)
   print(node_hours)

   # if the summarize flag was set, condense the presentation to a limited list of queues and states
   # combininig related queues and omitting under utilized states
   if summarize:
      # only count these states
      states = {
         'Queued',
         'Running',
      }
      # combined queues into these summarized queues
      combined_queues = {
         'backfill': ['backfill-small', 'backfill-medium', 'backfill-large'],
         'preemptable': ['preemptable'],
         'debug': ['debug','debug-scaling'],
         'small': ['small'],
         'medium': ['medium'],
         'large': ['large'],
      }
      # this will be the new data list with format {"queue_name": {"state": {"count": count, "node_hours": hours}}}
      new_data = {}
      # loop over combined queue names and count the number of jobs in each state for the combined queues
      for new_queue,old_queues in combined_queues.items():
         new_data[new_queue] = {}
         for old_queue in old_queues:
            for state_full_name,count in data[old_queue].items():
               print(state_full_name,count)
               state_code = get_state_code(state_full_name) # conversion like 'Running' -> 'R'
               if state_full_name in states:
                  if state_full_name not in new_data[new_queue]:
                     new_data[new_queue][state_full_name] = {'count': 0, 'node_hours': 0}
                  if state_full_name in new_data[new_queue]:
                     new_data[new_queue][state_full_name]['count'] += count
                     if old_queue in node_hours and state_code in node_hours[old_queue]:
                        new_data[new_queue][state_full_name]['node_hours'] += node_hours[old_queue][state_code]
                  else:
                     new_data[new_queue][state_full_name]['count'] = count
                     if old_queue in node_hours and state_code in node_hours[old_queue]:
                        new_data[new_queue][state_full_name]['node_hours'] = node_hours[old_queue][state_code]
      
      data = new_data


   else:
      # otherwise, print the full list of queues and states
      states = [
         'Transit',
         'Queued',
         'Held',
         'Waiting',
         'Running',
         'Exiting',
         'Begun',
      ]
   
   column_names = f"{'Queue Name':<20}: "
   lines = f"{'-'*20:<20}: "
   for state_name in states:
      column_names += f"{state_name:>10} "
      lines += f"{'-'*10:>10} "
   logger.info(column_names)
   logger.info(lines)
   
   state_totals = { state:{'count': 0, 'node_hours': 0} for state in states }
   for queue in data:
      row_str = f"{queue:<20}: "
      for state,counts in data[queue].items():
         row_str += f"{counts['count']:10d} "
         if state in states:
            state_totals[state]['count'] += counts['count']
            state_totals[state]['node_hours'] += counts['node_hours']
         row_str += f"{counts['node_hours']:10d} "

      logger.info(row_str)
   

   logger.info(lines)
   totals = f"{'Totals':<20}: "
   for state,counts in state_totals.items():
      totals += f"{counts['count']:10d} {counts['node_hours']:10d} "
   logger.info(totals)

# this command returns JSON formated as follows:
# {
#     "timestamp":1699637718,
#     "pbs_version":"2022.1.1.20220926110806",
#     "pbs_server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#     "Jobs":{
#         "359322.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov":{
#             "Job_Name":"STDIN",
#             "Job_Owner":"christopherkang@polaris-login-02.hsn.cm.polaris.alcf.anl.gov",
#             "job_state":"Q",
#             "queue":"debug",
#             "server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "Account_Name":"transformer_eval",
#             "Checkpoint":"u",
#             "ctime":"Sat Dec  3 20:25:38 2022",
#             "Error_Path":"polaris-login-02.hsn.cm.polaris.alcf.anl.gov:/lus/grand/projects/transformer_eval/ctkang/gpt-neox/configs/STDIN.e359322",
#             "Hold_Types":"n",
#             "interactive":"True",
#             "Join_Path":"n",
#             "Keep_Files":"doe",
#             "Mail_Points":"a",
#             "mtime":"Fri Nov 10 11:31:19 2023",
#             "Output_Path":"polaris-login-02.hsn.cm.polaris.alcf.anl.gov:/lus/grand/projects/transformer_eval/ctkang/gpt-neox/configs/STDIN.o359322",
#             "Priority":0,
#             "qtime":"Sat Dec  3 20:25:38 2022",
#             "Rerunable":"False",
#             "Resource_List":{
#                 "allow_account_check_failure":"True",
#                 "allow_negative_allocation":"True",
#                 "award_category":"Discretionary",
#                 "award_type":"Discretionary",
#                 "backfill_factor":84600,
#                 "backfill_max":50,
#                 "base_score":51,
#                 "burn_ratio":0.0615,
#                 "current_allocation":378419168,
#                 "eagle_fs":"True",
#                 "enable_backfill":0,
#                 "enable_fifo":1,
#                 "enable_wfp":0,
#                 "fifo_factor":1800,
#                 "filesystems":"home:eagle",
#                 "home_fs":"True",
#                 "mig_avail":"True",
#                 "ncpus":64,
#                 "ngpus":8,
#                 "ni_resource":"polaris",
#                 "nodect":1,
#                 "overburn":"False",
#                 "place":"free",
#                 "preempt_targets":"NONE",
#                 "project_priority":2,
#                 "route_backfill":"False",
#                 "score_boost":0,
#                 "select":"1:ngpus=8",
#                 "start_xserver":"False",
#                 "total_allocation":403200000,
#                 "total_cpus":560,
#                 "walltime":"01:00:00",
#                 "wfp_factor":100000
#             },
#             "substate":10,
#             "Variable_List":{
#                 "PBS_O_HOME":"/home/christopherkang",
#                 "PBS_O_LANG":"en_US.UTF-8",
#                 "PBS_O_LOGNAME":"christopherkang",
#                 "PBS_O_PATH":"/soft/datascience/venvs/polaris/2022-09-08/bin:/home/christopherkang/.conda/envs/llm/bin:/soft/datascience/conda/2022-09-08/mconda3/condabin:/soft/compilers/cudatoolkit/cuda-11.6.2/bin:/soft/libraries/nccl/nccl_2.14.3-1+cuda11.6_x86_64/include:/opt/cray/pe/pals/1.1.7/bin:/opt/cray/pe/craype/2.7.15/bin:/opt/cray/pe/gcc/11.2.0/bin:/opt/cray/pe/perftools/22.05.0/bin:/opt/cray/pe/papi/6.0.0.14/bin:/opt/cray/libfabric/1.11.0.4.125/bin:/opt/clmgr/sbin:/opt/clmgr/bin:/opt/sgi/sbin:/opt/sgi/bin:/usr/local/bin:/usr/bin:/bin:/opt/c3/bin:/dbhome/db2cat/sqllib/bin:/dbhome/db2cat/sqllib/adm:/dbhome/db2cat/sqllib/misc:/dbhome/db2cat/sqllib/gskit/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:/opt/pbs/bin:/sbin:/opt/cray/pe/bin",
#                 "PBS_O_MAIL":"/var/spool/mail/christopherkang",
#                 "PBS_O_SHELL":"/bin/bash",
#                 "PBS_O_HOST":"polaris-login-02.hsn.cm.polaris.alcf.anl.gov",
#                 "PBS_O_WORKDIR":"/lus/grand/projects/transformer_eval/ctkang/gpt-neox/configs",
#                 "PBS_O_SYSTEM":"Linux",
#                 "PBS_O_QUEUE":"debug"
#             },
#             "comment":"Not Running: Job is requesting an exclusive node and node is in use",
#             "etime":"Sat Dec  3 20:25:38 2022",
#             "umask":22,
#             "eligible_time":"8199:09:40",
#             "Submit_arguments":"-A transformer_eval -l filesystems=home:eagle -l walltime=1:0:0 -l select=1:ngpus=8 -q debug -I",
#             "project":"transformer_eval",
#             "Submit_Host":"polaris-login-02.hsn.cm.polaris.alcf.anl.gov"
#         },
#         "412020.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov":{
#             "Job_Name":"pyannote",
#             "Job_Owner":"nkvatsa@polaris-login-01.hsn.cm.polaris.alcf.anl.gov",
#             "job_state":"H",
#             "queue":"preemptable",
#             "server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "Account_Name":"BPC",
#             "Checkpoint":"u",
#             "ctime":"Fri Feb 10 16:47:11 2023",
#             "Error_Path":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov:/home/shiyanglai/system_log/std_err/",
#             "Hold_Types":"s",
#             "Join_Path":"n",
#             "Keep_Files":"doe",
#             "Mail_Points":"a",
#             "Mail_Users":"nkvatsa@uchicago.edu",
#             "mtime":"Fri Feb 10 17:02:23 2023",
#             "Output_Path":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov:/home/shiyanglai/system_log/std_out/",
#             "Priority":0,
#             "qtime":"Fri Feb 10 16:47:11 2023",
#             "Rerunable":"False",
#             "Resource_List":{
#                 "allow_account_check_failure":"True",
#                 "allow_negative_allocation":"True",
#                 "award_category":"Discretionary",
#                 "award_type":"Discretionary",
#                 "backfill_factor":84600,
#                 "backfill_max":50,
#                 "base_score":0,
#                 "burn_ratio":0.0129,
#                 "current_allocation":398011584,
#                 "enable_backfill":0,
#                 "enable_fifo":1,
#                 "enable_wfp":0,
#                 "fifo_factor":1800,
#                 "filesystems":"home:grand",
#                 "grand_fs":"True",
#                 "home_fs":"True",
#                 "mig_avail":"True",
#                 "ncpus":64,
#                 "ni_resource":"polaris",
#                 "nodect":1,
#                 "overburn":"False",
#                 "place":"scatter",
#                 "preempt_targets":"NONE",
#                 "project_priority":2,
#                 "route_backfill":"False",
#                 "score_boost":0,
#                 "select":"1:system=polaris",
#                 "total_allocation":403200000,
#                 "total_cpus":560,
#                 "walltime":"08:00:00",
#                 "wfp_factor":100000
#             },
#             "stime":"Fri Feb 10 17:02:23 2023",
#             "obittime":"Fri Feb 10 17:02:59 2023",
#             "substate":20,
#             "Variable_List":{
#                 "PBS_O_HOME":"/home/nkvatsa",
#                 "PBS_O_LANG":"en_US.UTF-8",
#                 "PBS_O_LOGNAME":"nkvatsa",
#                 "PBS_O_PATH":"/home/nkvatsa/.conda/envs/pyannote/bin:/soft/datascience/conda/2022-09-08/mconda3/condabin:/soft/compilers/cudatoolkit/cuda-11.6.2/bin:/soft/libraries/nccl/nccl_2.14.3-1+cuda11.6_x86_64/include:/opt/cray/pe/pals/1.1.7/bin:/opt/cray/pe/craype/2.7.15/bin:/opt/cray/pe/gcc/11.2.0/bin:/home/nkvatsa/.vscode-server/bin/e2816fe719a4026ffa1ee0189dc89bdfdbafb164/bin/remote-cli:/opt/cray/pe/perftools/22.05.0/bin:/opt/cray/pe/papi/6.0.0.14/bin:/opt/cray/libfabric/1.11.0.4.125/bin:/opt/clmgr/sbin:/opt/clmgr/bin:/opt/sgi/sbin:/opt/sgi/bin:/usr/local/bin:/usr/bin:/bin:/opt/c3/bin:/dbhome/db2cat/sqllib/bin:/dbhome/db2cat/sqllib/adm:/dbhome/db2cat/sqllib/misc:/dbhome/db2cat/sqllib/gskit/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:/opt/pbs/bin:/sbin:/opt/cray/pe/bin",
#                 "PBS_O_MAIL":"/var/spool/mail/nkvatsa",
#                 "PBS_O_SHELL":"/bin/bash",
#                 "PBS_O_WORKDIR":"/lus/grand/projects/BPC/ra/nvatsa/pipelines",
#                 "PBS_O_SYSTEM":"Linux",
#                 "PBS_O_QUEUE":"preemptable",
#                 "PBS_O_HOST":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
#             },
#             "comment":"job held, too many failed attempts to run",
#             "etime":"Fri Feb 10 16:47:11 2023",
#             "umask":22,
#             "run_count":21,
#             "eligible_time":"6546:33:12",
#             "Exit_status":-3,
#             "Submit_arguments":"run_polaris.sh",
#             "project":"BPC",
#             "Submit_Host":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
#         },
#         "1147579.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov":{
#             "Job_Name":"xgc1_core",
#             "Job_Owner":"jdominsk@polaris-login-01.hsn.cm.polaris.alcf.anl.gov",
#             "resources_used":{
#                 "cpupercent":465226,
#                 "cput":"16648:25:43",
#                 "mem":"28999922880kb",
#                 "ncpus":160,
#                 "vmem":"370249416kb",
#                 "walltime":"22:28:30"
#             },
#             "job_state":"R",
#             "queue":"large",
#             "server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "Account_Name":"TokamakITER",
#             "Checkpoint":"u",
#             "ctime":"Tue Nov  7 18:12:33 2023",
#             "Error_Path":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov:/eagle/TokamakITER/jdominsk/west55799_xgc1/run1_core_eDW2/./xgclog.stderr",
#             "exec_host":"x3006c0s31b1n0/0+x3006c0s37b0n0/0+x3006c0s37b1n0/0+x3006c0s7b0n0/0+x3006c0s7b1n0/0+x3007c0s13b0n0/0+x3007c0s13b1n0/0+x3007c0s19b0n0/0+x3007c0s19b1n0/0+x3007c0s1b0n0/0+x3007c0s1b1n0/0+x3007c0s25b0n0/0+x3007c0s25b1n0/0+x3007c0s31b0n0/0+x3007c0s31b1n0/0+x3007c0s37b0n0/0+x3007c0s37b1n0/0+x3007c0s7b0n0/0+x3007c0s7b1n0/0+x3008c0s13b0n0/0+x3008c0s13b1n0/0+x3008c0s19b0n0/0+x3008c0s19b1n0/0+x3008c0s1b0n0/0+x3008c0s1b1n0/0+x3008c0s25b0n0/0+x3008c0s25b1n0/0+x3008c0s31b0n0/0+x3008c0s31b1n0/0+x3008c0s37b0n0/0+x3008c0s37b1n0/0+x3008c0s7b0n0/0+x3008c0s7b1n0/0+x3109c0s13b0n0/0+x3109c0s13b1n0/0+x3109c0s19b0n0/0+x3109c0s19b1n0/0+x3109c0s1b0n0/0+x3109c0s1b1n0/0+x3109c0s25b0n0/0+x3109c0s25b1n0/0+x3109c0s31b0n0/0+x3109c0s31b1n0/0+x3109c0s37b0n0/0+x3109c0s37b1n0/0+x3109c0s7b0n0/0+x3109c0s7b1n0/0+x3110c0s13b0n0/0+x3110c0s13b1n0/0+x3110c0s19b0n0/0+x3110c0s19b1n0/0+x3110c0s1b0n0/0+x3110c0s1b1n0/0+x3110c0s25b0n0/0+x3110c0s25b1n0/0+x3110c0s31b0n0/0+x3110c0s31b1n0/0+x3110c0s37b0n0/0+x3110c0s37b1n0/0+x3110c0s7b0n0/0+x3110c0s7b1n0/0+x3111c0s13b0n0/0+x3111c0s13b1n0/0+x3111c0s19b0n0/0+x3111c0s19b1n0/0+x3111c0s1b0n0/0+x3111c0s1b1n0/0+x3111c0s25b0n0/0+x3111c0s25b1n0/0+x3111c0s31b0n0/0+x3111c0s31b1n0/0+x3111c0s37b0n0/0+x3111c0s37b1n0/0+x3111c0s7b0n0/0+x3111c0s7b1n0/0+x3112c0s13b0n0/0+x3112c0s13b1n0/0+x3112c0s19b0n0/0+x3112c0s19b1n0/0+x3112c0s1b1n0/0+x3112c0s25b0n0/0+x3112c0s25b1n0/0+x3112c0s31b0n0/0+x3112c0s31b1n0/0+x3112c0s37b0n0/0+x3112c0s37b1n0/0+x3112c0s7b0n0/0+x3112c0s7b1n0/0+x3201c0s13b0n0/0+x3201c0s13b1n0/0+x3201c0s19b0n0/0+x3201c0s19b1n0/0+x3201c0s1b0n0/0+x3201c0s1b1n0/0+x3201c0s25b0n0/0+x3201c0s25b1n0/0+x3201c0s31b0n0/0+x3201c0s31b1n0/0+x3201c0s37b0n0/0+x3201c0s37b1n0/0+x3201c0s7b0n0/0+x3201c0s7b1n0/0+x3202c0s13b0n0/0+x3202c0s13b1n0/0+x3202c0s19b0n0/0+x3202c0s19b1n0/0+x3202c0s1b0n0/0+x3202c0s1b1n0/0+x3202c0s25b0n0/0+x3202c0s25b1n0/0+x3202c0s31b0n0/0+x3202c0s31b1n0/0+x3202c0s37b1n0/0+x3202c0s7b0n0/0+x3202c0s7b1n0/0+x3203c0s13b0n0/0+x3203c0s13b1n0/0+x3203c0s19b0n0/0+x3203c0s19b1n0/0+x3203c0s1b0n0/0+x3203c0s1b1n0/0+x3203c0s25b0n0/0+x3203c0s25b1n0/0+x3203c0s31b0n0/0+x3203c0s31b1n0/0+x3203c0s37b0n0/0+x3203c0s37b1n0/0+x3203c0s7b0n0/0+x3203c0s7b1n0/0+x3204c0s13b0n0/0+x3204c0s13b1n0/0+x3204c0s19b0n0/0+x3204c0s19b1n0/0+x3204c0s1b0n0/0+x3204c0s1b1n0/0+x3204c0s25b0n0/0+x3204c0s25b1n0/0+x3204c0s31b0n0/0+x3204c0s31b1n0/0+x3204c0s37b0n0/0+x3204c0s37b1n0/0+x3204c0s7b0n0/0+x3204c0s7b1n0/0+x3001c0s13b0n0/0+x3001c0s13b1n0/0+x3001c0s19b1n0/0+x3001c0s1b0n0/0+x3001c0s1b1n0/0+x3001c0s25b0n0/0+x3001c0s25b1n0/0+x3001c0s31b0n0/0+x3001c0s31b1n0/0+x3001c0s37b0n0/0+x3001c0s37b1n0/0+x3001c0s7b0n0/0+x3001c0s7b1n0/0+x3002c0s13b0n0/0+x3002c0s13b1n0/0+x3002c0s19b0n0/0+x3002c0s19b1n0/0",
#             "exec_vnode":"(x3006c0s31b1n0:ncpus=1)+(x3006c0s37b0n0:ncpus=1)+(x3006c0s37b1n0:ncpus=1)+(x3006c0s7b0n0:ncpus=1)+(x3006c0s7b1n0:ncpus=1)+(x3007c0s13b0n0:ncpus=1)+(x3007c0s13b1n0:ncpus=1)+(x3007c0s19b0n0:ncpus=1)+(x3007c0s19b1n0:ncpus=1)+(x3007c0s1b0n0:ncpus=1)+(x3007c0s1b1n0:ncpus=1)+(x3007c0s25b0n0:ncpus=1)+(x3007c0s25b1n0:ncpus=1)+(x3007c0s31b0n0:ncpus=1)+(x3007c0s31b1n0:ncpus=1)+(x3007c0s37b0n0:ncpus=1)+(x3007c0s37b1n0:ncpus=1)+(x3007c0s7b0n0:ncpus=1)+(x3007c0s7b1n0:ncpus=1)+(x3008c0s13b0n0:ncpus=1)+(x3008c0s13b1n0:ncpus=1)+(x3008c0s19b0n0:ncpus=1)+(x3008c0s19b1n0:ncpus=1)+(x3008c0s1b0n0:ncpus=1)+(x3008c0s1b1n0:ncpus=1)+(x3008c0s25b0n0:ncpus=1)+(x3008c0s25b1n0:ncpus=1)+(x3008c0s31b0n0:ncpus=1)+(x3008c0s31b1n0:ncpus=1)+(x3008c0s37b0n0:ncpus=1)+(x3008c0s37b1n0:ncpus=1)+(x3008c0s7b0n0:ncpus=1)+(x3008c0s7b1n0:ncpus=1)+(x3109c0s13b0n0:ncpus=1)+(x3109c0s13b1n0:ncpus=1)+(x3109c0s19b0n0:ncpus=1)+(x3109c0s19b1n0:ncpus=1)+(x3109c0s1b0n0:ncpus=1)+(x3109c0s1b1n0:ncpus=1)+(x3109c0s25b0n0:ncpus=1)+(x3109c0s25b1n0:ncpus=1)+(x3109c0s31b0n0:ncpus=1)+(x3109c0s31b1n0:ncpus=1)+(x3109c0s37b0n0:ncpus=1)+(x3109c0s37b1n0:ncpus=1)+(x3109c0s7b0n0:ncpus=1)+(x3109c0s7b1n0:ncpus=1)+(x3110c0s13b0n0:ncpus=1)+(x3110c0s13b1n0:ncpus=1)+(x3110c0s19b0n0:ncpus=1)+(x3110c0s19b1n0:ncpus=1)+(x3110c0s1b0n0:ncpus=1)+(x3110c0s1b1n0:ncpus=1)+(x3110c0s25b0n0:ncpus=1)+(x3110c0s25b1n0:ncpus=1)+(x3110c0s31b0n0:ncpus=1)+(x3110c0s31b1n0:ncpus=1)+(x3110c0s37b0n0:ncpus=1)+(x3110c0s37b1n0:ncpus=1)+(x3110c0s7b0n0:ncpus=1)+(x3110c0s7b1n0:ncpus=1)+(x3111c0s13b0n0:ncpus=1)+(x3111c0s13b1n0:ncpus=1)+(x3111c0s19b0n0:ncpus=1)+(x3111c0s19b1n0:ncpus=1)+(x3111c0s1b0n0:ncpus=1)+(x3111c0s1b1n0:ncpus=1)+(x3111c0s25b0n0:ncpus=1)+(x3111c0s25b1n0:ncpus=1)+(x3111c0s31b0n0:ncpus=1)+(x3111c0s31b1n0:ncpus=1)+(x3111c0s37b0n0:ncpus=1)+(x3111c0s37b1n0:ncpus=1)+(x3111c0s7b0n0:ncpus=1)+(x3111c0s7b1n0:ncpus=1)+(x3112c0s13b0n0:ncpus=1)+(x3112c0s13b1n0:ncpus=1)+(x3112c0s19b0n0:ncpus=1)+(x3112c0s19b1n0:ncpus=1)+(x3112c0s1b1n0:ncpus=1)+(x3112c0s25b0n0:ncpus=1)+(x3112c0s25b1n0:ncpus=1)+(x3112c0s31b0n0:ncpus=1)+(x3112c0s31b1n0:ncpus=1)+(x3112c0s37b0n0:ncpus=1)+(x3112c0s37b1n0:ncpus=1)+(x3112c0s7b0n0:ncpus=1)+(x3112c0s7b1n0:ncpus=1)+(x3201c0s13b0n0:ncpus=1)+(x3201c0s13b1n0:ncpus=1)+(x3201c0s19b0n0:ncpus=1)+(x3201c0s19b1n0:ncpus=1)+(x3201c0s1b0n0:ncpus=1)+(x3201c0s1b1n0:ncpus=1)+(x3201c0s25b0n0:ncpus=1)+(x3201c0s25b1n0:ncpus=1)+(x3201c0s31b0n0:ncpus=1)+(x3201c0s31b1n0:ncpus=1)+(x3201c0s37b0n0:ncpus=1)+(x3201c0s37b1n0:ncpus=1)+(x3201c0s7b0n0:ncpus=1)+(x3201c0s7b1n0:ncpus=1)+(x3202c0s13b0n0:ncpus=1)+(x3202c0s13b1n0:ncpus=1)+(x3202c0s19b0n0:ncpus=1)+(x3202c0s19b1n0:ncpus=1)+(x3202c0s1b0n0:ncpus=1)+(x3202c0s1b1n0:ncpus=1)+(x3202c0s25b0n0:ncpus=1)+(x3202c0s25b1n0:ncpus=1)+(x3202c0s31b0n0:ncpus=1)+(x3202c0s31b1n0:ncpus=1)+(x3202c0s37b1n0:ncpus=1)+(x3202c0s7b0n0:ncpus=1)+(x3202c0s7b1n0:ncpus=1)+(x3203c0s13b0n0:ncpus=1)+(x3203c0s13b1n0:ncpus=1)+(x3203c0s19b0n0:ncpus=1)+(x3203c0s19b1n0:ncpus=1)+(x3203c0s1b0n0:ncpus=1)+(x3203c0s1b1n0:ncpus=1)+(x3203c0s25b0n0:ncpus=1)+(x3203c0s25b1n0:ncpus=1)+(x3203c0s31b0n0:ncpus=1)+(x3203c0s31b1n0:ncpus=1)+(x3203c0s37b0n0:ncpus=1)+(x3203c0s37b1n0:ncpus=1)+(x3203c0s7b0n0:ncpus=1)+(x3203c0s7b1n0:ncpus=1)+(x3204c0s13b0n0:ncpus=1)+(x3204c0s13b1n0:ncpus=1)+(x3204c0s19b0n0:ncpus=1)+(x3204c0s19b1n0:ncpus=1)+(x3204c0s1b0n0:ncpus=1)+(x3204c0s1b1n0:ncpus=1)+(x3204c0s25b0n0:ncpus=1)+(x3204c0s25b1n0:ncpus=1)+(x3204c0s31b0n0:ncpus=1)+(x3204c0s31b1n0:ncpus=1)+(x3204c0s37b0n0:ncpus=1)+(x3204c0s37b1n0:ncpus=1)+(x3204c0s7b0n0:ncpus=1)+(x3204c0s7b1n0:ncpus=1)+(x3001c0s13b0n0:ncpus=1)+(x3001c0s13b1n0:ncpus=1)+(x3001c0s19b1n0:ncpus=1)+(x3001c0s1b0n0:ncpus=1)+(x3001c0s1b1n0:ncpus=1)+(x3001c0s25b0n0:ncpus=1)+(x3001c0s25b1n0:ncpus=1)+(x3001c0s31b0n0:ncpus=1)+(x3001c0s31b1n0:ncpus=1)+(x3001c0s37b0n0:ncpus=1)+(x3001c0s37b1n0:ncpus=1)+(x3001c0s7b0n0:ncpus=1)+(x3001c0s7b1n0:ncpus=1)+(x3002c0s13b0n0:ncpus=1)+(x3002c0s13b1n0:ncpus=1)+(x3002c0s19b0n0:ncpus=1)+(x3002c0s19b1n0:ncpus=1)",
#             "Hold_Types":"n",
#             "Join_Path":"n",
#             "Keep_Files":"doe",
#             "Mail_Points":"a",
#             "Mail_Users":"jdominsk@pppl.gov",
#             "mtime":"Fri Nov 10 11:34:29 2023",
#             "Output_Path":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov:/eagle/TokamakITER/jdominsk/west55799_xgc1/run1_core_eDW2/./xgclog.stdout",
#             "Priority":0,
#             "qtime":"Tue Nov  7 18:12:33 2023",
#             "Rerunable":"False",
#             "Resource_List":{
#                 "allow_account_check_failure":"True",
#                 "allow_negative_allocation":"True",
#                 "award_category":"INCITE",
#                 "award_type":"INCITE-2023",
#                 "backfill_factor":84600,
#                 "backfill_max":50,
#                 "base_score":51,
#                 "burn_ratio":0.4707,
#                 "current_allocation":16005557248,
#                 "eagle_fs":"True",
#                 "enable_backfill":0,
#                 "enable_fifo":0,
#                 "enable_wfp":1,
#                 "fifo_factor":1800,
#                 "filesystems":"home:eagle",
#                 "home_fs":"True",
#                 "ncpus":160,
#                 "ni_resource":"polaris",
#                 "nodect":160,
#                 "nodes":160,
#                 "overburn":"False",
#                 "place":"scatter",
#                 "preempt_targets":"NONE",
#                 "project_priority":25,
#                 "route_backfill":"False",
#                 "score_boost":0,
#                 "select":"160:ncpus=1",
#                 "total_allocation":30240000000,
#                 "total_cpus":560,
#                 "walltime":"24:00:00",
#                 "wfp_factor":100000
#             },
#             "stime":"Thu Nov  9 13:05:50 2023",
#             "session_id":37300,
#             "jobdir":"/home/jdominsk",
#             "substate":42,
#             "Variable_List":{
#                 "PBS_O_HOME":"/home/jdominsk",
#                 "PBS_O_LANG":"en_US.UTF-8",
#                 "PBS_O_LOGNAME":"jdominsk",
#                 "PBS_O_PATH":"/opt/cray/pe/pals/1.1.7/bin:/opt/cray/pe/craype/2.7.15/bin:/opt/nvidia/hpc_sdk/Linux_x86_64/21.9/compilers/extras/qd/bin:/opt/nvidia/hpc_sdk/Linux_x86_64/21.9/compilers/bin:/opt/nvidia/hpc_sdk/Linux_x86_64/21.9/cuda/bin:/opt/cray/pe/perftools/22.05.0/bin:/opt/cray/pe/papi/6.0.0.14/bin:/opt/cray/libfabric/1.11.0.4.125/bin:/opt/clmgr/sbin:/opt/clmgr/bin:/opt/sgi/sbin:/opt/sgi/bin:/home/jdominsk/bin:/usr/local/bin:/usr/bin:/bin:/opt/c3/bin:/dbhome/db2cat/sqllib/bin:/dbhome/db2cat/sqllib/adm:/dbhome/db2cat/sqllib/misc:/dbhome/db2cat/sqllib/gskit/bin:/usr/lib/mit/bin:/usr/lib/mit/sbin:/opt/pbs/bin:/sbin:/opt/cray/pe/bin:/home/jdominsk/bin",
#                 "PBS_O_MAIL":"/var/spool/mail/jdominsk",
#                 "PBS_O_SHELL":"/bin/bash",
#                 "PBS_O_WORKDIR":"/eagle/TokamakITER/jdominsk/west55799_xgc1/run1_core_eDW2",
#                 "PBS_O_SYSTEM":"Linux",
#                 "PBS_O_QUEUE":"prod",
#                 "PBS_O_HOST":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
#             },
#             "comment":"Job run at Thu Nov 09 at 19:05 on (x3006c0s31b1n0:ncpus=1)+(x3006c0s37b0n0:ncpus=1)+(x3006c0s37b1n0:ncpus=1)+(x3006c0s7b0n0:ncpus=1)+(x3006c0s7b1n0:ncpus=1)+(x3007c0s13b0n0:ncpus=1)+(x3007c0s13b1n0:ncpus=1)+(x3007c0s19b0n0:ncpus=1)+(x3007c0s19b1n0:ncpu...",
#             "etime":"Tue Nov  7 18:12:33 2023",
#             "umask":22,
#             "run_count":1,
#             "eligible_time":"42:53:26",
#             "Submit_arguments":"job_launch.sh",
#             "estimated":{
#                 "exec_vnode":"(x3006c0s31b1n0:ncpus=1)+(x3006c0s37b0n0:ncpus=1)+(x3006c0s37b1n0:ncpus=1)+(x3006c0s7b0n0:ncpus=1)+(x3006c0s7b1n0:ncpus=1)+(x3007c0s13b0n0:ncpus=1)+(x3007c0s13b1n0:ncpus=1)+(x3007c0s19b0n0:ncpus=1)+(x3007c0s19b1n0:ncpus=1)+(x3007c0s1b0n0:ncpus=1)+(x3007c0s1b1n0:ncpus=1)+(x3007c0s25b0n0:ncpus=1)+(x3007c0s25b1n0:ncpus=1)+(x3007c0s31b0n0:ncpus=1)+(x3007c0s31b1n0:ncpus=1)+(x3007c0s37b0n0:ncpus=1)+(x3007c0s37b1n0:ncpus=1)+(x3007c0s7b0n0:ncpus=1)+(x3007c0s7b1n0:ncpus=1)+(x3008c0s13b0n0:ncpus=1)+(x3008c0s13b1n0:ncpus=1)+(x3008c0s19b0n0:ncpus=1)+(x3008c0s19b1n0:ncpus=1)+(x3008c0s1b0n0:ncpus=1)+(x3008c0s1b1n0:ncpus=1)+(x3008c0s25b0n0:ncpus=1)+(x3008c0s25b1n0:ncpus=1)+(x3008c0s31b0n0:ncpus=1)+(x3008c0s31b1n0:ncpus=1)+(x3008c0s37b0n0:ncpus=1)+(x3008c0s37b1n0:ncpus=1)+(x3008c0s7b0n0:ncpus=1)+(x3008c0s7b1n0:ncpus=1)+(x3109c0s13b0n0:ncpus=1)+(x3109c0s13b1n0:ncpus=1)+(x3109c0s19b0n0:ncpus=1)+(x3109c0s19b1n0:ncpus=1)+(x3109c0s1b0n0:ncpus=1)+(x3109c0s1b1n0:ncpus=1)+(x3109c0s25b0n0:ncpus=1)+(x3109c0s25b1n0:ncpus=1)+(x3109c0s31b0n0:ncpus=1)+(x3109c0s31b1n0:ncpus=1)+(x3109c0s37b0n0:ncpus=1)+(x3109c0s37b1n0:ncpus=1)+(x3109c0s7b0n0:ncpus=1)+(x3109c0s7b1n0:ncpus=1)+(x3110c0s13b0n0:ncpus=1)+(x3110c0s13b1n0:ncpus=1)+(x3110c0s19b0n0:ncpus=1)+(x3110c0s19b1n0:ncpus=1)+(x3110c0s1b0n0:ncpus=1)+(x3110c0s1b1n0:ncpus=1)+(x3110c0s25b0n0:ncpus=1)+(x3110c0s25b1n0:ncpus=1)+(x3110c0s31b0n0:ncpus=1)+(x3110c0s31b1n0:ncpus=1)+(x3110c0s37b0n0:ncpus=1)+(x3110c0s37b1n0:ncpus=1)+(x3110c0s7b0n0:ncpus=1)+(x3110c0s7b1n0:ncpus=1)+(x3111c0s13b0n0:ncpus=1)+(x3111c0s13b1n0:ncpus=1)+(x3111c0s19b0n0:ncpus=1)+(x3111c0s19b1n0:ncpus=1)+(x3111c0s1b0n0:ncpus=1)+(x3111c0s1b1n0:ncpus=1)+(x3111c0s25b0n0:ncpus=1)+(x3111c0s25b1n0:ncpus=1)+(x3111c0s31b0n0:ncpus=1)+(x3111c0s31b1n0:ncpus=1)+(x3111c0s37b0n0:ncpus=1)+(x3111c0s37b1n0:ncpus=1)+(x3111c0s7b0n0:ncpus=1)+(x3111c0s7b1n0:ncpus=1)+(x3112c0s13b0n0:ncpus=1)+(x3112c0s13b1n0:ncpus=1)+(x3112c0s19b0n0:ncpus=1)+(x3112c0s19b1n0:ncpus=1)+(x3112c0s1b1n0:ncpus=1)+(x3112c0s25b0n0:ncpus=1)+(x3112c0s25b1n0:ncpus=1)+(x3112c0s31b0n0:ncpus=1)+(x3112c0s31b1n0:ncpus=1)+(x3112c0s37b0n0:ncpus=1)+(x3112c0s37b1n0:ncpus=1)+(x3112c0s7b0n0:ncpus=1)+(x3112c0s7b1n0:ncpus=1)+(x3201c0s13b0n0:ncpus=1)+(x3201c0s13b1n0:ncpus=1)+(x3201c0s19b0n0:ncpus=1)+(x3201c0s19b1n0:ncpus=1)+(x3201c0s1b0n0:ncpus=1)+(x3201c0s1b1n0:ncpus=1)+(x3201c0s25b0n0:ncpus=1)+(x3201c0s25b1n0:ncpus=1)+(x3201c0s31b0n0:ncpus=1)+(x3201c0s31b1n0:ncpus=1)+(x3201c0s37b0n0:ncpus=1)+(x3201c0s37b1n0:ncpus=1)+(x3201c0s7b0n0:ncpus=1)+(x3201c0s7b1n0:ncpus=1)+(x3202c0s13b0n0:ncpus=1)+(x3202c0s13b1n0:ncpus=1)+(x3202c0s19b0n0:ncpus=1)+(x3202c0s19b1n0:ncpus=1)+(x3202c0s1b0n0:ncpus=1)+(x3202c0s1b1n0:ncpus=1)+(x3202c0s25b0n0:ncpus=1)+(x3202c0s25b1n0:ncpus=1)+(x3202c0s31b0n0:ncpus=1)+(x3202c0s31b1n0:ncpus=1)+(x3202c0s37b1n0:ncpus=1)+(x3202c0s7b0n0:ncpus=1)+(x3202c0s7b1n0:ncpus=1)+(x3203c0s13b0n0:ncpus=1)+(x3203c0s13b1n0:ncpus=1)+(x3203c0s19b0n0:ncpus=1)+(x3203c0s19b1n0:ncpus=1)+(x3203c0s1b0n0:ncpus=1)+(x3203c0s1b1n0:ncpus=1)+(x3203c0s25b0n0:ncpus=1)+(x3203c0s25b1n0:ncpus=1)+(x3203c0s31b0n0:ncpus=1)+(x3203c0s31b1n0:ncpus=1)+(x3203c0s37b0n0:ncpus=1)+(x3203c0s37b1n0:ncpus=1)+(x3203c0s7b0n0:ncpus=1)+(x3203c0s7b1n0:ncpus=1)+(x3204c0s13b0n0:ncpus=1)+(x3204c0s13b1n0:ncpus=1)+(x3204c0s19b0n0:ncpus=1)+(x3204c0s19b1n0:ncpus=1)+(x3204c0s1b0n0:ncpus=1)+(x3204c0s1b1n0:ncpus=1)+(x3204c0s25b0n0:ncpus=1)+(x3204c0s25b1n0:ncpus=1)+(x3204c0s31b0n0:ncpus=1)+(x3204c0s31b1n0:ncpus=1)+(x3204c0s37b0n0:ncpus=1)+(x3204c0s37b1n0:ncpus=1)+(x3204c0s7b0n0:ncpus=1)+(x3204c0s7b1n0:ncpus=1)+(x3001c0s13b0n0:ncpus=1)+(x3001c0s13b1n0:ncpus=1)+(x3001c0s19b1n0:ncpus=1)+(x3001c0s1b0n0:ncpus=1)+(x3001c0s1b1n0:ncpus=1)+(x3001c0s25b0n0:ncpus=1)+(x3001c0s25b1n0:ncpus=1)+(x3001c0s31b0n0:ncpus=1)+(x3001c0s31b1n0:ncpus=1)+(x3001c0s37b0n0:ncpus=1)+(x3001c0s37b1n0:ncpus=1)+(x3001c0s7b0n0:ncpus=1)+(x3001c0s7b1n0:ncpus=1)+(x3002c0s13b0n0:ncpus=1)+(x3002c0s13b1n0:ncpus=1)+(x3002c0s19b0n0:ncpus=1)+(x3002c0s19b1n0:ncpus=1)",
#                 "start_time":"Thu Nov  9 13:04:14 2023"
#             },
#             "project":"TokamakITER",
#             "Submit_Host":"polaris-login-01.hsn.cm.polaris.alcf.anl.gov"
#         },
#     }
# }
import chardet
def qstat_jobs(exec='/opt/pbs/bin/qstat',
              args=['-f','-F','json']) -> dict:
   cmd = exec + ' ' + ' '.join(args)
   completed_process = sp.run(cmd.split(' '),capture_output=True)
   if completed_process.returncode != 0:
      raise Exception(completed_process.stderr.decode('utf-8'))
   try:
      return json.loads(completed_process.stdout.decode(errors='ignore'))
   except:
      # try parsing line by line:
      byte_lines = completed_process.stdout.split(b'\n')
      str_lines = []
      for bline in byte_lines:
         try:
            str_lines.append(bline.decode('utf-8'))
         except UnicodeDecodeError:
            pass
      
      return json.loads('\n'.join(str_lines))

def convert_jobs_to_dataframe(job_data: dict,server_data: dict) -> pd.DataFrame:
   jd = job_data['Jobs']
   job_data = []
   for jobid,job in jd.items():
      score = execute_job_sort_formula(server_data,job) 
      jobid = jobid.split('.')[0]
      row = {
         'jobid':jobid,
         'user':job['Variable_List'].get('PBS_O_LOGNAME',''),
         'state':job['job_state'],
         'queue':job['queue'],
         'nodes':job['Resource_List'].get('nodect',0),
         'score':score,
         'runtime':string_time_to_minutes(job['Resource_List']['walltime']),
         'qtime':job.get('qtime',''), # "Fri Nov 10 11:34:29 2023"
         'ctime':job.get('ctime',''), # "Fri Nov 10 11:34:29 2023"
         'etime':job.get('etime',''), # "Fri Nov 10 11:34:29 2023"
         'mtime':job.get('mtime',''), # "Fri Nov 10 11:34:29 2023"
         'stime':job.get('stime',''), # "Fri Nov 10 11:34:29 2023"
         'obittime':job.get('obittime',''), # "Fri Nov 10 11:34:29 2023"
         'eligible_time':string_time_to_minutes(job.get('eligible_time','0:0:0')), # "42:53:26"
         'project':job.get('project',''),
         'name':job.get('Job_Name',''),
         'award_category': job['Resource_List'].get('award_category',''),
         'filesystems':job['Resource_List'].get('filesystems',''),
         'total_allocation':job['Resource_List'].get('total_allocation',''),
         'current_allocation':job['Resource_List'].get('current_allocation',''),
         'jobdir':job['Resource_List'].get('jobdir',''),
         'workdir':job['Variable_List'].get('PBS_O_WORKDIR',''),
         'node_hours': int(job['Resource_List'].get('nodect',0) * string_time_to_hours(job['Resource_List']['walltime'])),
      }

      if 'stime' in job and 'obittime' in job:
         stime = dateutil.parser.parse(row['stime'])
         obittime = dateutil.parser.parse(row['obittime'])
         row['run_time'] = (obittime - stime).total_seconds()/60
      else:
         row['run_time'] = 0

      if 'qtime' in job:
         qtime = dateutil.parser.parse(row['qtime'])
         row['queued_time'] = (datetime.datetime.now() - qtime).total_seconds()/60

      job_data.append(row)
   
   df = pd.DataFrame(job_data)
   df['qtime'] = pd.to_datetime(df['qtime'])
   df['ctime'] = pd.to_datetime(df['ctime'])
   df['etime'] = pd.to_datetime(df['etime'])
   df['mtime'] = pd.to_datetime(df['mtime'])
   df['stime'] = pd.to_datetime(df['stime'])
   df['obittime'] = pd.to_datetime(df['obittime'])


   return df


def print_jobs(job_data: dict,server_data: dict = None) -> None:
   if server_data is None:
      server_data = qstat_server()
   jd = job_data['Jobs']
   logger.info(f"{'Job ID':<10s}: {'User':>20s} {'State':>10s} {'Queue':>20s} {'Nodes':>10s} {'Score':>10s}")
   for jobid,job in jd.items():
      score = execute_job_sort_formula(server_data,job) 
      jobid = jobid.split('.')[0]
      logger.info(f"{jobid:<10s}: {job['Variable_List']['PBS_O_LOGNAME']:>20s} {job['job_state']:>10s} {job['queue']:>20s} {job['Resource_List']['nodect']:>10d} {score:10.2f}")


def print_top_jobs(job_df: pd.DataFrame, 
                   top_n: int = 10,
                   state_filter = ['R','Q'],
                   queue_filter = ['debug','debug-scaling','small','medium','large','preemptable']) -> None:
   tmpdf = job_df[job_df['state'].isin(state_filter)]
   tmpdf = tmpdf[tmpdf['queue'].isin(queue_filter)]
   pd.set_option('display.float_format', '{:10.0f}'.format)
   ordered_df = tmpdf.sort_values('score',ascending=False)
   grouped_df = ordered_df.groupby('queue')
   for name,group in grouped_df:
      group = group.head(top_n)
      logger.info(f"Top {top_n} out of {group.shape[0]} jobs in queue {name}:\n{group[['jobid','user','project','state','queue','nodes','score','filesystems']]}")
   # logger.info(f"Top {top_n} out of {job_df.shape[0]} jobs by score:\n{ordered_df[['jobid','user','project','state','queue','nodes','score']]}")


# This code returns data in the following format:
# {
#     "timestamp":1699645501,
#     "pbs_version":"2022.1.1.20220926110806",
#     "pbs_server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#     "Server":{
#         "polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov":{
#             "server_state":"Active",
#             "server_host":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "scheduling":"True",
#             "total_jobs":8304,
#             "state_count":"Transit:0 Queued:211 Held:47 Waiting:0 Running:11 Exiting:0 Begun:0 ",
#             "managers":"allcock@*,ascovel@*,blenard@*,cblackworth@*,grog@*,gwest@*,homerdin@*,jbouvet@*,leggett@*,mluczkow@*,pershey@*,richp@*,toonen@*,zpettit@*",
#             "operators":"appmm2pbs@*,rloy@*",
#             "default_queue":"prod",
#             "log_events":2047,
#             "mailer":"/usr/sbin/sendmail",
#             "mail_from":"cobalt@alcf.anl.gov",
#             "query_other_jobs":"True",
#             "resources_available":{
#                 "eagle_fs":"True",
#                 "grand_fs":"False",
#                 "home_fs":"True",
#                 "swift_fs":"True",
#                 "valid_filesystems":"home_fs,swift_fs,grand_fs,eagle_fs"
#             },
#             "resources_default":{
#                 "allow_account_check_failure":"True",
#                 "allow_negative_allocation":"True",
#                 "backfill_factor":84600,
#                 "backfill_max":50,
#                 "base_score":0,
#                 "enable_backfill":0,
#                 "enable_fifo":0,
#                 "enable_wfp":0,
#                 "fifo_factor":1800,
#                 "ncpus":1,
#                 "ni_resource":"polaris",
#                 "preempt_targets":"NONE",
#                 "score_boost":0,
#                 "total_cpus":560,
#                 "wfp_factor":100000
#             },
#             "default_chunk":{
#                 "ncpus":64
#             },
#             "resources_assigned":{
#                 "mem":"0gb",
#                 "mpiprocs":4,
#                 "ncpus":3300,
#                 "nodect":210
#             },
#             "scheduler_iteration":600,
#             "flatuid":"True",
#             "resv_enable":"True",
#             "node_fail_requeue":310,
#             "max_array_size":10000,
#             "node_group_enable":"True",
#             "node_group_key":"tier0,tier1,system",
#             "default_qsub_arguments":"-k doe -r n -W umask=0022",
#             "pbs_license_info":"6200@license-polaris-01.lab.alcf.anl.gov:6200@license-polaris-02.lab.alcf.anl.gov:6200@license-polaris-03.lab.alcf.anl.gov",
#             "pbs_license_min":1120,
#             "pbs_license_max":1200,
#             "pbs_license_linger_time":31536000,
#             "license_count":"Avail_Global:28 Avail_Local:1 Used:1119 High_Use:1119",
#             "pbs_version":"2022.1.1.20220926110806",
#             "job_sort_formula":"base_score + score_boost + (enable_wfp * wfp_factor * (eligible_time ** 2 / min(max(walltime,21600.0),43200.0) ** 3  * project_priority * nodect / total_cpus)) + (enable_backfill * min(backfill_max, eligible_time / backfill_factor)) + (enable_fifo * eligible_time / fifo_factor)",
#             "eligible_time_enable":"True",
#             "job_history_enable":"True",
#             "max_concurrent_provision":5,
#             "backfill_depth":10,
#             "python_restart_max_hooks":1000000000,
#             "python_restart_max_objects":1000000000,
#             "python_restart_min_interval":"123127:46:40",
#             "max_job_sequence_id":9999999
#         }
#     }
# }
def qstat_server(exec='/opt/pbs/bin/qstat',
                 args=['-B','-f','-F','json']) -> dict:
   cmd = exec + ' ' + ' '.join(args)
   completed_process = sp.run(cmd.split(' '),stdout=sp.PIPE,stderr=sp.PIPE)
   if completed_process.returncode != 0:
      raise Exception(completed_process.stderr.decode('utf-8'))
   return json.loads(completed_process.stdout.decode('utf-8'))

def execute_job_sort_formula(server_data: dict, job_data: dict) -> float:
   server_dict = server_data['Server']
   first_server = server_dict[list(server_dict.keys())[0]]
   formula_str = first_server['job_sort_formula']

   rl = job_data['Resource_List']
   base_score        = float(rl['base_score'])
   score_boost       = float(rl['score_boost'])
   enable_wfp        = float(rl['enable_wfp'])
   wfp_factor        = float(rl['wfp_factor'])
   eligible_time     = float(string_time_to_seconds(job_data['eligible_time']))
   walltime          = float(string_time_to_seconds(rl['walltime']))
   project_priority  = float(rl['project_priority'])
   nodect            = float(rl['nodect'])
   total_cpus        = float(rl['total_cpus'])
   enable_backfill   = float(rl['enable_backfill'])
   backfill_max      = float(rl['backfill_max'])
   backfill_factor   = float(rl['backfill_factor'])
   enable_fifo       = float(rl['enable_fifo'])
   fifo_factor       = float(rl['fifo_factor'])

   return eval(formula_str)




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
