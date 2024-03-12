#!/usr/bin/env python
import argparse,logging
import pbs
logger = logging.getLogger(__name__)


def main():
   ''' Print summary information about a system using PBS '''
   # logging_format = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
   # logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_format = ''
   logging_datefmt = ''
   logging_level = logging.INFO
   
   parser = argparse.ArgumentParser(description='print summary information about a system using PBS')

   parser.add_argument('--full-node-state', default=True, action='store_false', help="Print full node state information")
   
   parser.add_argument('--debug', default=False, action='store_true', help="Set Logger to DEBUG")
   parser.add_argument('--error', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--warning', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--logfilename', default=None, help='if set, logging information will go to file')
   args = parser.parse_args()

   if args.debug and not args.error and not args.warning:
      logging_level = logging.DEBUG
   elif not args.debug and args.error and not args.warning:
      logging_level = logging.ERROR
   elif not args.debug and not args.error and args.warning:
      logging_level = logging.WARNING

   logging.basicConfig(level=logging_level,
                       format=logging_format,
                       datefmt=logging_datefmt,
                       filename=args.logfilename)

   pbsnodes_data = pbs.pbsnodes()
   pbs.print_nodes_in_state(pbsnodes_data,summarize=args.full_node_state)
   # pbs.print_ss_node_count(pbsnodes_data)
   pbsqstat_queues = pbs.qstat_queues()
   pbs.print_queued_jobs_states(pbsqstat_queues,summarize=args.full_node_state)
   pbsqstat_jobs = pbs.qstat_jobs()
   pbsqstat_server = pbs.qstat_server()
   # pbs.print_jobs(pbsqstat_jobs)
   jobdf = pbs.convert_jobs_to_dataframe(pbsqstat_jobs,pbsqstat_server)
   pbs.print_top_jobs(jobdf)
   


if __name__ == "__main__":
   main()
