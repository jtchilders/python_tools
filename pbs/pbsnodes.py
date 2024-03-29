import subprocess as sp
import json
import logging
logger = logging.getLogger(__name__)

# returns data formated as follows:
# {
#     "timestamp":1699630815,
#     "pbs_version":"2022.1.1.20220926110806",
#     "pbs_server":"polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#     "nodes":{
#          "x3006c0s13b1n0":{
#             "Mom":"x3006c0s13b1n0.hsn.cm.polaris.alcf.anl.gov",
#             "ntype":"PBS",
#             "state":"free",
#             "pcpus":64,
#             "resv":"M1150389.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "resources_available":{
#                 "arch":"linux",
#                 "debug":"True",
#                 "demand":"False",
#                 "gputype":"A100",
#                 "host":"x3006c0s13b1n0",
#                 "mem":"527672488kb",
#                 "ncpus":64,
#                 "ngpus":4,
#                 "ss11":"False",
#                 "system":"polaris",
#                 "tier0":"x3006-g1",
#                 "tier1":"g1",
#                 "vnode":"x3006c0s13b1n0"
#             },
#             "resources_assigned":{
#             },
#             "resv_enable":"True",
#             "sharing":"force_exclhost",
#             "license":"l",
#             "last_state_change_time":1699617414,
#             "last_used_time":1699617413
#         },
#         "x3008c0s37b1n0":{
#             "Mom":"x3008c0s37b1n0.hsn.cm.polaris.alcf.anl.gov",
#             "ntype":"PBS",
#             "state":"job-exclusive",
#             "pcpus":64,
#             "jobs":[
#                 "1147579.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov"
#             ],
#             "resv":"M1150389.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "resources_available":{
#                 "arch":"linux",
#                 "demand":"False",
#                 "gputype":"A100",
#                 "host":"x3008c0s37b1n0",
#                 "mem":"527672488kb",
#                 "ncpus":64,
#                 "ngpus":4,
#                 "ss11":"False",
#                 "system":"polaris",
#                 "tier0":"x3008-g1",
#                 "tier1":"g1",
#                 "vnode":"x3008c0s37b1n0"
#             },
#             "resources_assigned":{
#                 "ncpus":1
#             },
#             "resv_enable":"True",
#             "sharing":"force_exclhost",
#             "license":"l",
#             "last_state_change_time":1699556747,
#             "last_used_time":1699556747
#         },
#         "x3009c0s19b0n0":{
#             "Mom":"x3009c0s19b0n0.hsn.cm.polaris.alcf.anl.gov",
#             "ntype":"PBS",
#             "state":"down",
#             "pcpus":64,
#             "resv":"M1150389.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
#             "resources_available":{
#                 "arch":"linux",
#                 "demand":"False",
#                 "gputype":"A100",
#                 "host":"x3009c0s19b0n0",
#                 "mem":"527672488kb",
#                 "ncpus":64,
#                 "ngpus":4,
#                 "ss11":"True",
#                 "system":"polaris",
#                 "tier0":"x3009-g2",
#                 "tier1":"g2",
#                 "vnode":"x3009c0s19b0n0"
#             },
#             "resources_assigned":{
#             },
#             "comment":"node down: communication closed",
#             "resv_enable":"True",
#             "sharing":"force_exclhost",
#             "license":"l",
#             "last_state_change_time":1699558943,
#             "last_used_time":1699480944
#         },
#         ...
#     }
# }
def pbsnodes(exec: str ='pbsnodes',
             args: list = ['-a','-F','json']) -> dict:
   """
   Executes the pbsnodes command and returns the output as a JSON object.
   
   :param exec: The name or path of the pbsnodes executable (default: 'pbsnodes')
   :type exec: str
   
   :param args: Additional arguments to pass to pbsnodes (default: [])
   :type args: list
   
   :return: The output of the pbsnodes command as a JSON object
   :rtype: dict
   
   :raises Exception: If the pbsnodes command returns a non-zero exit code, an exception is raised with the error message from stderr
   """
   cmd = exec + ' ' + ' '.join(args)
   result = sp.run(cmd.split(' '),stdout=sp.PIPE,stderr=sp.PIPE)
   
   if result.returncode != 0:
      raise Exception(result.stderr.decode('utf-8'))
   
   return json.loads(result.stdout.decode('utf-8'))


def count_nodes(pbsnodes_data: dict) -> int:
   """
   Count the number of nodes in the pbsnodes_data dictionary.

   Args:
       pbsnodes_data (dict): A dictionary containing information about the PBS nodes.

   Returns:
       int: The number of nodes in the pbsnodes_data.
   """
   return len(pbsnodes_data['nodes'])

def count_free_nodes(pbsnodes_data: dict) -> int:
   """
   Counts the number of free nodes in the given pbsnodes_data.

   Parameters:
   - pbsnodes_data (dict): The data containing information about the nodes.

   Returns:
   - int: The number of free nodes.
   """
   return len([n for n in pbsnodes_data['nodes'].values() if n['state'] == 'free'])

def get_node_states(pbsnodes_data: dict) -> list:
   """
   Get a unique list of states from the pbsnodes_data dictionary.
   
   Parameters:
       pbsnodes_data (dict): The input dictionary containing information about the nodes.
   
   Returns:
       list: A list of unique states of the nodes.
   """
   return list(set([n['state'] for n in pbsnodes_data['nodes'].values()]))

def get_nodes_in_state(pbs_nodes_data: dict) -> dict:
   """
   Generate a dictionary of nodes in each state based on the provided PBS nodes data.

   Parameters:
   - pbs_nodes_data (dict): A dictionary containing information about PBS nodes.

   Returns:
   - dict: A dictionary where each key represents a different state and the value is a list of nodes in that state.
   """
   unique_states = get_node_states(pbs_nodes_data)
   return {state:[n for n in pbs_nodes_data['nodes'].values() if n['state'] == state] for state in unique_states}

def get_ss11_nodes(pbs_nodes_data: dict) -> list:
   """
   Get a list of nodes with ss11 resources.

   Parameters:
   - pbs_nodes_data (dict): A dictionary containing information about PBS nodes.

   Returns:
   - list: A list of nodes with ss11 resources.
   """
   output = []
   for n in pbs_nodes_data['nodes'].values():
      if 'ss11' in n['resources_available']:
         if n['resources_available']['ss11'] == 'True':
            output.append(n)
   return output

def print_nodes_in_state(pbs_nodes_data: dict, summarize: bool = False) -> None:
   """
   Print the nodes in each state in the provided PBS nodes data with dynamic column width.

   Parameters:
   - pbs_nodes_data (dict): A dictionary containing information about PBS nodes.

   Returns:
   - None
   """
   # get dictionary of number of nodes in each state key
   nodes_in_state = get_nodes_in_state(pbs_nodes_data)
   
   total_nodes = count_nodes(pbs_nodes_data)
   # if the summarize flag is set, combine states into summary categories as
   # defined by this dictionary:
   if summarize:
      new_nodes_in_state = {}
      for state in nodes_in_state:
         if 'resv-exclusive' in state:
            new_nodes_in_state['in-reservation'] = new_nodes_in_state.get('in-reservation', []) + nodes_in_state[state]
         elif 'job-exclusive' in state:
            new_nodes_in_state['in-use'] = new_nodes_in_state.get('in-use', []) + nodes_in_state[state]
         elif 'down' in state or 'offline' in state:
            new_nodes_in_state['offline'] = new_nodes_in_state.get('offline', []) + nodes_in_state[state]
         elif 'free' in state:
            new_nodes_in_state['free'] = new_nodes_in_state.get('free', []) + nodes_in_state[state]

      nodes_in_state = new_nodes_in_state

   # Find the longest state string
   longest_state_len = max(len(state) for state in nodes_in_state)

   # Adjust the column width based on the longest state string
   col_width = max(longest_state_len, len("Node State")) + 1

   logger.info(f"{'Node State':<{col_width}}: {'Count':>5}")
   logger.info(f"{'-'*col_width}: {'-'*5}")
   for state in nodes_in_state:
      logger.info(f"{state:<{col_width}}: {len(nodes_in_state[state]):5}")
   logger.info(f"{'-'*col_width}: {'-'*5}")
   label = "Total nodes"
   logger.info(f"{label:<{col_width}}: {total_nodes:5}")
   logger.info(f"{'-'*col_width}: {'-'*5}")


def print_ss_node_count(pbs_nodes_data: dict) -> None:
   """
   Print the number of nodes with ss11 resources in the provided PBS nodes data.

   Parameters:
   - pbs_nodes_data (dict): A dictionary containing information about PBS nodes.

   Returns:
   - None
   """
   ss_nodes = get_ss11_nodes(pbs_nodes_data)
   logger.info(f"{'Node SS State':<20s}: {'Count':>5}")
   logger.info(f"{'-'*20}: {'-'*5}")
   logger.info(f"{'ss11':<20s}: {len(ss_nodes):5}")
   logger.info(f"{'ss10':<20s}: {len(pbs_nodes_data['nodes']) - len(ss_nodes):5}")
   logger.info(f"{'-'*20}: {'-'*5}")