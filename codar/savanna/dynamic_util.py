import time
import os

class DynamicUtil():

   @staticmethod
   def get_env(env, key, default=""):
       value = default
       if key in env.keys():
           value = env[key]
       else:
           value = os.environ.get(key, default) 
       return value

   @staticmethod
   def get_index(lst, val):
       try:
           return lst.index(val)
       except:
           return -1

   @staticmethod
   def generate_rfile(pipeline, tau_fname):
       rfile_json = {}
       rfile_json['node']=[]
       nodes_assigned = pipeline.get_assigned_nodes()
       index = 0
       for asgn_node in nodes_assigned:
           print("Assigned node ", asgn_node)
           node = {}
           node['name']= asgn_node
           node['mapping'] = []
           flag = 0
           for run in pipeline.runs:
               if run.name == 'rmonitor':
                   continue
               print("Run name", run.name)
               run_map = {}
               n_ranks_per_node = 0
               if run.nodes_assigned is None:
                   n_ranks_per_node = run.tasks_per_node
               elif asgn_node in run.nodes_assigned: 
                   n_ranks_per_node = int(run.nprocs/len(run.nodes_assigned))
                   #index = DynamicUtil.get_index(run.nodes_assigned, asgn_node)
               if n_ranks_per_node != 0 and (index + 1) * n_ranks_per_node <= run.nprocs: # and run.get_start_time > time.time():
                   run_map['stream_nm'] = "../" + run.name + "/" + tau_fname + ".bp"
                   ranks = list(range(index * n_ranks_per_node, index * n_ranks_per_node + n_ranks_per_node))
                   run_map['ranks'] = ranks
                   node['mapping'].append(run_map)
                   flag = 1
           if flag:
               rfile_json['node'].append(node)
           index = index + 1
       return rfile_json



