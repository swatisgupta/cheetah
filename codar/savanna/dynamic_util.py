import time
import os
import logging


#logging.basicConfig(
#    format='%(asctime)s %(levelname)-8s %(message)s',
#    level=logging.INFO,
#    datefmt='%Y-%m-%d %H:%M:%S')


class DynamicUtil():

   log_dynamic = logging.getLogger('codar.savanna.dynamic')

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
               if run.name == 'rmonitor' or not run.monitor:
                   continue
               run_map = {}
               print("Run name", run.name,  run.tasks_per_node, run.nodes_assigned)
               n_ranks_per_node = 0
               if run.nodes_assigned is None:
                   n_ranks_per_node = run.tasks_per_node
                   print("Run name", run.name,  run.tasks_per_node)
               elif asgn_node in run.nodes_assigned: 
                   n_ranks_per_node = int(run.nprocs/len(run.nodes_assigned))
                   print("Run name", run.name,  run.nodes_assigned)
                   #index = DynamicUtil.get_index(run.nodes_assigned, asgn_node)
               if n_ranks_per_node != 0 and (index + 1) * n_ranks_per_node <= run.nprocs: # and run.get_start_time > time.time():
                   ranks = list(range(index * n_ranks_per_node, index * n_ranks_per_node + n_ranks_per_node))
                   run_map['stream_nm'] = run.monitor['stream_nm']
                   run_map['ranks'] = ranks
                   run_map['stream_eng'] = run.monitor['stream_eng'] 
                   run_map['model_params'] = run.monitor['model_params']
                   node['mapping'].append(run_map)
                   flag = 1
           if flag:
               rfile_json['node'].append(node)
           index = index + 1
       return rfile_json

   @staticmethod
   def generate_dag(workflow_dagfile, path):
       pipeline_map = {}
       pipeline_map_p = {}
       if workflow_dagfile == "":
           return pipeline_map, pipeline_map_p
 
       filename = path + "/" + workflow_dagfile 
       if not os.path.exists(filename):
           return pipeline_map, pipeline_map_p
    
       with open(filename) as dagfile:
           line = dagfile.readline()
           while line:
                dependency_list = line.split(":")
                component = dependency_list[0].strip()
                depends_on = dependency_list[1].strip()
                dependency_file = dependency_list[2]
                dependency_params = "" 
                if len(dependency_list) > 3:
                    dependency_params = dependency_list[3]
                if depends_on not in pipeline_map.keys():
                    pipeline_map[depends_on] = {}
                if component not in pipeline_map[depends_on].keys():
                    pipeline_map[depends_on][component] = {}

                if component not in pipeline_map_p.keys():
                    pipeline_map_p[component] = {}
                if depends_on not in pipeline_map_p[component].keys():
                    pipeline_map_p[component][depends_on] = {}
 
                pipeline_map[depends_on][component][dependency_file] = dependency_params.split()
                line = dagfile.readline()

       list1 = []
       for deps in pipeline_map.keys():
           if deps not in list1:
               print("DEPS...", deps)
               list1.extend(DynamicUtil.acyclic_dependency(deps, pipeline_map))                    

       #list1 = []
       #for deps in pipeline_map_p.keys():
       #    if deps is not in list1:
       #        list1.extend(self._acyclic_dependency(deps, pipeline_map_p))                    
       return pipeline_map, pipeline_map_p

   @staticmethod
   def acyclic_dependency(comp, map):
       list = []
       if comp not in map.keys():
           return list
       for i in map[comp].keys():
           print("Rec call acyclic_dependency(", i, " , ",  map, ")" ) 
           list.extend(DynamicUtil.acyclic_dependency(i,map))
       for i in list:
           map[comp][i] = {}
       return list


'''
class TreeNode():
    def __init(self, name, files):
        self.Name = name
        self.files = files
 
class Tree():
     def __init(self):
         self.parent = []
         self.child = []

     def add_parent(self, tree_node):
         self.parent.append(tree_node)
 
     def add_child(self, tree_node):
         self.child.append(tree_node)
'''
     
