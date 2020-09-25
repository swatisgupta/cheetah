#!/usr/bin/env python3

#from mpi4py import MPI
import os
import threading
import socket as soc
import zmq
import datetime
import dateutil.parser
import sys
import json
import time 
import codar.savanna.producer
import codar.savanna.consumer
from codar.savanna.dynamic_util import DynamicUtil
from queue import Queue 
import math
import datetime
import traceback

class DynamicControls():
    
    def __init__(self, consumer):
        self.recv_port = 8080
        self.cur_oport = 8085
        self.cur_lport = 8086
        self.starttime = datetime.datetime.now()
        self.reciever_thread = None  
        self.decision_thread = None 
        self.recv_cond = threading.Condition()
        self.send_cond = threading.Condition()
        self.msg_cond = threading.Condition()
        self.pipeline_cond = threading.Condition()
        self.msg_queue = []
        self.stop_recv = False
        self.stop_send = False
        self.recv_socket = None
        self.pipelines_oport = {}
        self.pipelines = {}
        self.machine = ""
        self.monitors = {}
        self.active_pipelines = []
        self.active_pipelines_runs = {}
        self.pipeline_sockets = {}
        self.pipeline_socket_port = {}
        self.pipeline_socket_ip = {}
        self.pipeline_models = {}
        self.pipeline_restart = {}
        self.pipeline_runs = {} 
        self.pipeline_dag = {}
        self.pipeline_priority = {}
        self.pipeline_rev_priority = {}
        self.consumer = consumer
        self.run_map = {}
        self.timestamp = {}

    def _create_request(self, model_name, timestamp, req_type, msg={}):
        timestamp = list(divmod(timestamp.total_seconds(), 60)) 
        request = {}
        request["model"] = model_name
        request["timestamp"] = timestamp
        request["msg_type"] = req_type
        request["message"] = msg
        return json.dumps(request)

    def _receiver(self):
        address = soc.gethostbyname(soc.gethostname())
        context = zmq.Context()
        self.recv_socket = context.socket(zmq.REP)
        socket_str = "tcp://" + address + ":" + str(self.recv_port)
        self.recv_socket.bind(socket_str) 
        
        keep_alive = True    
        print("Running receiver at socket ", socket_str)
        timestamps = {} 
        while keep_alive == True:
            try:
                with self.recv_cond:
                    if self.stop_recv == True:
                        keep_alive = False
                        print("Receiver: Signing off....")
                        sys.stdout.flush()
                        continue

                #print("Waiting for a message....")
                sys.stdout.flush()
                message = self.recv_socket.recv()
                message = message.decode("utf-8") 
                print("Received a critical update from monitor : ", message)
                self.recv_socket.send_string("OK")
                #print("Send ack : OK")
                sys.stdout.flush()
                message = json.loads(message)
                #print("Message decoded : ", message)
                sys.stdout.flush()
                port, timestamp = self._get_mesg_address_timestamp(message)

                if port is not None:
                    if port not in timestamps.keys():
                        timestamps[port] = timestamp

                    if timestamps[port] <= timestamp:
                        with self.msg_cond:
                            self.msg_queue.append(message)
                        timestamps[port] = timestamp 

                #print("Send msg to queue : OK")    
                sys.stdout.flush()

            except Exception as e:
                print("Reciever : Got exception...", e)
                traceback.print_stack()
                sys.stdout.flush() 
  
    def _open_sender_connections(self, address, port):
        #address = soc.gethostbyname(soc.gethostname())
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket_str = "tcp://" + address + ":" + str(port)
        socket.connect(socket_str)
        print("Opened socket : ", socket_str)
        return socket

    def _get_mesg_address_timestamp(self, message):
        port = None
        timestamp = None
        if "socket" in message.keys():
            port = message["socket"]
        if "timestamp" in message.keys():
            date_time_str = message["timestamp"]
            timestamp = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
        return port, timestamp

    def __policy_iterate(self, pipeline_id, update):
           r_steps = 0
           n_map = {}
           for state in update:
               for node in update.keys():
                   r_steps = int(update[node]['G_STEPS'])
                   n_map = update[node]
                   break
               break

           with self.pipeline_cond:
               niter = self.consumer.get_pipeline_nrestart(pipeline_id) + 1

               if pipeline_id not in self.active_pipelines:
                   return refresh

               restart_steps = int(self.pipeline_restart[pipeline_id])
               #print("Current steps ", r_steps,  " : Terminate after ", restart_steps)
               if r_steps != -1 and r_steps >= restart_steps:
                   print("Stopping the pipeline: ", pipeline_id, " Timestamp : ", time.time())
                   DynamicUtil.log_dynamic.info("Total steps completed {} for pipeline {}".format(r_steps, pipeline_id))
                   self.consumer.set_pipeline_restart(pipeline_id, False)
                   self.consumer.stop_pipeline_all(pipeline_id)
                   print("Done stopping the pipeline: ", pipeline_id, " Timestamp : ", time.time())
                   dereg = True
               else:
                   run_names = []
                   run_p = {}
                   run_map = self.pipeline_runs[pipeline_id]

                   for run in run_map.keys():

                       if run not in n_map['STEPS']:
                           continue

                       r_params = run_map[run]
                       #print("Parameters for ", run, "are ", r_params) 
                       if r_params:
                           run_cond = int(r_params['model_params'][2])
                           input_file = r_params['model_params'][5]
                           key = r_params['model_params'][6]
                           step_fn = r_params['model_params'][7].strip()
                           ch_iter = int(r_params['model_params'][8])
                           max_iter = int(r_params['model_params'][9])
                           last_killed = int(r_params['last_killed'])
                           if step_fn == 'log2':
                               run_cond = run_cond - int(math.log2(run_cond))
                           elif step_fn == 'log':
                               run_cond = run_cond - int(math.log(run_cond))
                           #print("run condition for run ", run, " is ", run_cond, " step function is ", step_fn,  flush = True)   
                           if niter % ch_iter == 0 and last_killed < niter and n_map['STEPS'][run] >= run_cond and n_map['STEPS'][run] < max_iter and r_steps > (ch_iter-1) * max_iter * 2:
                               run_names.append(run)
                               DynamicUtil.log_dynamic.info("Total steps completed {}, steps completed by run {} at iteration {} are {} >= {} for pipeline {}".format(r_steps, run, niter, n_map['STEPS'][run], run_cond,  pipeline_id))
                               self.pipeline_runs[pipeline_id][run]['model_params'][2] = run_cond
                               self.pipeline_runs[pipeline_id][run]['model_params'][9] = n_map['STEPS'][run]
                               self.pipeline_runs[pipeline_id][run]['last_killed'] = niter
                           run_p[run] = {'nstep' : [input_file, key, run_cond]}
                   if len(run_names) > 0:
                       print("Stopping the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", run_p, "  Timestamp : ", time.time())
                       self.consumer.stop_pipeline_runs(pipeline_id, run_names, run_p)
                       print("Stopped the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", run_p, "  Timestamp : ", time.time())
                       #request = self._create_request(model, datetime.now() - self.starttime , "req:change_params", self.pipeline_runs[pipeline_id])
                       #socket.send_string(request)
                       #message = socket.recv()


    def __policy_stop_on_cond(self):
        pass 

    def __policy_start_on_cond(self):
        pass

    def __policy_readjust_on_cond(self):
        pass
 
    def _decode_and_inact(self, message):
        print("Decoding message ", time.time() )
        #sys.stdout.flush()
        refresh = False
        port = message["socket"]
        model = message["model"]
        state = message["message"]
        message_type = message["msg_type"]
        timestamp =  message["timestamp"]
        #print("message type",   message_type )
        #sys.stdout.flush()

        if message_type == "res:connect":
            #print("message type reached",   message_type )
            sys.stdout.flush()
            with self.pipeline_cond:
               pipeline_id = self.pipelines_oport[port]
               self.pipeline_socket_ip[pipeline_id] = state
               print("Will connect to Ip address:", state , "and port ", port, " for pipeline ", pipeline_id) 
               sys.stdout.flush()
               return refresh
         
        dereg = False
        pipeline_id = -1
        if model == "heartbeat":
           r_steps = 0
           n_map = {}
           for st in state:  
               for node in st.keys():
                   r_steps = int(st[node]['G_STEPS']) 
                   n_map = st[node]
                   break
               break

           with self.pipeline_cond:
               pipeline_id = self.pipelines_oport[port] 
               niter = self.consumer.get_pipeline_nrestart(pipeline_id) + 1

               if pipeline_id not in self.active_pipelines: 
                   return refresh
               restart_steps = int(self.pipeline_restart[pipeline_id])
               #print("Current steps ", r_steps,  " : Terminate after ", restart_steps)
               if r_steps != -1 and r_steps >= restart_steps:
                   print("Stopping the pipeline: ", pipeline_id, " Timestamp : ", time.time())
                   DynamicUtil.log_dynamic.info("Total steps completed {} for pipeline {}".format(r_steps, pipeline_id))
                   self.consumer.set_pipeline_restart(pipeline_id, False)
                   self.consumer.stop_pipeline_all(pipeline_id)
                   print("Done stopping the pipeline: ", pipeline_id, " Timestamp : ", time.time())
                   dereg = True
               else: 
                   run_names = []
                   run_p = {}
                   run_map = self.pipeline_runs[pipeline_id]
                   
                   for run in run_map.keys():

                       if run not in n_map['STEPS']:
                           continue

                       r_params = run_map[run]
                       #print("Parameters for ", run, "are ", r_params) 
                       if r_params: 
                           run_cond = int(r_params['model_params'][2])
                           input_file = r_params['model_params'][5]
                           key = r_params['model_params'][6]
                           step_fn = r_params['model_params'][7].strip()
                           ch_iter = int(r_params['model_params'][8])
                           max_iter = int(r_params['model_params'][9])
                           last_killed = int(r_params['last_killed'])
                           if step_fn == 'log2':
                               run_cond = run_cond - int(math.log2(run_cond))
                           elif step_fn == 'log':                    
                               run_cond = run_cond - int(math.log(run_cond))
                           #print("run condition for run ", run, " is ", run_cond, " step function is ", step_fn,  flush = True)   
                           if niter % ch_iter == 0 and last_killed < niter and n_map['STEPS'][run] >= run_cond and n_map['STEPS'][run] < max_iter and r_steps > (ch_iter-1) * max_iter * 2:
                               run_names.append(run)
                               DynamicUtil.log_dynamic.info("Total steps completed {}, steps completed by run {} at iteration {} are {} >= {} for pipeline {}".format(r_steps, run, niter, n_map['STEPS'][run], run_cond,  pipeline_id))
                               self.pipeline_runs[pipeline_id][run]['model_params'][2] = run_cond 
                               self.pipeline_runs[pipeline_id][run]['model_params'][9] = n_map['STEPS'][run]
                               self.pipeline_runs[pipeline_id][run]['last_killed'] = niter 
                           run_p[run] = {'nstep' : [input_file, key, run_cond]}
                   if len(run_names) > 0:
                       print("Stopping the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", run_p, "  Timestamp : ", time.time())
                       self.consumer.stop_pipeline_runs(pipeline_id, run_names, run_p) 
                       print("Stopped the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", run_p, "  Timestamp : ", time.time())
                       #request = self._create_request(model, datetime.now() - self.starttime , "req:change_params", self.pipeline_runs[pipeline_id])
                       #socket.send_string(request)
                       #message = socket.recv()
        if model == "pace":
           r_steps = 0
           n_map = {}
           for st in state:  
               for node in st.keys():
                   n_map = st[node]
                   break
               break
           new_per_node = 0
           n_per_node = 0
           t_per_node = 0
           m_cpus = []
           m_gpus = []
           cpus = []
           gpus = [] 
           with self.pipeline_cond:
               pipeline_id = self.pipelines_oport[port] 
               
               if pipeline_id not in self.active_pipelines: 
                   return refresh
               #print("Got a message for ", pipeline_id , "with timestamp" , dateutil.parser.parse(timestamp) ," and the last timestamp was ", self.timestamp[pipeline_id], flush = True)
               if self.timestamp[pipeline_id] > dateutil.parser.parse(timestamp): # , '%y-%m-%d %H:%M:%S.%f'):
                   return refresh

               runs_names_inc = []
               runs_params = {}
               runs_names_dec = []
               run_map = self.pipeline_runs[pipeline_id] 
               dag_child = self.pipeline_dag[pipeline_id]["child_dag"]
               dag_parent = self.pipeline_dag[pipeline_id]["parent_dag"]
               for run in run_map.keys():
                   r_params = run_map[run]
                   done_run = 0
                   #print("Parameters for ", run, "are ", r_params) 
                   if r_params: 
                       expected_steptime = int(r_params['model_params'][1])
                       do_change = int(r_params['model_params'][3])
                       if run not in n_map['N_STEPS']:
                           continue
                       if n_map['N_STEPS'][run] == self.pipeline_runs[pipeline_id][run]['last_killed'] or n_map['N_STEPS'][run] == 0: 
                           continue
                       if run == 'rendering':
                           continue

                       for parents in dag_parent[run].keys():
                          done_run = 0
                       """ 
                            if do_change and n_map['N_STEPS'][run] < n_map['N_STEPS'][parents] - 10:
                                print("Adding run ",run , " to inc set") 
                                runs_names_inc.append(run)
                                runs_params[run] = {'cpus_node':'1', 'command':'add'}
                                new_per_node += 1
                                done_run = 1
                                #self.pipeline_runs[pipeline_id][run]['last_killed'] = n_map['N_STEPS'][run] 
                                break
                       """
                       nadjust = 2
                       if done_run == 1:
                           continue
                       elif do_change == 1 and n_map['AVG_STEP_TIME'][run] != 0 and n_map['AVG_STEP_TIME'][run] > 1.3 * expected_steptime: # and n_map['N_STEPS'][run] <= n_map['N_STEPS'][parents]:
                           print("Adding run ",run , " to inc set") 
                           runs_names_inc.append(run)
                           runs_params[run] = {'cpus_node': str(nadjust), 'command':'add'}
                           new_per_node += nadjust
                           #self.pipeline_runs[pipeline_id][run]['last_killed'] = n_map['N_STEPS'][run] 
                       elif do_change == 1 and n_map['AVG_STEP_TIME'][run] != 0 and n_map['AVG_STEP_TIME'][run] < 0.7 * expected_steptime: # and (n_map['N_STEPS'][run] == n_map['N_STEPS'][parents] or n_map['N_STEPS'][run] == n_map['N_STEPS'][parents] -1 ):
                           n_per_node, m_cpus, m_gpus, rns = self.consumer.get_active_cres(pipeline_id, [run], 0) 
                           #print ("run name ", run, " npernode ", n_per_node, flush = True)
                           if n_per_node > 1:
                               print("Adding run ", run , " to dec set") 
                               runs_names_dec.append(run)
                               runs_params[run] = {'cpus_node': str(nadjust), 'command':'del'}
                               new_per_node -= 2
                               #self.pipeline_runs[pipeline_id][run]['last_killed'] = n_map['N_STEPS'][run] 
                       
               run_names = []
               run_names.extend(runs_names_inc)
               run_names.extend(runs_names_dec)    
               if len(run_names) > 0:
                   dep_runs = []
                   for run in run_names:
                       if run in dag_child.keys():
                           for dep in dag_child[run].keys():
                               if dep not in run_names: 
                                   dep_runs.append(dep)
                   run_names.extend(dep_runs)

                   n_per_node, m_cpus, m_gpus, rns = self.consumer.get_active_cres(pipeline_id, run_names, 0) 
                   t_per_node, u_cpus, u_gpus, rns = self.consumer.get_active_cres(pipeline_id, run_names)
                   t_cpus = self.get_machine_cpu()
                   new_per_node += n_per_node
                   if self.machine == 'local':
                       cpus = range(len(t_cpus) - t_per_node + new_per_node)  
                   else:
                       cpus = [i for i in  u_cpus + m_cpus if i not in u_cpus and i not in m_cpus]  
                       cpus = [i for i in  cpus + t_cpus if i not in cpus and i not in t_cpus]  
                       cpus.extend(m_cpus)
                   gpus = m_gpus
                   #print("CPUS...", cpus, " Total CPUS..", t_cpus, " NEW_PER_NODE...", new_per_node, flush = True )

               r_names = []
               r_names.extend(run_names)
               runs_names_hold = [] 
               #print("Run names before ", run_names, flush = True)  
               if self.machine != 'local' and len(cpus) > new_per_node:
                   rnames = self.consumer.get_inactive(pipeline_id)
                   #print("Inactive run names ", rnames, flush = True)  
                   if len(rnames) != 0:
                       r = rnames[0]
                       runs_names_hold.append(r)  
                       #run_names.append(r)
                       res = len(cpus) - new_per_node
                       runs_params[rnames[0]] = {'cpus_node':res, 'command':'re-assign'}
                       new_per_node += res

               priority = 10000
               while self.machine != 'local' and len(cpus) < new_per_node:
                   vic_names, prior_runs, priority = self.find_victim(pipeline_id, r_names, priority)
                   if len(vic_names) == 0:
                       if len(r_names) == 1:
                           print('Cannot restart runs with new params')
                           break 
                       else:
                           vic_names = prior_runs
                   elif self.pipeline_rev_priority[pipeline_id][r_names[0]] > priority: 
                       break   
                   for vic_name in vic_names:
                       if vic_name in runs_names_inc:
                           if len(cpus) == new_per_node - nadjust:
                               x_per_node, m_cpus, m_gpus, rns = self.consumer.get_active_cres(pipeline_id, [vic_name], all=0)                           
                               cpus = [x for x in cpus if x not in m_cpus]
                           else:
                               print("Stopping the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.time())
                               m_cpus, m_gpus = self.consumer.stop_pipeline_runs(pipeline_id, [vic_name])
                               print("Stopped the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.time(), "CPUS" ,  m_cpus, "GPUS", m_gpus)
                           new_per_node -= len(m_cpus)
                           new_per_node -= nadjust
                           run_names.remove(vic_name)
                       elif vic_name in runs_names_dec:
                           new_per_node += nadjust
                           print("Stopping the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.time())

                           m_cpus, m_gpus = self.consumer.stop_pipeline_runs(pipeline_id, [vic_name])
                           print("Stopped the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.time(), "CPUS" ,  m_cpus, "GPUS", m_gpus)
                           new_per_node -= len(m_cpus)
                           run_names.remove(vic_name)
                       else:
                           print("Stopping the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.time())
                           m_cpus, m_gpus = self.consumer.stop_pipeline_runs(pipeline_id, [vic_name])                                    
                           print("Stopped the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.time(), "CPUS" ,  m_cpus, "GPUS", m_gpus)
                           if vic_name not in run_names:
                               cpus.extend(m_cpus) 
                               gpus.extend(m_gpus)
                           else:
                               run_names.remove(vic_name)
                       #self.pipeline_runs[pipeline_id][vic_name]['model_params'][3] = 0
                       x = len(cpus)
                       r_names.append(vic_name)
                       if len(cpus) >  new_per_node:
                           run_names.append(vic_name)
                           res = len(cpus) - new_per_node
                           runs_params[vic_name] = {'cpus_node':res, 'command':'re-assign'} 
                           break
                   #print("CPUS...", cpus," : ", len(cpus), " NEW_PER_NODE...", new_per_node, flush = True )

               if len(run_names) > 0 and ((self.machine == 'local' and len(cpus) - n_per_node >= new_per_node) or len(cpus) >= new_per_node):
                   #print("Run names  : ", run_names, " CPUS ", len(cpus), " N_PER_NODE ", n_per_node, " REQUIRED ", new_per_node)
                   print("Stopping and restarting the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", runs_params, "  Timestamp : ", time.time())
                   self.consumer.stop_pipeline_runs(pipeline_id, run_names)                                    
                   run_names.extend(runs_names_hold)
                   self.consumer.restart_pipeline_runs(pipeline_id, run_names, runs_params, cpus, gpus)    
                   print("Stopped and restarted the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", runs_params, "  Timestamp : ", time.time())
                   for runs_x in run_names:
                       print("Run name  : ", runs_x, " Last killed updated to ", n_map['N_STEPS'][runs_x])
                       self.pipeline_runs[pipeline_id][runs_x]['last_killed'] =  n_map['N_STEPS'][runs_x]
                   refresh = True
                   time.sleep(120)  
               self.timestamp[pipeline_id] = datetime.datetime.now()
        if dereg == True:
            self._deregister_pipeline(pipeline_id)
        sys.stdout.flush()
        return refresh

    def find_victim(self, pipeline_id, r_names, pr):
        priority = self.pipeline_priority[pipeline_id]
        order_runs = [] 
        #print("Getting a victim to kill...", priority) 
        n_p = len(priority.keys())
        sorted_keys = sorted(priority.keys(), reverse=True) 
        no_victim = 0
        #print("Sorted keys are...", sorted_keys)
        pr1 = pr
        for p in sorted_keys:
            if p >= pr:
                continue
            pr1 = p
            runs = priority[p]
            #print("Looking for runs...", runs)
            found = 1 
            for run in  r_names:
                if run in runs:
                    found = 0
                    order_runs.append(run)
                    no_victim = 1
                    break
            if found == 1:
                #print("return values", runs, order_runs, pr1, flush = True) 
                return runs, order_runs, pr1
            if no_victim == 1:
                break
        #print("return values", [], order_runs, pr1, flush = True) 
        return [], order_runs, pr1

    def get_machine_cpu(self):
        if self.machine == 'local':
            return list(range(15))
        elif self.machine == 'deepthought2_cpu': 
            return list(range(20))
        elif self.machine == 'summit': 
            return list(range(44))
        else:
            return []

    def _sender(self):   
        keep_requesting = True 
        stop = False
        time.sleep(60) 

        #print("Sender: Starting....", flush = True) 
        while keep_requesting == True:
            #check if there is a stop request :: ....
            try:
                with self.send_cond:
                    if self.stop_send == True:
                        keep_requesting == False 
                        #print("Sender: Signing off...")
                        keep_requesting == False 
                        #print("Sender: Signing off...")
                        continue

                
                #print("Checking queued requests:")
                with self.msg_cond:
                    print(len(self.msg_queue))
                    while len(self.msg_queue) > 0:
                        msg = self.msg_queue[0]
                        self.msg_queue.remove(msg)
                        print("Recieved from runtime monitor : ", msg)
                        sys.stdout.flush()
                        refresh = self._decode_and_inact(msg)
                        if refresh == True:
                            self.msg_queue.clear()
                with self.pipeline_cond: 
                    for id in self.active_pipelines:    
                         model = self.pipeline_models[id]
                         socket = None 
                         if self.pipeline_sockets[id] is None:
                             if self.pipeline_socket_ip[id] is not None:
                                 port = self.pipeline_socket_port[id] 
                                 ip = self.pipeline_socket_ip[id]
                                 self.pipeline_sockets[id] = self._open_sender_connections(ip, port)
                             else:
                                 continue  
                         socket = self.pipeline_sockets[id]
                         request = self._create_request(model, datetime.datetime.now() - self.starttime , "req:get_update")
                         #print("Sending request ", request, " to pipeline :", id)
                         socket.send_string(request)
                         message = socket.recv()
                         #print("Received ack msg : ", message)
                         sys.stdout.flush()
                time.sleep(30) 
            except Exception as e:
                 print("Sender : Got an exception ", e)  
 
    def _register_pipeline(self, pipeline, rmonitor, model, dag, restart_steps, run_map, run_priority, run_priority_rev):
        with self.pipeline_cond:
            self.pipelines_oport[self.cur_oport] = pipeline.id 
            self.pipelines[pipeline.id] = pipeline
            self.monitors[pipeline.id] = rmonitor
            self.pipeline_socket_port[pipeline.id] = self.cur_oport 
            self.pipeline_sockets[pipeline.id] = None
            self.pipeline_socket_ip[pipeline.id] = None
            self.pipeline_models[pipeline.id] = model
            self.pipeline_dag[pipeline.id] = dag
            self.pipeline_restart[pipeline.id] = restart_steps 
            self.pipeline_runs[pipeline.id] = run_map 
            self.pipeline_priority[pipeline.id] = run_priority 
            self.pipeline_rev_priority[pipeline.id] = run_priority_rev 
            self.active_pipelines.append(pipeline.id) 
            self.active_pipelines_runs[pipeline.id] = []
            self.timestamp[pipeline.id] = datetime.datetime.now()
 
    def _deregister_pipeline(self, pipeline_id):
        with self.pipeline_cond:
            self.active_pipelines.remove(pipeline_id)
      
    def process_pipeline(self, pipeline):
        adios2_strs = []
        adios2_engs = []
        monitor = False
        rmonitor = None
        rmon_pos = 0
        run_map = {}
        run_priority = {}
        run_priority_rev = {}
        run_map[pipeline.id] = {}
        tau_fname = "tau_metrics"
        tau_ftype = "trace" 
        launch_mode = pipeline.launch_mode #what changes if job is MPMD??
        onefile = 0 
        hclib='papi'
        '''
        print("OS environment : ", os.environ.keys())
        tau_fname = os.environ.get("TAU_ADIOS2_FILENAME", "tau-metrics") 
        onefile = int(os.environ.get("TAU_ADIOS2_ONE_FILE", 0))
        metrics = os.environ.get("TAU_METRICS", "")
        trace = os.environ.get("TAU_TRACE", 1)
        profile = os.environ.get("TAU_PROFILE", 0)
        adios2_eng = os.environ.get("TAU_ADIOS2_ENGINE", "BPFile")
        '''
        workflow_dagfile = os.environ.get("SAVANNA_WORKFLOW_FILE", "")
        workflow_model = os.environ.get("SAVANNA_MONITOR_MODEL", "")
        workflow_restart = int(os.environ.get("SAVANNA_RESTART_PIPELINE", 0))
        separate_dirs = int(os.environ.get("SAVANNA_SEPARATE_DIRS", 0))
        workflow_restart_steps = -1
        if workflow_restart != 0:
            #print("Setting restart")
            pipeline.restart = True
            workflow_restart_steps = int(os.environ.get("SAVANNA_RESTART_STEPS", 0))
        pipeline_dag1, pipeline_dag2 = DynamicUtil.generate_dag(workflow_dagfile, pipeline.working_dir)
        i = -1
        runs_map = {}
        for run in pipeline.runs:
            self.machine = run.machine.name
            print("Machine .... ", self.machine, flush = True )
            adios2_str = ""
            i += 1
            if run.name == "rmonitor":
                monitor = True
                rmon_pos = i
                rmonitor = run
                continue 

            if run.name not in pipeline_dag1.keys():
                pipeline_dag1[run.name]= {}

            if run.name not in pipeline_dag2.keys():
                pipeline_dag2[run.name]= {}

            eng = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_ENG", "None")
            stream_file = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_STREAM", "None")
            params = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_MPARAMS", "")
            workflow_model = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_MODEL", "")
            hold_job = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_HOLD", "0")
            priority = int(DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_PRIORITY", "1"))
           
            if workflow_model == "memory":
                metrics = DynamicUtil.get_env(run.env, "TAU_METRICS", "")
                if 'PAPI' in metrics:
                    hclib='papi'
                elif 'LIKWID' in metrics:
                    hclib='likwid'
                
            if stream_file != "None":
                if separate_dirs == 1:
                    stream_file="../" + run.name + "/" + stream_file               
                run.monitor['stream_eng'] = eng 
                run.monitor['stream_nm'] = stream_file 
                run.monitor['model_params'] = params.split(',')
                run.monitor['last_killed'] = 0
                #print(params) 

            if int(hold_job) == 1:
                run.hold = True

            run.grace_kill = True #False 

            runs_map[run.name] = run.monitor

            if priority not in run_priority.keys():
                run_priority[priority] = [] 

            run_priority_rev[run.name] = priority 
            run_priority[priority].append(run.name)  

        if not monitor:
           return pipeline              
        

        run = rmonitor 
        if run.name == "rmonitor":
            self.run_map[pipeline.id] = run_map[pipeline.id]
            args=[]
            if run.args is not None:
                args = run.args
            self.cur_oport += 2
            #print("Original args : ", args)        
            index = DynamicUtil.get_index(args, "--bind_inport")
            if index != -1:
                args[index+1] = str(self.cur_oport)
            else:
                args.extend(['--bind_inport', str(self.cur_oport)])
            index = DynamicUtil.get_index(args, "--bind_outaddr") 
            if index != -1:
                args[index+1] = str(soc.gethostbyname(soc.gethostname()))
            else:
                args.extend(['--bind_outaddr', str(soc.gethostbyname(soc.gethostname()))])
            index = DynamicUtil.get_index(args, "--bind_outport") 
            if index != -1:
                args[index+1] = str(self.recv_port)
            else:
                args.extend(['--bind_outport', str(self.recv_port)])
            ''' 
            if onefile:
                index = DynamicUtil.get_index(args, "--tau_one_file") 
                if index == -1:
                    args.extend(['--tau_one_file'])
            index = DynamicUtil.get_index(args, "--tau_file_type") 
            if index != -1:
                args[index+1] = str(tau_ftype)
            else:
                args.extend(['--tau_file_type', str(tau_ftype)])
            '''  
            index = DynamicUtil.get_index(args, "--hc_lib") 
            if index != -1:
                args[index+1] = str(hclib)
            else:
                args.extend(['--hc_lib', str(hclib)])

            index =  DynamicUtil.get_index(args, '--model') 
            if index != -1:
                args[index+1] = str(workflow_model)
            else:
                args.extend(['--model', str(workflow_model)])
            print("Model set:", workflow_model)
            run.args = args
            pipeline.runs[rmon_pos] = run
            pipeline_dag = {}
            pipeline_dag["child_dag"] = pipeline_dag1
            pipeline_dag["parent_dag"] = pipeline_dag2
            self._register_pipeline(pipeline, run, workflow_model, pipeline_dag, workflow_restart_steps, runs_map, run_priority, run_priority_rev)
        return pipeline

    def start(self):
        self.receiver_thread = threading.Thread(target=self._receiver)  
        self.receiver_thread.start()
        self.decision_thread = threading.Thread(target=self._sender)  
        self.decision_thread.start()

    def stop(self):
        with self.send_cond:
            self.stop_send = True
 
        with self.recv_cond:
            self.stop_recv = True 

        self.decision_thread.join()      
        self.receiver_thread.join()      

