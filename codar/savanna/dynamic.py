
#!/usr/bin/env python3

#from mpi4py import MPI
import os
import threading
import socket as soc
import zmq
import datetime
import sys
import json
from datetime import datetime
import time 
import codar.savanna.producer
import codar.savanna.consumer
from codar.savanna.dynamic_util import DynamicUtil
from queue import Queue 


class DynamicControls():
    
    def __init__(self, consumer):
        self.recv_port = 8080
        self.cur_oport = 8085
        self.cur_lport = 8086
        self.starttime = datetime.now()
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
        self.pipeline_sockets = {}
        self.pipeline_socket_port = {}
        self.pipeline_models = {}
        self.pipeline_restart = {}
        self.pipeline_runs = {} 
        self.pipeline_dag = {}
        self.pipeline_priority = {}
        self.consumer = consumer
        self.run_map = {}

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
                #print("Received a critical update from monitor : ", message)
                self.recv_socket.send_string("OK")
                #print("Send ack : OK")
                sys.stdout.flush()
                message = json.loads(message)

                with self.msg_cond:
                    self.msg_queue.append(message) 

            except Exception as e:
                print("Reciever : Got exception...", e)
                sys.stdout.flush() 
  
    def _open_sender_connections(self, port):
        address = soc.gethostbyname(soc.gethostname())
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket_str = "tcp://" + address + ":" + str(port)
        socket.connect(socket_str)
        print("Opened socket : ", socket_str)
        return socket

    def _decode_and_inact(self, message):
        port = message["socket"]
        model = message["model"]
        state = message["message"]
        timestamp =  message["timestamp"]
        dereg = False
        pipeline_id = -1
        if model == "outsteps2":
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
                   return
               restart_steps = int(self.pipeline_restart[pipeline_id])
               #print("Current steps ", r_steps,  " : Terminate after ", restart_steps)
               if r_steps != -1 and r_steps >= restart_steps:
                   print("Stopping the pipeline: ", pipeline_id, " Timestamp : ", time.now())
                   self.consumer.set_pipeline_restart(pipeline_id, False)
                   self.consumer.stop_pipeline_all(pipeline_id)
                   print("Done stopping the pipeline: ", pipeline_id, " Timestamp : ", time.now())
                   dereg = True
               else: 
                   run_names = []
                   run_p = {}
                   run_map = self.pipeline_runs[pipeline_id] 
                   for run in run_map.keys():
                       r_params = run_map[run]
                       #print("Parameters for ", run, "are ", r_params) 
                       if r_params: 
                           run_cond = int(r_params['model_params'][2])
                           input_file = r_params['model_params'][5]
                           key = r_params['model_params'][6]
                           step_fn = r_params['model_params'][7]
                           ch_iter = int(r_params['model_params'][8])
                           if niter % ch_iter == 0 and n_map['STEPS'][run] >= run_cond:
                               run_names.append(run)
                               if step_fn == 'log2':
                                   run_cond = math.log2(run_cond)
                               elif step_fn == 'log':                    
                                   run_cond = math.log(run_cond)
                               self.pipeline_runs[pipeline_id][run]['model_params'][2] = run_cond 
                           run_p[run] = {'nstep' : [input_file, key, run_cond]}
                   if len(run_names) > 0:
                       print("Stopping the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", run_p, "  Timestamp : ", time.now())
                       self.consumer.stop_pipeline_runs(pipeline_id, run_names, run_p) 
                       print("Stopped the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", run_p, "  Timestamp : ", time.now())
        if model == "outsteps1":
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
                   return
               runs_names_inc = []
               runs_params = {}
               runs_names_dec = []
               run_map = self.pipeline_runs[pipeline_id] 
               dag = self.pipeline_dag[pipeline.id]
               for run in run_map.keys():
                   r_params = run_map[run]
                   #print("Parameters for ", run, "are ", r_params) 
                   if r_params: 
                       expected_steptime = int(r_params['model_params'][1])
                       if n_map['AVG_STEP_TIME'][run] != 0 and n_map['AVG_STEP_TIME'][run] >= 2 * expected_steptime:
                           print("Adding run ",run , " to inc set") 
                           runs_names_inc.append(run)
                           runs_params[run] = {'cpus_node':'2', 'command':'add'}
                           new_per_node += 2
                       elif n_map['AVG_STEP_TIME'][run] != 0 and n_map['AVG_STEP_TIME'][run] < 0.5 * expected_steptime:
                           print("Adding run ", run , " to dec set") 
                           runs_names_dec.append(run)
                           runs_params[run] = {'cpus_node':'2', 'command':'del'}
                           new_per_node -= 2
               run_names = runs_names_inc
               run_names.extend(runs_names_dec)    
               if len(run_names) > 0:
                   dep_runs = []
                   for run in run_names:
                       if run in dag.keys():
                           for dep in dag.keys():
                               if dep not in run_names: 
                                   dep_runs.append(dep)
                   n_per_node, m_cpus, m_gpus = self.consumer.get_active_cres(pipeline_id, run_names, 0) 
                   t_per_node, u_cpus, u_gpus = self.consumer.get_active_cres(pipeline_id, run_names)
                   new_per_node += n_per_node
                   t_cpus = self.get_machine_cpu()
                   cpus = [i for i in  u_cpus + m_cpus if i not in u_cpus and i not in m_cpus]  
                   cpus = [i for i in  cpus + t_cpus if i not in cpus and i not in t_cpus]  
                   cpus.extend(m_cpus)
                   new_per_node += n_per_node
                   gpus = m_gpus

               r_names = run_names 
               while self.machine != 'local' and len(cpus) < new_per_node:
                   vic_names = self.find_victim(pipeline_id, r_names)
                   if len(vic_names) == 0:
                       print('Cannot restart runs with new params')
                       break 
                   for vic_name in vic_names:
                       print("Stopping the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.now())
                       m_cpus, m_gpus = self.consumer.stop_pipeline_runs(pipeline_id, [vic_name])                                    
                       print("Stopped the pipeline : ", pipeline_id, " run(victim)  : ", vic_name, "  Timestamp : ", time.now())

                       cpus.extend(m_cpus) 
                       gpus.extend(m_gpus)
                       r_names.append(vic_name)

                       if len(cpus) >= new_per_node:
                           break
               if len(run_names) > 0 and ((self.machine == 'local' and len(cpus) - n_per_node >= new_per_node) or len(cpus) >= new_per_node):
                   print("Run names  : ", run_names, " CPUS ", len(cpus), " N_PER_NODE ", n_per_node, " REQUIRED ", new_per_node)
                   print("Stopping and restarting the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", runs_params, "  Timestamp : ", time.now())
                   self.consumer.stop_pipeline_runs(pipeline_id, run_names)                                    
                   self.consumer.restart_pipeline_runs(pipeline_id, run_names, runs_params, cpus, gpus)    
                   print("Stopped and restarted the pipeline : ", pipeline_id, " runs : ", run_names, " with params ", runs_params, "  Timestamp : ", time.now())

        if dereg == True:
            self._deregister_pipeline(pipeline_id)
        sys.stdout.flush()
        

    def find_victim(self, pipeline_id, r_names):
        priority = self.pipeline_priority[pipeline_id]
        print("Getting a victim to kill...", priority) 
        n_p = len(priority.keys())
        sorted_keys = sorted(priority.keys(), reverse=True) 
        for p in sorted_keys:
            runs = priority[p]
            found = 1
            for run in  r_names:
                if run in runs:
                    found = 0
                    break
            if found == 1:
                return runs
        return []

    def get_machine_cpu(self):
        if self.machine == 'local':
            return range(15)
        elif self.machine == 'deepthought2_cpu': 
            return range(20)
        elif self.machine == 'summit': 
            return range(44)
        else:
            return []

    def _sender(self):   
        keep_requesting = True 
        stop = False

        print("Running sender....")
        while keep_requesting == True:
            #check if there is a stop request :: ....
            try:
                with self.send_cond:
                    if self.stop_send == True:
                        keep_requesting == False 
                        print("Sender: Signing off...")
                        continue

                #print("Checking queued requests")
                with self.msg_cond:
                    while len(self.msg_queue) > 0:
                        msg = self.msg_queue[0]
                        self.msg_queue.remove(msg)
                        print("Recieved from runtime monitor : ", msg)
                        self._decode_and_inact(msg)

                with self.pipeline_cond: 
                    for id in self.active_pipelines:    
                         model = self.pipeline_models[id]
                         socket = None 
                         if self.pipeline_sockets[id] is None:
                             port = self.pipeline_socket_port[id] 
                             self.pipeline_sockets[id] = self._open_sender_connections(port) 
                         socket = self.pipeline_sockets[id]
                         request = self._create_request(model, datetime.now() - self.starttime , "req:get_update")
                         #print("Sending request ", request, " to pipeline :", id)
                         socket.send_string(request)
                         message = socket.recv()
                         #print("Received ack msg : ", message)
                         sys.stdout.flush()
                time.sleep(2) 
            except Exception as e:
                 print("Sender : Got an exception ", e)  
 
    def _register_pipeline(self, pipeline, rmonitor, model, dag, restart_steps, run_map, run_priority):
        with self.pipeline_cond:
            self.pipelines_oport[self.cur_oport] = pipeline.id 
            self.pipelines[pipeline.id] = pipeline
            self.monitors[pipeline.id] = rmonitor
            self.pipeline_socket_port[pipeline.id] = self.cur_oport 
            self.pipeline_sockets[pipeline.id] = None
            self.pipeline_models[pipeline.id] = model
            self.pipeline_dag[pipeline.id] = dag
            self.pipeline_restart[pipeline.id] = restart_steps 
            self.pipeline_runs[pipeline.id] = run_map 
            self.pipeline_priority[pipeline.id] = run_priority 
            self.active_pipelines.append(pipeline.id) 
      
    def _deregister_pipeline(self, pipeline_id):
        with self.pipeline_cond:
            self.active_pipelines.remove(pipeline.id)
      
    def process_pipeline(self, pipeline):
        adios2_strs = []
        adios2_engs = []
        monitor = False
        rmonitor = None
        rmon_pos = 0
        run_map = {}
        run_priority = {}
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
        workflow_model = os.environ.get("SAVANNA_MONITOR_MODEL", "outsetps2")
        workflow_restart = int(os.environ.get("SAVANNA_RESTART_PIPELINE", 0))
        workflow_restart_steps = -1
        if workflow_restart != 0:
            print("Setting restart")
            pipeline.restart = True
            workflow_restart_steps = int(os.environ.get("SAVANNA_RESTART_STEPS", 0))
        pipeline_dag = DynamicUtil.generate_dag(workflow_dagfile, pipeline.working_dir)
        
        i = -1
        runs_map = {}
        for run in pipeline.runs:
            self.machine = run.machine
            adios2_str = ""
            i += 1
            if run.name == "rmonitor":
                monitor = True
                rmon_pos = i
                rmonitor = run
                continue 

            if run.name not in pipeline_dag.keys():
                pipeline_dag[run.name]= {}

            eng = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_ENG", "None")
            stream_file = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_STREAM", "None")
            params = DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_MPARAMS", "")
            priority = int(DynamicUtil.get_env(run.env, "SAVANNA_MONITOR_PRIORITY", "1"))
            if workflow_model == "memory":
                metric = DynamicUtil.get_env(run.env, "TAU_METRICS", "")
                if 'PAPI' in metrics:
                    hclib='papi'
                elif 'LIKWID' in metrics:
                    hclib='likwid'

            if stream_file != "None":
                stream_file="../" + run.name + "/" + stream_file               
                run.monitor['stream_eng'] = eng 
                run.monitor['stream_nm'] = stream_file 
                run.monitor['model_params'] = params.split(',')
                print(params) 

            run.grace_kill = True 

            runs_map[run.name] = run.monitor

            if priority not in run_priority.keys():
                run_priority[priority] = [] 

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
            print("Original args : ", args)        
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
            self._register_pipeline(pipeline, run, workflow_model, pipeline_dag, workflow_restart_steps, runs_map, run_priority)
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

