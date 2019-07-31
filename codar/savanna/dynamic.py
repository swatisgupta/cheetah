
#!/usr/bin/env python3

#from mpi4py import MPI
import os
import threading
import socket as soc
import zmq
import datetime
import json
from datetime import datetime
import time 
import codar.savanna.producer
import codar.savanna.consumer
from codar.savanna.dynamic_util import DynamicUtil

class DynamicControls():
    
    def __init__(self, consumer):
        self.recv_port = 8080
        self.cur_oport = 8085
        self.starttime = datetime.now()
        self.reciever_thread = None  
        self.decision_thread = None 
        self.recv_cond = threading.Condition()
        self.send_cond = threading.Condition()
        self.pipeline_cond = threading.Condition()
        self.msg_queue = []
        self.queue_lock = threading.Condition()
        self.stop_recv = False
        self.stop_send = False
        self.recv_socket = None
        self.pipelines_oport = {}
        self.pipelines = {}
        self.monitors = {}
        self.active_pipelines = {}
        self.pipeline_sockets = {}
        self.pipeline_models = {}
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
        print("Running receiver....")
    
        while keep_alive == True:
            with self.recv_cond:
                if self.stop_recv == True:
                   keep_alive = False
                   continue
            try:
                message = self.recv_socket.recv()
                self.recv_socket.send_string("OK")
                message = message.decode("utf-8") 
                print("Received a critical update from monitor : ", message)
                if message == "done":
                    print("got stop request") 
                with self.queue_lock:
                     msg_queue.append(message) 
            except:
                keep_alive = False
                continue 
  
    def _open_sender_connections(self, port):
        address = soc.gethostbyname(soc.gethostname())
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket_str = "tcp://" + address + ":" + str(port)
        socket.connect(socket_str)
        print("Opened socket : ", socket_str)
        return socket

    def _sender(self):   
        keep_requesting = True 
        stop = False
        print("Running sender....")
        while keep_requesting == True:
            #check if there is a stop request :: ....
            with self.send_cond:
                if self.stop_send == True:
                    keep_requesting == False 
                    continue

            #check pending requests :: ...
            with self.queue_lock:
                 while len(self.msg_queue) != 0:
                     msg = self.msg_queue.pop(0)
                     print("Found a critical update from monitor in queue : ", message)
                     #decode message and take action if required!!!

            #get updates from the active pipelines :: ....
            with self.pipeline_cond: 
                for id in self.active_pipelines:    
                    model = self.pipeline_models[id]
                    socket = self.pipeline_socket[id]
                    request = create_request(model, datetime.now() - starttime , "req:get_update")
                    print("Sending request ", request, " to pipeline :", id)
                    socket.send_string(request)
                    message = socket.recv()
                    #decode message and take action if required!!!
                    message = message.decode("utf-8")
                    print("Received response : ", message)

    def _register_pipeline(self, pipeline, rmonitor, model):
        with  self.pipeline_cond:
            self.pipelines_oport[self.cur_oport] = pipeline.id 
            self.pipelines[pipeline.id] = pipeline
            self.monitors[pipeline.id] = rmonitor
            self.pipeline_sockets[pipeline.id] = self._open_sender_connections(self.cur_oport) 
            self.pipeline_models[pipeline.id] = model
                  
    def process_pipeline(self, pipeline):
        adios2_strs = []
        adios2_engs = []
        monitor = False
        rmonitor = None
        rmon_pos = 0
        run_map = {}
        run_map[pipeline.id] = {}
        tau_fname = "tau_metrics"
        tau_ftype = "trace" 
        launch_mode = pipeline.launch_mode #what changes if job is MPMD??
        onefile = 0 
        hclib='papi'
        print("OS environment : ", os.environ.keys())
        tau_fname = os.environ.get("TAU_ADIOS2_FILENAME", "tau-metrics") 
        onefile = int(os.environ.get("TAU_ADIOS2_ONE_FILE", 0))
        metrics = os.environ.get("TAU_METRICS", "")
        trace = os.environ.get("TAU_TRACE", 1)
        profile = os.environ.get("TAU_PROFILE", 0)
        adios2_eng = os.environ.get("TAU_ADIOS2_ENGINE", "BPFile")
        if 'PAPI' in metrics:
            hclib='papi'
        elif 'LIKWID' in metrics:
            hclib='likwid'
        i = -1
        for run in pipeline.runs:
            adios2_str = ""
            i += 1
            if run.name == "rmonitor":
                monitor = True
                rmon_pos = i
                rmonitor = run
                continue
            ''' 
            adios2_eng = ""
            tau_fname = DynamicUtil.get_env(run.env, "TAU_ADIOS2_FILENAME", "tau-metrics") 
            onefile = int(DynamicUtil.get_env(run.env, "TAU_ADIOS2_ONE_FILE", 0))
            metrics = DynamicUtil.get_env(run.env, "TAU_METRICS")
            trace = DynamicUtil.get_env(run.env, "TAU_TRACE")
            profile = DynamicUtil.get_env(run.env, "TAU_PROFILE")
            adios2_eng = DynamicUtil.get_env(run.env,"TAU_ADIOS2_ENGINE", "BPFile")
            adios2_str = run.name + "/" + DynamicUtil.get_env(run.env, "TAU_ADIOS2_FILENAME", "tau-metrics") + ".bp"
            '''
            adios2_str = "../" + run.name + "/" + tau_fname + ".bp"
            if adios2_str is not "":
                adios2_str = adios2_str
                adios2_strs.append(adios2_str)
                adios2_engs.append(adios2_eng)
                run_map[pipeline.id][adios2_str] = run.name

        if not monitor:
           return pipeline              
        

        #look into the run directory for any files on decisions..
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
            if onefile:
                index = DynamicUtil.get_index(args, "--tau_one_file") 
                if index == -1:
                    args.extend(['--tau_one_file'])
            index = DynamicUtil.get_index(args, "--tau_file_type") 
            if index != -1:
                args[index+1] = str(tau_ftype)
            else:
                args.extend(['--tau_file_type', str(tau_ftype)])
            index = DynamicUtil.get_index(args, "--hc_lib") 
            if index != -1:
                args[index+1] = str(hclib)
            else:
                args.extend(['--hc_lib', str(hclib)])

            #ideally we want to set a default model if user didn't specify one
            if DynamicUtil.get_index(args, '--memory') == -1:
                args.append('--memory')
             #can have an else case where user can specify certain configurations for decision language 
                
            args.append('--adios2_streams')
            args.extend(adios2_strs) 
            args.append('--adios2_stream_engs')
            args.extend(adios2_engs)
            #rfile_json = DynamicUtil.generate_rfile(pipeline, tau_fname)
            #rfile = run.working_dir + "/" + "res_map.js"
            #with open(rfile, 'w') as fp
            #    json.dump(rfile_json, fp)
            #args.append('--rmap_file', rfile)
            print("New args : ", args)        
            run.args = args
            pipeline.runs[rmon_pos] = run
            self._register_pipeline(pipeline, run, 'memory')
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

