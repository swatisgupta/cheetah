from codar.cheetah import Campaign
from codar.cheetah import parameters as p
from codar.savanna.machines import DTH2CPUNode
from codar.cheetah.parameters import SymLink
import copy
import math 

class XGC1XCA(Campaign):
    # A name for the campaign
    name = "xgc1_xgca"

    # Define your workflow. Setup the applications that form the workflow.
    # exe may be an absolute path.
    # The adios xml file is automatically copied to the campaign directory.
    # 'runner_override' may be used to launch the code on a login/service node as a serial code
    #   without a runner such as aprun/srun/jsrun etc.
    codes = [ ("xgc1", dict(exe="run-xgc1.sh", adios_xml_file='adios2cfg.xml', runner_override=False)),
              ("xgca", dict(exe="run-xgca.sh", adios_xml_file='adios2cfg.xml', runner_override=False)),
              ("restart1", dict(exe="restart.sh", runner_override=True)),
              ("rmonitor", dict(exe="/lustre/ssinghal/Dynamic_workflow_management/rmonitor/bin/r_monitor", runner_override=True)),
            ]
 
    # List of machines on which this code can be run
    supported_machines = ['deepthought2_cpu']

    # Kill an experiment right away if any workflow components fail (just the experiment, not the whole group)
    kill_on_partial_failure = True

    # Any setup that you may need to do in an experiment directory before the experiment is run
    run_dir_setup_script = "setup-xgc1-xgca.sh"

    # A post-process script that is run for every experiment after the experiment completes
    run_post_process_script = "post-xgc1-xgca.sh"

    # Directory permissions for the campaign sub-directories
    umask = '027'

    # Options for the underlying scheduler on the target system. Specify the project ID and job queue here.
    scheduler_options = {'deepthought2_cpu':{'project': 'sussman-lab', 'queue': 'scavenger'}}

    # A way to setup your environment before the experiment runs. Export environment variables such as LD_LIBRARY_PATH here.
    app_config_scripts = {'deepthought2_cpu':'setup-deepthought2.sh'}

    nprocs_per_node = 4
    nthreads = 10
    nprocs = 192 
    t_particles=250000 #For small scale run
    #t_particles=100000 #For test run
    #t_particles = 100000 * nprocs * nthreads * 10 * 8 

    """ 
    OUTSETPS_2 MODEL PARAMETERS:
    1.) Initial value to start for counting steps,
    2.) Frequency at which outputsteps are written, 
    3.) Number of steps (M1) to be used for computing stopping creteria ( used along with function defined as paramemter 7.
    4.) Number of digits in Output File
    5.) Input file name to edit parameters for rerun 
    6.) Parameter key to change for rerun
    7.) Function to use as stopping creteria
    8.) Iteration frequency at which to check for stopping the component.
    9.) Maximum steps allowed for this component     
    """
    # Setup the sweep parameters for a Sweep
    sweep1_parameters = [
            # ParamRunner 'nprocs' specifies the no. of ranks to be spawned 
            p.ParamRunner       ('xgc1', 'nprocs', [nprocs]),
            p.ParamEnvVar       ('xgc1', 'openmp', 'OMP_NUM_THREADS', [nthreads]),
            p.ParamEnvVar       ('xgc1', 'savanna_model', 'SAVANNA_MONITOR_MODEL', ['outsetps2']),
            p.ParamEnvVar       ('xgc1', 'savana_params', 'SAVANNA_MONITOR_MPARAMS', ["0, 2, 100, 5, .bp, input, sml_mstep, fx, 2, 100"]),
            p.ParamEnvVar       ('xgc1', 'savana_stream', 'SAVANNA_MONITOR_STREAM', ['restart_dir/xgc.restart.']),
            p.ParamEnvVar       ('xgc1', 'savana_eng', 'SAVANNA_MONITOR_ENG', ['BPFile']),
            p.ParamKeyValue     ('xgc1', 'nphi', 'XGC1_exec/input', 'sml_nphi_total', [2]), #2 or nprocs/192
            p.ParamKeyValue     ('xgc1', 'grid', 'XGC1_exec/input', 'sml_grid_nrho', [2]),  #2 or 6
            p.ParamKeyValue     ('xgc1', 'inputdir', 'XGC1_exec/input', 'sml_input_file_dir', ["'XGC-1_inputs'"]),
            p.ParamKeyValue     ('xgc1', 'nsteps', 'XGC1_exec/input', 'sml_mstep', [100]),
            p.ParamKeyValue     ('xgc1', 'coupling1', 'XGC1_exec/input', 'sml_coupling_xgc1_dir', ["'../xgc1'"]), #2 or nprocs/192
            p.ParamKeyValue     ('xgc1', 'coupling2', 'XGC1_exec/input', 'sml_coupling_xgca_dir', ["'../xgca'"]), #2 or nprocs/192
            p.ParamKeyValue     ('xgc1', 'restart1', 'XGC1_exec/input', 'sml_restart', [".f."]),
            p.ParamKeyValue     ('xgc1', 'step_freq', 'XGC1_exec/input', 'sml_restart_write_period', [2]),
            p.ParamKeyValue     ('xgc1', '1d_diag', 'XGC1_exec/input', 'diag_1d_period', [1]),
            p.ParamKeyValue     ('xgc1', 'num_particles', 'XGC1_exec/input', 'ptl_num', [t_particles]),
            p.ParamKeyValue     ('xgc1', 'sml_monte_num', 'XGC1_exec/input', 'sm_monte__num', [100*t_particles]),
            p.ParamKeyValue     ('xgc1', 'max_particles', 'XGC1_exec/input', 'ptl_maxnum', [5000000]),

            # Sweep over four values for the nprocs 
            p.ParamRunner       ('xgca', 'nprocs', [nprocs]),
            p.ParamEnvVar       ('xgca', 'openmp', 'OMP_NUM_THREADS', [nthreads]),
            p.ParamEnvVar       ('xgca', 'savanna_model', 'SAVANNA_MONITOR_MODEL', ['outsetps2']),
            p.ParamEnvVar       ('xgca', 'savana_params', 'SAVANNA_MONITOR_MPARAMS', ["0, 2, 100, 5, .bp, input, sml_mstep, log2, 2, 100"]), 
            p.ParamEnvVar       ('xgca', 'savana_stream', 'SAVANNA_MONITOR_STREAM', ['restart_dir/xgc.restart.']),
            p.ParamEnvVar       ('xgca', 'savana_eng', 'SAVANNA_MONITOR_ENG', ['BPFile']),
            p.ParamKeyValue     ('xgca', 'nphi', 'XGCa_exec/input', 'sml_nphi_total', [2]), #2 or nprocs/192
            p.ParamKeyValue     ('xgca', 'grid', 'XGCa_exec/input', 'sml_grid_nrho', [2]),  #2 or 6
            p.ParamKeyValue     ('xgca', 'inputdir', 'XGCa_exec/input', 'sml_input_file_dir', ["'XGC-1_inputs'"]),
            p.ParamKeyValue     ('xgca', 'nsteps', 'XGCa_exec/input', 'sml_mstep', [100]),
            p.ParamKeyValue     ('xgca', 'coupling1', 'XGCa_exec/input', 'sml_coupling_xgc1_dir', ["'../xgc1'"]), #2 or nprocs/192
            p.ParamKeyValue     ('xgca', 'coupling2', 'XGCa_exec/input', 'sml_coupling_xgca_dir', ["'../xgca'"]), #2 or nprocs/192
            p.ParamKeyValue     ('xgca', 'step_freq', 'XGCa_exec/input', 'sml_restart_write_period', [2]),
            p.ParamKeyValue     ('xgca', '1d_diag', 'XGCa_exec/input', 'diag_1d_period', [1]),
            p.ParamKeyValue     ('xgca', 'num_particles', 'XGCa_exec/input', 'ptl_num', [t_particles]),
            p.ParamKeyValue     ('xgca', 'max_particles', 'XGCa_exec/input', 'ptl_maxnum', [5000000]),
            p.ParamKeyValue     ('xgca', 'sml_monte_num', 'XGC1_exec/input', 'sm_monte__num', [100*t_particles]),

            p.ParamRunner       ('rmonitor', 'nprocs', [1]),
            p.ParamRunner       ('restart1', 'nprocs', [1]),
    ]

    ncpus_per_proc = math.ceil(nthreads/2)

    xgc1_node1 = DTH2CPUNode()
    for i in range(nprocs_per_node):
        for j in range(ncpus_per_proc):
            xgc1_node1.cpu[i * ncpus_per_proc + j] = "xgc1:{}".format(i)
            print(str(i * ncpus_per_proc + j), " xgc1:{}".format(i))

    xgca_node1 = DTH2CPUNode()
    for i in range(nprocs_per_node):
        for j in range(ncpus_per_proc):
            xgca_node1.cpu[i * ncpus_per_proc + j] = "xgca:{}".format(i)
            print(str(i * ncpus_per_proc + j), " xgca:{}".format(i))

    other_node1 = DTH2CPUNode()
    other_node1.cpu[0] = "restart1:0"
    other_node1.cpu[1] = "rmonitor:0"

    seperate_node_layout1 = [xgc1_node1, xgca_node1, other_node1]

    sweep1 = p.Sweep (parameters = sweep1_parameters, node_layout={'deepthought2_cpu':seperate_node_layout1}, rc_dependency={'xgca':'xgc1', 'restart1':'xgca'})

    # Create a SweepGroup and add the above Sweeps. Set batch job properties such as the no. of nodes, 
    sweepGroup1 = p.SweepGroup ("sg-1", # A unique name for the SweepGroup
                                walltime=3600,  # Total runtime for the SweepGroup
                                #per_run_timeout=400,    # Timeout for each experiment                                
                                parameter_groups=[sweep1],   # Sweeps to include in this group
                                launch_mode='default',  # Launch mode: default, or MPMD if supported
                                nodes=49,  # No. of nodes for the batch job.
                                run_repetitions=0,  # No. of times each experiment in the group must be repeated (Total no. of runs here will be 3)
                                component_subdirs = True, # Codes have their own separate workspace in the experiment directory
                                component_inputs = {'xgc1': ['XGC1_exec/adios.in','XGC1_exec/mon_in', 'XGC1_exec/petsc.rc',  SymLink('/homes/ssinghal/XGC-Devel-xgc1-xgca-coupling/xgc_build/xgc-es'), SymLink('XGC-1_inputs'), 'XGC1_exec/adioscfg.xml'], 
                                                    'xgca': ['XGCa_exec/adios.in','XGCa_exec/mon_in', 'XGCa_exec/petsc.rc',  SymLink('/homes/ssinghal/XGC-Devel-xgc1-xgca-coupling/xgc_build/xgca'), SymLink('XGC-1_inputs'), 'XGCa_exec/adioscfg.xml'],
                                                    }

                                )
    
    # Activate the SweepGroup
    sweeps = [sweepGroup1]

