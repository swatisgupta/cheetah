import math
from codar.cheetah import Campaign
from codar.cheetah import parameters as p
#from codar.savanna.machines import SummitNode
from codar.savanna.machines import DTH2CPUNode
from codar.cheetah.parameters import SymLink
import copy


class GrayScott(Campaign):
    # A name for the campaign
    name = "gray_scott"

    # Define your workflow. Setup the applications that form the workflow.
    # exe may be an absolute path.
    # The adios xml file is automatically copied to the campaign directory.
    # 'runner_override' may be used to launch the code on a login/service node as a serial code
    #   without a runner such as aprun/srun/jsrun etc.
    codes = [ ("simulation", dict(exe="gray-scott", adios_xml_file='adios2.xml', runner_override=False)),
              ("isosurface", dict(exe="isosurface_VTKM", adios_xml_file='adios2.xml', runner_override=False, sleep_after=5)), 
              ("rendering", dict(exe="rendering_VTKM", adios_xml_file='adios2.xml', runner_override=False, sleep_after=10)), 
              ("rmonitor", dict(exe="/lustre/ssinghal/Dynamic_workflow_management/rmonitor/bin/r_monitor", runner_override=True)),
            ] 

    # List of machines on which this code can be run
    supported_machines = ['local', 'deepthought2_cpu', 'summit']

    # Kill an experiment right away if any workflow components fail (just the experiment, not the whole group)
    kill_on_partial_failure = False

    # Any setup that you may need to do in an experiment directory before the experiment is run
    run_dir_setup_script = 'pre-setup.sh'

    # A post-process script that is run for every experiment after the experiment completes
    run_post_process_script = None

    # Directory permissions for the campaign sub-directories
    umask = '027'

    # Options for the underlying scheduler on the target system. Specify the project ID and job queue here.
    scheduler_options = {'deepthought2_cpu': {'project':'sussman-lab', 'queue': 'scavenger'},
                         'summit': {'project':'csc143'}}

    # A way to setup your environment before the experiment runs. Export environment variables such as LD_LIBRARY_PATH here.
    app_config_scripts = {'local':'setup.sh', 'deepthought2_cpu': 'setup.sh', 'summit':'setup.sh'}

    # Setup the sweep parameters for a Sweep
    sweep1_parameters = [
            # ParamRunner 'nprocs' specifies the no. of ranks to be spawned 
            p.ParamRunner       ('simulation', 'nprocs', [12]),

            # Create a ParamCmdLineArg parameter to specify a command line argument to run the application
            p.ParamCmdLineArg   ('simulation', 'settings', 1, ["settings.json"]),
            # Edit key-value pairs in the json file
            # Sweep over two values for the F key in the json file.
            p.ParamConfig       ('simulation', 'feed_rate_U', 'settings.json', 'F', [0.01]),
            p.ParamConfig       ('simulation', 'kill_rate_V', 'settings.json', 'k', [0.048]),
            p.ParamConfig       ('simulation', 'domain_size', 'settings.json', 'L', [512]),
            p.ParamConfig       ('simulation', 'num_steps', 'settings.json', 'steps', [100]),
            p.ParamConfig       ('simulation', 'plot_gap', 'settings.json', 'plotgap', [2]),

            # Setup an environment variable
            p.ParamEnvVar       ('simulation', 'openmp', 'OMP_NUM_THREADS', [1]),
            p.ParamEnvVar       ('simulation', 'savanna_model', 'SAVANNA_MONITOR_MODEL', ['outsetps1']),
            p.ParamEnvVar       ('simulation', 'savana_params', 'SAVANNA_MONITOR_MPARAMS', ["step, 50, .bp"]),
            p.ParamEnvVar       ('simulation', 'savana_stream', 'SAVANNA_MONITOR_STREAM', ['gs.bp']),
            p.ParamEnvVar       ('simulation', 'savana_eng', 'SAVANNA_MONITOR_ENG', ['SST']),
            p.ParamEnvVar       ('simulation', 'savana_priority', 'SAVANNA_MONITOR_PRIORITY', [1]),

            # Change the engine for the 'SimulationOutput' IO object in the adios xml file to SST for coupling.
            # As both the applications use the same xml file, you need to do this just once.
            p.ParamADIOS2XML    ('simulation', 'SimulationOutput', 'engine', [ {'SST':{}} ]),

            # Now setup options for the pdf_calc application.
            # Sweep over four values for the nprocs 
            p.ParamRunner       ('isosurface', 'nprocs', [2]),
            p.ParamCmdLineArg   ('isosurface', 'infile', 1, ['../simulation/gs.bp']),
            p.ParamCmdLineArg   ('isosurface', 'outfile', 2, ['iso.bp']),
            p.ParamCmdLineArg   ('isosurface', 'isoval', 3, [0.7]),
            p.ParamEnvVar       ('isosurface', 'savanna_model', 'SAVANNA_MONITOR_MODEL', ['outsetps1']),
            p.ParamEnvVar       ('isosurface', 'savana_params', 'SAVANNA_MONITOR_MPARAMS', ["step, 30, .bp"]),
            p.ParamEnvVar       ('isosurface', 'savana_stream', 'SAVANNA_MONITOR_STREAM', ['iso.bp']),
            p.ParamEnvVar       ('isosurface', 'savana_eng', 'SAVANNA_MONITOR_ENG', ['SST']),
            p.ParamEnvVar       ('isosurface', 'savana_priority', 'SAVANNA_MONITOR_PRIORITY', [2]),

            p.ParamRunner       ('rendering', 'nprocs', [1]),
            p.ParamCmdLineArg   ('rendering', 'infile', 1, ['../isosurface/iso.bp']),
            p.ParamCmdLineArg   ('rendering', 'outfile', 2, ['out.bp']),
            p.ParamEnvVar       ('rendering', 'savanna_model', 'SAVANNA_MONITOR_MODEL', ['outsetps1']),
            p.ParamEnvVar       ('rendering', 'savana_params', 'SAVANNA_MONITOR_MPARAMS', ["step, 30, .bp"]),
            p.ParamEnvVar       ('rendering', 'savana_stream', 'SAVANNA_MONITOR_STREAM', ['out.bp']),
            p.ParamEnvVar       ('rendering', 'savana_eng', 'SAVANNA_MONITOR_ENG', ['BPFile']),
            p.ParamEnvVar       ('rendering', 'savana_priority', 'SAVANNA_MONITOR_PRIORITY', [2]),

            p.ParamRunner       ('rmonitor', 'nprocs', [1]),

    ]

    #shared_node = SummitNode()
    #for i in range(32):
    #    shared_node.cpu[i] = "simulation:{}".format(i)
    #for i in range(6):
    #    shared_node.cpu[i + 32] = "isosurface:{}".format(i)
    #for i in range(2):
    #    shared_node.cpu[i + 38] = "rendering:{}".format(i)  
    #shared_node_layout = [shared_node]


    dthought2_node = DTH2CPUNode()

    for i in range(12):
        dthought2_node.cpu[i] = "simulation:{}".format(i)
    for i in range(2):
        dthought2_node.cpu[i + 12] = "isosurface:{}".format(i)
    for i in range(1):
        dthought2_node.cpu[i + 14] = "rendering:{}".format(i)

    dthought2_node.cpu[15] = "rmonitor:0"

    dth2_layout = [dthought2_node]
  
    # a Sweep object. This one does not define a node-layout, and thus, all cores of a compute node will be 
    #   utilized and mapped to application ranks.
    sweep1 = p.Sweep (parameters = sweep1_parameters, node_layout={'deepthought2_cpu':dth2_layout})
                      # rc_dependency={'pdf_calc':'simulation',}, # Specify dependencies between workflow components
    #sweep2 = p.Sweep (parameters = sweep1_parameters, node_layout={'local':[{'simulation':5, 'isosurface':2, 'rendering':1, 'rmonitor':1}]})

    # Create a SweepGroup and add the above Sweeps. Set batch job properties such as the no. of nodes, 
    sweepGroup1 = p.SweepGroup ("sg-1", # A unique name for the SweepGroup
                                walltime=3600,  # Total runtime for the SweepGroup
                                per_run_timeout=600,    # Timeout for each experiment                                
                                parameter_groups=[sweep1],   # Sweeps to include in this group
                                launch_mode='default',  # Launch mode: default, or MPMD if supported
                                nodes=1,  # No. of nodes for the batch job.
                                run_repetitions=0,  # No. of times each experiment in the group must be repeated (Total no. of runs here will be 3)
                                component_subdirs = True, # Codes have their own separate workspace in the experiment directory
                                )
    
    # Activate the SweepGroup
    sweeps = [sweepGroup1]
