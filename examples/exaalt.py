from datetime import timedelta

from codar.cheetah import Campaign
from codar.cheetah import parameters as p

class Exaalt(Campaign):
    name = "Exaalt"
    codes = [("stage_write", dict(exe="stage_write/stage_write")),
             ("exaalt", dict(exe="pt_producer_global"))]

    # Note that titan has 16 processes per node
    supported_machines = ['titan']

    project = "PROJECT123"
    queue = "batch"

    kill_on_partial_failure = True

    inputs = ["states_list.txt"]

    sweeps = [
      # Submit everything as a single job, to avoid queuing delay.
      # Titan allows 12 hour walltime for jobs of 313-3,749 nodes in
      # the batch queue.
      p.SweepGroup(nodes=384,
                   walltime=timedelta(hours=12),
                   per_run_timeout=timedelta(hours=1),
                   parameter_groups=[
        p.Sweep([
          # 256 exaalt nodes
          p.ParamRunner("exaalt", "nprocs", [4096]),
          p.ParamCmdLineArg("exaalt", "states_list_file", 1,
                            ["states_list.txt"]),
          p.ParamCmdLineArg("exaalt", "no_of_states", 2, [1433600]),
          p.ParamCmdLineArg("exaalt", "bp_output_file", 3, ["output.bp"]),
          p.ParamCmdLineArg("exaalt", "transport_method", 4, ["FLEXPATH"]),
          p.ParamCmdLineArg("exaalt", "transport_variables", 5, [""]),
          p.ParamCmdLineArg("exaalt", "transport_options", 6, ["none"]),

          # 16, 8, or 4 stage nodes
          p.ParamRunner("stage_write", "nprocs", [256,128,64]),
          p.ParamCmdLineArg("stage_write", "input_bp_file", 1,
                            ["output.bp"]),
          p.ParamCmdLineArg("stage_write", "output_bp_file", 2,
                            ["staged.bp"]),
          p.ParamCmdLineArg("stage_write", "adios_read_method", 3,
                            ["FLEXPATH"]),
          p.ParamCmdLineArg("stage_write", "read_method_params", 4, [""]),
          p.ParamCmdLineArg("stage_write", "adios_write_method", 5,
                            ["MPI_AGGREGATE"]),
          p.ParamCmdLineArg("stage_write", "write_method_params", 6,
                            ["have_metadata_file=0;num_aggregators=16"]),
          p.ParamCmdLineArg("stage_write", "variables_to_transform", 7,
              ["atom_id,atom_type,px,py,pz,imx,imy,imz,atom_vid,vx,vy,vz"]),
          p.ParamCmdLineArg("stage_write", "transform_params", 8,
                            ["none","zlib:9", "bzip2:9","lz4"]),
        ]),
        p.Sweep([
          # 128 exaalt nodes
          p.ParamRunner("exaalt", "nprocs", [2048]),
          p.ParamCmdLineArg("exaalt", "states_list_file", 1,
                            ["states_list.txt"]),
          p.ParamCmdLineArg("exaalt", "no_of_states", 2, [1433600]),
          p.ParamCmdLineArg("exaalt", "bp_output_file", 3, ["output.bp"]),
          p.ParamCmdLineArg("exaalt", "transport_method", 4, ["FLEXPATH"]),
          p.ParamCmdLineArg("exaalt", "transport_variables", 5, [""]),
          p.ParamCmdLineArg("exaalt", "transport_options", 6, ["none"]),

          # 8, 4, or 2 stage nodes
          p.ParamRunner("stage_write", "nprocs", [128,64,32]),
          p.ParamCmdLineArg("stage_write", "input_bp_file", 1, ["output.bp"]),
          p.ParamCmdLineArg("stage_write", "output_bp_file", 2, ["staged.bp"]),
          p.ParamCmdLineArg("stage_write", "adios_read_method", 3,
                            ["FLEXPATH"]),
          p.ParamCmdLineArg("stage_write", "read_method_params", 4, [""]),
          p.ParamCmdLineArg("stage_write", "adios_write_method", 5,
                            ["MPI_AGGREGATE"]),
          p.ParamCmdLineArg("stage_write", "write_method_params", 6,
                            ["have_metadata_file=0;num_aggregators=8"]),
          p.ParamCmdLineArg("stage_write", "variables_to_transform", 7,
              ["atom_id,atom_type,px,py,pz,imx,imy,imz,atom_vid,vx,vy,vz"]),
          p.ParamCmdLineArg("stage_write", "transform_params", 8,
                            ["none","zlib:9", "bzip2:9","lz4"]),
        ]),
        p.Sweep([
          # 64 exaalt nodes
          p.ParamRunner("exaalt", "nprocs", [1024]),
          p.ParamCmdLineArg("exaalt", "states_list_file", 1,
                            ["states_list.txt"]),
          p.ParamCmdLineArg("exaalt", "no_of_states", 2, [1433600]),
          p.ParamCmdLineArg("exaalt", "bp_output_file", 3, ["output.bp"]),
          p.ParamCmdLineArg("exaalt", "transport_method", 4, ["FLEXPATH"]),
          p.ParamCmdLineArg("exaalt", "transport_variables", 5, [""]),
          p.ParamCmdLineArg("exaalt", "transport_options", 6, ["none"]),

          # 4, 2, or 1 stage nodes
          p.ParamRunner("stage_write", "nprocs", [64,32,16]),
          p.ParamCmdLineArg("stage_write", "input_bp_file", 1, ["output.bp"]),
          p.ParamCmdLineArg("stage_write", "output_bp_file", 2, ["staged.bp"]),
          p.ParamCmdLineArg("stage_write", "adios_read_method", 3,
                            ["FLEXPATH"]),
          p.ParamCmdLineArg("stage_write", "read_method_params", 4, [""]),
          p.ParamCmdLineArg("stage_write", "adios_write_method", 5,
                            ["MPI_AGGREGATE"]),
          p.ParamCmdLineArg("stage_write", "write_method_params", 6,
                            ["have_metadata_file=0;num_aggregators=4"]),
          p.ParamCmdLineArg("stage_write", "variables_to_transform", 7,
            ["atom_id,atom_type,px,py,pz,imx,imy,imz,atom_vid,vx,vy,vz"]),
          p.ParamCmdLineArg("stage_write", "transform_params", 8,
                            ["none","zlib:9", "bzip2:9","lz4"]),
        ]),

        # No staging or compression. Simulation writes data to disk.
        # These are the baseline test cases.
        p.Sweep([
          # 256 exaalt nodes
          p.ParamRunner("exaalt", "nprocs", [4096]),
          p.ParamCmdLineArg("exaalt", "states_list_file", 1,
                            ["states_list.txt"]),
          p.ParamCmdLineArg("exaalt", "no_of_states", 2, [1433600]),
          p.ParamCmdLineArg("exaalt", "bp_output_file", 3, ["output.bp"]),
          p.ParamCmdLineArg("exaalt", "transport_method", 4,
                            ["MPI_AGGREGATE"]),
          p.ParamCmdLineArg("exaalt", "transport_variables", 5,
                            ["have_metadata_file=0;num_aggregators=256"]),
          p.ParamCmdLineArg("exaalt", "transport_options", 6, ["none"]),
        ]),
        p.Sweep([
          # 128 exaalt nodes
          p.ParamRunner("exaalt", "nprocs", [2048]),
          p.ParamCmdLineArg("exaalt", "states_list_file", 1,
                            ["states_list.txt"]),
          p.ParamCmdLineArg("exaalt", "no_of_states", 2, [1433600]),
          p.ParamCmdLineArg("exaalt", "bp_output_file", 3, ["output.bp"]),
          p.ParamCmdLineArg("exaalt", "transport_method", 4, ["MPI_AGGREGATE"]),
          p.ParamCmdLineArg("exaalt", "transport_variables", 5,
                            ["have_metadata_file=0;num_aggregators=128"]),
          p.ParamCmdLineArg("exaalt", "transport_options", 6, ["none"]),
        ]),
        p.Sweep([
            # 64 exaalt nodes
            p.ParamRunner("exaalt", "nprocs", [1024]),
            p.ParamCmdLineArg("exaalt", "states_list_file", 1,
                              ["states_list.txt"]),
            p.ParamCmdLineArg("exaalt", "no_of_states", 2, [1433600]),
            p.ParamCmdLineArg("exaalt", "bp_output_file", 3, ["output.bp"]),
            p.ParamCmdLineArg("exaalt", "transport_method", 4,
                              ["MPI_AGGREGATE"]),
            p.ParamCmdLineArg("exaalt", "transport_variables", 5,
                              ["have_metadata_file=0;num_aggregators=64"]),
            p.ParamCmdLineArg("exaalt", "transport_options", 6, ["none"]),
        ]),
      ])
    ]