#!/bin/tcsh

module unload petsc
module unload hdf5
module unload fftw
module unload hypre
module unload netlib-lapack
module unload cuda
module unload python
module unload gcc

module load gcc/6.1.0
module load cuda/9.1.85
module load lapack/3.6.1
module load hypre/2.11.2/gnu/4.9.3/openmpi/1.8.6/nothreads
module load fftw/3.3.8/gnu/6.1.0/openmpi/1.10.2/avx/shared
module load hdf5/gnu/6.1.0/openmpi/1.10.2/shared/1.10.0
setenv LD_LIBRARY_PATH "/cell_root/software/hdf/1.10.0/gcc/6.1.0/openmpi/1.10.2/shared/sys/lib":${LD_LIBRARY_PATH}

setenv SAVANNA_WORKFLOW_FILE "workflow_dag.txt"
setenv SAVANNA_MONITOR_MODEL "outsteps2"
setenv SAVANNA_RESTART_PIPELINE 0
setenv SAVANNA_RESTART_STEPS 100 


