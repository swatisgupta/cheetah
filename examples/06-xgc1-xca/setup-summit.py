#!/bin/bash

GCC=0

module use -a /gpfs/alpine/proj-shared/csc143/jyc/summit/sw/modulefiles
module unload /gpfs/alpine/csc143/world-shared/jyc/summit/sw/spack/share/spack/modules/linux-rhel7-ppc64le

module unload petsc
module unload adios2
module unload adios
module unload hdf5
module unload fftw
module unload hypre
module unload netlib-lapack
module unload cuda
module unload python
module unload pgi

module unload xl pgi gcc
if [ "${GCC}" -eq "1" ]; then
module load gcc/6.4.0
export XGC_PLATFORM=summit.gcc.jyc
else
module load pgi/19.4
export XGC_PLATFORM=summit.pgi.jyc
fi

module load python/2.7.15-anaconda2-5.3.0
module load cuda/10.1.105
module load netlib-lapack/3.8.0
module load hypre/2.13.0
module load fftw/3.3.8
module load hdf5/1.10.3
module load petsc/3.7.2-py2


export SAVANNA_WORKFLOW_FILE="workflow_dag.txt"
export SAVANNA_RESTART_PIPELINE=1
export SAVANNA_RESTART_STEPS=400 


