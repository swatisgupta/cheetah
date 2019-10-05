#!/bin/bash

mkdir -p xgc1/timing
mkdir -p xgc1/restart_dir
mkdir -p xgca/timing
mkdir -p xgca/restart_dir
rm -f xgc1/*.unlock
rm -f xgca/*.unlock

USER=ssinghal
NVME=0

PREFIX=/mnt/bb/$USER
if [ $NVME -eq 1 ]; then
    # Use NVME
    rm -rf $PREFIX/coupling
    mkdir $PREFIX/coupling
    ln -snf $PREFIX/coupling .
    df -h | grep /mnt/bb
else
    # For writing to filesystem
    rm -rf ./coupling
    mkdir -p ./coupling
fi

cp /gpfs/alpine/scratch/ssinghal/csc299/XGC1-XGCA-EXPS/06-xgc1-xca/workflow_dag.txt . 
