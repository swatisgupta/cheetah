# README #

This is an example of ruuning XGC1-XGCa coupling.

## Build and input parameters
We need to build XGC1 and XGCa. No specific compiler flag is necessary. 
XGC1-XGCa coupling features can be controled through `input` file. In `input`, we need `coupling_param` section. 

```
&coupling_param
sml_coupling_on=.true.
/
```

## Run

First, we need to set Adios method (MPI for files. DIMES/DATASPACES/FLEXPATH for staging). For an example, to run with DIMES method, we can use the following script:
```
METHOD=DIMES
setmethod.py -i XGC1_exec/adioscfg.xml coupling.info $METHOD "app_id=1"
setmethod.py -i XGC1_exec/adioscfg.xml couplingp     $METHOD "app_id=1"
setmethod.py -i XGC1_exec/adioscfg.xml couplingt     $METHOD "app_id=1"
setmethod.py -i XGCa_exec/adioscfg.xml coupling.info $METHOD "app_id=2"
setmethod.py -i XGCa_exec/adioscfg.xml couplingp     $METHOD "app_id=2"
setmethod.py -i XGCa_exec/adioscfg.xml couplingt     $METHOD "app_id=2"
sed -i "s/staging_read_method_name=.*./staging_read_method_name='$METHOD'/g" XGC1_exec/adios.in
sed -i "s/staging_read_method_name=.*./staging_read_method_name='$METHOD'/g" XGCa_exec/adios.in
```
`setmethod.py` is an utility script to change method in adioscfg.xml.

To manage multiple executables and staging services, we will use `stagerun` wrapper command. The following is an example on Cori:
```
OPT="--noserver"
if [ $METHOD = "DATASPACES" ] || [ $METHOD = "DIMES" ]; then
    OPT=""
fi

./clear_simu.sh
./stagerun $OPT -s 4 --oe server.log --mpicmd="srun" --opt="-N 2" : \
    -n 256 --cwd=./XGC1_exec --oe xgc1.log --opt="-N 8" ../xgc-es : \
    -n 256 --cwd=./XGCa_exec --oe xgca.log --opt="-N 8" ../xgca 
```

Then, check the progress with the log files as follows:
```
tail -f server.log xgc1.log xgca.log
```

Note: `stagerun` is a wrapper to run multiple executables. Details can be found in https://github.com/jychoi-hpc/stagerun

On Titan, `stagerun` command will be as follows:
```
./stagerun $OPT -s 4 --oe server.log --mpicmd="aprun" --opt="-N 1" : \
    -n 256 --cwd=./XGC1_exec --oe xgc1.log --opt="-N 8" ../xgc-es : \
    -n 256 --cwd=./XGCa_exec --oe xgca.log --opt="-N 8" ../xgca 
```

## Job script
Here are examples of job script.

```
#!/bin/bash
#PBS -A ENV003
#PBS -j oe
#PBS -q debug 
#PBS -l nodes=13
#PBS -l walltime=00:30:00
#PBS -V

export CRAY_CUDA_PROXY=1
export OMP_NUM_THREADS=1

METHOD=MPI
setmethod.py -i XGC1_exec/adioscfg.xml coupling.info $METHOD "app_id=1"
setmethod.py -i XGC1_exec/adioscfg.xml couplingp     $METHOD "app_id=1"
setmethod.py -i XGC1_exec/adioscfg.xml couplingt     $METHOD "app_id=1"
setmethod.py -i XGCa_exec/adioscfg.xml coupling.info $METHOD "app_id=2"
setmethod.py -i XGCa_exec/adioscfg.xml couplingp     $METHOD "app_id=2"
setmethod.py -i XGCa_exec/adioscfg.xml couplingt     $METHOD "app_id=2"
sed "s/staging_read_method_name=.*./staging_read_method_name='$METHOD'/g" XGC1_exec/adios.in > XGC1_exec/adios.in.tmp
sed "s/staging_read_method_name=.*./staging_read_method_name='$METHOD'/g" XGCa_exec/adios.in > XGCa_exec/adios.in.tmp
mv XGC1_exec/adios.in.tmp XGC1_exec/adios.in
mv XGCa_exec/adios.in.tmp XGCa_exec/adios.in

mkdir -p XGC1_exec/restart_dir
mkdir -p XGCa_exec/restart_dir

OPT="--noserver"
if [ $METHOD = "DATASPACES" ] || [ $METHOD = "DIMES" ]; then
    OPT=""
fi

./clear_simu.sh
./stagerun $OPT -s 1 --oe server.log --mpicmd="aprun" : \
    -n 96 --cwd=./XGC1_exec --oe xgc1.log --opt="-N 16" ../xgc-es : \
    -n 96 --cwd=./XGCa_exec --oe xgca.log --opt="-N 16" ../xgca
```