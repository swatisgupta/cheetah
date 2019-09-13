#!/bin/bash
#_RANK=$ALPS_APP_PE
_RANK=$PMIX_RANK
#tau_exec -T papi,mpi,pthread,adios2 ./xgc-es 2>&1 | sed 's/^/[XGC1:'$_RANK'] /'
./xgc-es 2>&1 | sed 's/^/[XGC1:'$_RANK'] /'
