#!/bin/bash
#_RANK=$ALPS_APP_PE
_RANK=$PMIX_RANK
#tau_exec -T papi,mpi,pthread,adios2 ./xgca 2>&1 | sed 's/^/[XGCA:'$_RANK'] /'
./xgca 2>&1 | sed 's/^/[XGCA:'$_RANK'] /'
