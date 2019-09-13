#!/bin/bash

NVME=0
PREFIX=/mnt/bb/$USER

ls -alr $PREFIX/coupling | tee nvme-list-1.log
du -h $PREFIX/coupling | tee nvme-du-1.log

if [ $NVME -eq 1 ]; then
    echo "Check files on NVME"
    ls -alr $PREFIX/coupling | tee nvme-list.log
    du -h $PREFIX/coupling | tee nvme-du.log
    mkdir nvme-dir
    /usr/bin/cp -r $PREFIX/coupling/* nvme-dir/
fi



