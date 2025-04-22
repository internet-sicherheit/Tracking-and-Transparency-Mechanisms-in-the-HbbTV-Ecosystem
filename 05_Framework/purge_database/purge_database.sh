#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "ERROR! No arguments supplied. Please provide a profile name."
	echo "Usage:  ./purge_database.sh <profile number> <path>"
    exit 1
  else
    profile=$1
    path=$2
fi

echo "Attempting to execute 'connect_via_ssh.exp'-script. Please wait a few seconds..."
./connect_via_ssh.exp
echo "'connect_via_ssh.exp'-script (successfully) executed."

echo "Attempting to copy cookie stores (.tar-files)."
echo 'Saving export to $path'
sshpass -p '{Password of TV}' scp -r root@{IP of TV}:/mnt/lg/cmn_data/var/lib/wam/Default/cookies.tar ${path}tv_export/${profile}_cookies_$(date +%s)_test.tar
sshpass -p '{Password of TV}' scp -r root@{IP of TV}:/mnt/lg/cmn_data/var/lib/wam/Default/local_storage.tar ${path}tv_export/${profile}_local_storage_$(date +%s)_test.tar
echo "tar files copied."

echo "Attempting to execute 'rm_tar_file.exp'-script. Please wait a few seconds..."
echo "'rm_tar_file.exp'-script (successfully) executed."

echo "Cookie store export script has terminated."
