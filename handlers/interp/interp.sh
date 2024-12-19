#!/bin/bash

# set -x

source ~/anaconda3/etc/profile.d/conda.sh
conda activate cap

cinterp=$1
dati=$2
cartella_dati=$3
root_lavoro=$4

mkdir -p $root_lavoro

##cp -v $file_dati $root_lavoro/
#cp -v $3/$2 $root_lavoro/


cd $root_lavoro
$cinterp -n c48 $2
