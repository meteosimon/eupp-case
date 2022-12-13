#!/bin/bash
#$ -cwd
#$ -N _EUPP
#$ -l h_rt=00:30:00
#$ -l h_vmem=6g
#$ -t 1-21

if [ $# -ne 3 ] ; then
  echo "Missing input args; stop"
  exit 333
fi
model="${1}"
country="${2}"
station="${3}"

# Activating conda environment
conda activate reto_renv

if [ ${model} == "crch" ] ; then
  printf "Rscript crch_run.R -c '${country}' -s $station\n"
  Rscript crch_run.R -c "${country}" -s $station
elif [ ${model} == "bamlss" ] ; then
  printf "Rscript bamlss_run.R -c "${country}" -s $station\n"
  Rscript bamlss_run.R -c "${country}" -s $station
elif [ ${model} == "bamlss03" ] ; then
  printf "Rscript bamlss_run.R -c "${country}" -s $station -y 3\n"
  Rscript bamlss_run.R -c "${country}" -s $station -y 3
else
  printf "ERROR: Unknown model (first input argument) ${model}\n"
fi

