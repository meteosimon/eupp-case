#!/bin/bash
#$ -cwd
#$ -N _BAMLSS
#$ -l h_rt=00:30:00
#$ -l h_vmem=6g
#$ -t 1-21

if [ $# -ne 2 ] ; then
	echo "Missing input args; stop"
	exit 333
fi
country="${1}"
station="${2}"

# Activating conda environment
conda activate reto_renv

Rscript bamlss_run.R -c "${country}" -s $station

