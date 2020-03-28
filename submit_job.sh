#!/bin/bash

#SBATCH -p scavenger
#SBATCH -e slurm/slurm_%A_%a.err
#SBATCH -o slurm/slurm_%A_%a.out 
#SBATCH --mem=10G
#SBATCH --array=1-13322

#  NOTE: MUST CHANGE THE ARRAY SIZE TO BE EQUAL TO THE NUMBER OF FILES IN THE DIRECTORY

# change directory, create slurm and rout directories
cd '/hpc/group/fuqua/mie4/edgar_parsing'
mkdir 'slurm'
mkdri 'rout'

# load R
module load R/3.6.0

# run R command, store out file
R CMD BATCH --no-restore --no-save code/3_process_one_cik.R rout/rout_$SLURM_ARRAY_TASK_ID.Rout
