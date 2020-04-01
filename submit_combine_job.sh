#!/bin/bash

#SBATCH -p scavenger
#SBATCH -e slurm/slurm_combine.err
#SBATCH -o slurm/slurm_combine.out 
#SBATCH --mem=200G
#SBATCH -c 10

# change directory, create slurm and rout directories
cd '/hpc/group/fuqua/mie4/data_projects/edgar_parsing'

# load R
module load R/3.6.0

# run R command, store out file
R CMD BATCH --no-restore --no-save code/4_combine_tables.R rout/rout_combine.Rout
