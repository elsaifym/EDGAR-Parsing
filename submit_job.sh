#!/bin/bash

#SBATCH -p scavenger
#SBATCH -e Slurm/slurm_%A_%a.err
#SBATCH -o Slurm/slurm_%A_%a.out 
#SBATCH --mem=10G
#SBATCH --array=1-13292

cd '/hpc/group/fuqua/mie4/EDGAR Parsing'
module load R/3.6.0
R CMD BATCH 'Code/3_process_one_cik.R'