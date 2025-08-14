Repository for my bachelor thesis one an exploration of gender-bias in citation networks.
The repository has two branches as I encountered complications when trying to merge them.

This repository contains script, jobs and jupyter notebooks used in this exploration.

Due to the file sizes of csvs used. These can instead be found in the bachelor thesis dropbox: https://www.dropbox.com/scl/fo/ochtkbod8fcbiqazx1nph/AMQXUdYOMYfbfA9U2KnkIjs?rlkey=swz12nr0u94kwq6skck8gqozc&st=9yui6rsj&dl=0 


Disclaimer: Some notebooks contain "medicine" as a field. This was a field that was explored, but left out due to its size.

Python script ran as sbatch jobs on ITU’s HPC, due to the size of the output files. The jupyter notebooks, were used to explore the results of the output files and run statistical analyses on them.

Set up:
1.	Create an environment: conda env create -f environmental.yml
a.	Conda activate myenv
2.	Submit a job from the folder SLURM_JOBS_AND_SCRIPTS
a.	Sbtach script_name.job
i.	Replace -script_name’ with the job related to the script you want to run. 
ii.	Any potential errors can then be found in .err, and any print statements can be found in .out

