#! /bin/bash
#=============================================================================
# dw_simple_sub.sh - submits a DataWarp batch job to test basic Moab and 
# DataWarp functionality at small scale.
# 
# Can only be run on a system with Moab/DataWarp integration with a scratch
# filesystem (PFS) that is visible by DataWarp for stage-in and stage-out.
#
# How it works:
#
# 1) This script:
#
#   a) Finds a usable scratch directory
#
#   b) Creates a unique run directory therein 
#
#   c) Assigns directory and file names in that run directory
#
#   d) Writes a job script to the run directory
#
#   e) Creates a stage-in source directory in the run directory
#
#   f) Creates an expected results directory
#
#   g) Submits the job script (created in step 1d)
#
#   h) Outputs messages describing the needed verification steps
#
# 2) The submitted job:
#
#   a) Allocates a shared scratch DataWarp area (#DW jobdw directive)
#
#   b) Performs a stage-in into DataWarp (#DW stage_in directive)
#
#   c) Launches the compute part of the job
#
#   d) Issues various file modification commands against the DataWarp
#      shared scratch area
#
#   e) Performs a stage-out back to the scratch run directory (#DW stage_out
#      directive)
#
# 3) The user follows the messages (step 1h) to verify the results:
#
#   a) Check the job output for error messages
#
#   b) Compare (via diff) the actual stage-out destination directory with the 
#      expected stage out destination directory
# 
# Last update: 20160321  Cornell Wright 
#=============================================================================

#-----------------------------------------------------------------------------
# Convenience functions
#-----------------------------------------------------------------------------
cmd() {
  echo "$HOST ---> $*"
  cmd_msg=$(eval "$*")
  return $?
}

msg() {
  echo "$HOST $*" 
}

errx() {
  msg "Error: $*; Exiting"
  exit 12
}

find_scratch () {
  if [[ -z $DW_SIMPLE_SCR ]]; then
    if [[ -e $1 ]]; then
      export DW_SIMPLE_SCR=$1
    fi
  fi
}

#-----------------------------------------------------------------------------
# Start up
#-----------------------------------------------------------------------------
pkg="dw_simple"
ver="20160321"
dashes="---------------------------------------------------------------------"
msg "$dashes"
msg "$pkg version $ver starting" 

#-----------------------------------------------------------------------------
# Find a usable scratch directory
#-----------------------------------------------------------------------------
msg "Finding scratch directory"
find_scratch /scratch1/users/$LOGNAME
find_scratch /lustre/scratch4/$LOGNAME
find_scratch /lustre/scratch5/$LOGNAME

# Add additional find_scratch calls (above) as needed for various systems

if [[ -z $DW_SIMPLE_SCR ]]; then 
  errx "Unable to find scratch directory; add a directory near $0 line $(($LINENO-5))"
fi
msg "Scratch directory is $DW_SIMPLE_SCR"

#-----------------------------------------------------------------------------
# Create unique run directory in scratch directory, assign dir and file names
#-----------------------------------------------------------------------------
dir="$DW_SIMPLE_SCR/run_${pkg}_$(date +%Y%m%d.%H%M%S)"
while [[ -e $dir ]]; do
  msg "Directory $dir already exists, trying another"
  sleep 1
  dir="$DW_SIMPLE_SCR/run_${pkg}_$(date +%Y%m%d.%H%M%S)"
done
cmd "mkdir -p $dir"
msg "Run directory is $dir"

dir_si=$dir/stage_in
dir_so=$dir/stage_out
dir_ex=$dir/expect
job_file="$dir/${pkg}_job.sh"
job_out="$dir/${pkg}_job.out"
newdata="New file $(date)."

#-----------------------------------------------------------------------------
# Build job script
#-----------------------------------------------------------------------------
job='#! /bin/bash
#MSUB -l nodes=1:ppn=1,walltime=10:00
#MSUB -o '$job_out' -joe
#DW jobdw access_mode=striped type=scratch capacity=1GiB
#DW stage_in destination=$DW_JOB_STRIPED/dest source='$dir_si' type=directory
#DW stage_out source=$DW_JOB_STRIPED/dest destination='$dir_so' type=directory
echo "$(date) '$pkg' version '$ver' start"
set | egrep "^PBS|^DW|^HOST"
echo "'$dashes'"
echo "CHECK: The following commands should complete without error"
echo "CHECK: The next find command should show 3 files: change_me, delete_me, keep_me"
aprun -n 1 -b bash -xc "find $DW_JOB_STRIPED -ls"
aprun -n 1 -b bash -xc "diff -r $DW_JOB_STRIPED/dest '$dir_si'"
aprun -n 1 -b bash -xc "rm $DW_JOB_STRIPED/dest/delete_me"
aprun -n 1 -b bash -xc "dd bs=1000 seek=128 count=256 status=none if=/dev/zero of=$DW_JOB_STRIPED/dest/change_me"
aprun -n 1 -b bash -xc "echo \"'$newdata'\" > $DW_JOB_STRIPED/dest/new_file"
echo "CHECK: The next find command should show 3 files: change_me, keep_me, new_file"
aprun -n 1 -b bash -xc "find $DW_JOB_STRIPED -ls"
aprun -n 1 -b bash -xc "diff -r $DW_JOB_STRIPED/dest '$dir_ex'"
echo "'$dashes'"
echo "$(date) '$pkg' version '$ver' end"
'

echo "$job" > $job_file
msg "Job script is $job_file"

#-----------------------------------------------------------------------------
# Create Stage-in directory and expect directory
#-----------------------------------------------------------------------------
msg "Creating stage-in directory: $dir_si"
cmd "mkdir -p $dir_si"
cmd "dd bs=1000 count=512 status=none if=/dev/urandom of=$dir_si/keep_me"
cmd "dd bs=1000 count=512 status=none if=/dev/urandom of=$dir_si/delete_me"
cmd "dd bs=1000 count=512 status=none if=/dev/urandom of=$dir_si/change_me"
msg "Creating expected results directory: $dir_ex"
cmd "cp -fpR $dir_si $dir_ex"
cmd "rm $dir_ex/delete_me"
cmd "dd bs=1000 seek=128 count=256 status=none if=/dev/zero of=$dir_ex/change_me"
cmd "echo \"$newdata\" > $dir_ex/new_file"

#-----------------------------------------------------------------------------
# Submit job and advise user on results checking
#-----------------------------------------------------------------------------
msg "Submitting job"
cmd "msub $job_file"
jobid=${cmd_msg#$'\n'}         # Strip leading newline
msg "msub returns: \"$jobid\""
if [[ -z $jobid ]]; then errx "msub failed; no job ID returned"; fi
jobid=($jobid)                 # Convert to array
if [[ ${#jobid[*]} -ne 3 ]]; then errx "msub did not return 3 job IDs, DataWarp probably not recognized"; fi
msg "DataWarp job submitted"
msg "$dashes"
msg "When job ${jobid[1]} completes, check for error messages in $job_out"
msg "When job ${jobid[2]} completes, compare directories stage_out and expect via:"
msg "     diff -r $dir_so $dir_ex"
msg "$dashes"
msg "$pkg version $ver done" 

# --- end of dw_simple_sub.sh ---

