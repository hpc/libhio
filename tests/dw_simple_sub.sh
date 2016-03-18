#! /bin/bash
#=============================================================================
# dw_simple_sub.sh - submits a DataWarp batch job to test basic Moab and 
# DataWarp functionality at small scale.
# 
# Can only be run on a system with Moab/DataWarp integration with a scratch
# filesystem (PFS) that is visible by DataWarp for stage-in and stage-out.
# 
# Last update: 20160318  Cornell Wright 
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
  msg "Error: $*; Exitting"
  exit 12
}

find_scr () {
  if [[ -e $1 ]]; then export DW_SIMPLE_SCR=$1; fi
}

#-----------------------------------------------------------------------------
# Create unique run directory - must be on PFS
#-----------------------------------------------------------------------------

# Find a usable scratch directory, first one found is used.
# Add additional find_scr calls as needed for various systems
find_scr /scratch1/$LOGNAME
find_scr /lustre/scratch4/$LOGNAME
find_scr /lustre/scratch5/$LOGNAME

if [[ -z $DW_SIMPLE_SCR ]]; then errx "Unable to find scratch directory"; fi

pkg="dw_simple"

dir="$DW_SIMPLE_SCR/run_${pkg}_$(date +%Y%m%d.%H%M%S)"
while [[ -e $dir ]]; do
  msg "Directory $dir already exists, trying another"
  sleep 1
  dir="$DW_SIMPLE_SCR/run_${pkg}_$(date +%Y%m%d.%H%M%S)"
done
cmd "mkdir -p $dir"

#-----------------------------------------------------------------------------
# Build job script
#-----------------------------------------------------------------------------
job_file="$dir/${pkg}_job.sh"
job_out="$dir/${pkg}_job.out"
dir_si=$dir/stage_in
dir_so=$dir/stage_out
dir_ex=$dir/expect

job='#! /bin/bash
#MSUB -l nodes=1:ppn=1,walltime=10:00
#MSUB -o '$job_out' -joe
#DW jobdw access_mode=striped type=scratch capacity=1GiB
#DW stage_in destination=$DW_JOB_STRIPED/dest source='$dir_si' type=directory
#DW stage_out source=$DW_JOB_STRIPED/dest destination='$dir_so' type=directory
echo "$(date) Start"
set | egrep "^PBS|^DW|^HOST"
echo "-----------------------------------------------------------------------------"
echo "CHECK: The following commands should complete without error"
echo "CHECK: The next find command should show 3 files: keep_me, change_me, delete_me"
aprun -n 1 -b bash -xc "find $DW_JOB_STRIPED -ls"
aprun -n 1 -b bash -xc "diff -r $DW_JOB_STRIPED/dest '$dir_si'"
aprun -n 1 -b bash -xc "rm $DW_JOB_STRIPED/dest/delete_me"
aprun -n 1 -b bash -xc "dd bs=1000 seek=128 count=256 status=none if=/dev/zero of=$DW_JOB_STRIPED/dest/change_me"
aprun -n 1 -b bash -xc "find $DW_JOB_STRIPED -ls"
aprun -n 1 -b bash -xc "diff -r $DW_JOB_STRIPED/dest '$dir_ex'"
echo "-----------------------------------------------------------------------------"
echo "$(date) End"
'

echo "$job" > $job_file

#-----------------------------------------------------------------------------
# Create Stage-in directory and expect directory
#-----------------------------------------------------------------------------
cmd "mkdir -p $dir_si"
cmd "dd bs=1000 count=512 status=none if=/dev/urandom of=$dir_si/keep_me"
cmd "dd bs=1000 count=512 status=none if=/dev/urandom of=$dir_si/delete_me"
cmd "dd bs=1000 count=512 status=none if=/dev/urandom of=$dir_si/change_me"
cmd "cp -fpR $dir_si $dir_ex"
cmd "rm $dir_ex/delete_me"
cmd "dd bs=1000 seek=128 count=256 status=none if=/dev/zero of=$dir_ex/change_me"

#-----------------------------------------------------------------------------
# Submit job and advise user on results checking
#-----------------------------------------------------------------------------
cmd "msub $job_file"
jobid=${cmd_msg#$'\n'}         # Strip leading newline
msg "msub returns: \"$jobid\""
if [[ -z $jobid ]]; then errx "msub failed; no job ID returned"; fi
jobid=($jobid)                 # Convert to array
if [[ ${#jobid[*]} -ne 3 ]]; then errx "msub did not return 3 job IDs, DataWarp probably not recognized"; fi
msg "DataWarp job submitted"
msg "When job ${jobid[1]} completes, check for error messages in $job_out"
msg "When job ${jobid[2]} completes, compare directories stage_out and expect via:"
msg "     diff -r $dir_so $dir_ex"

# --- end of dw_simple_sub.sh ---

