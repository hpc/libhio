#! /bin/bash
# -*- Mode: sh; sh-basic-offset:2 ; indent-tabs-mode:nil -*- */
#
# Copyright (c) 2014-2016 Los Alamos National Security, LLC.  All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

#=============================================================================
# dw_simple_sub.sh - submits a DataWarp batch job to test basic Moab and
# DataWarp functionality at small scale.
#
# Can only be run on a system with Moab/DataWarp integration with a scratch
# filesystem (PFS) that is visible by DataWarp for stage-in and stage-out
# or for caching.
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
# 2) The submitted job (if striped mode):
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
# 2) The submitted job (if cached mode):
#
#   a) Allocates a cached DataWarp area (#DW jobdw directive)
#
#   b) Launches the compute part of the job
#
#   c) Issues various file modification commands against the DataWarp
#      cached area
#
# 3) The user follows the messages (step 1h) to verify the results:
#
#   a) Check the job output for error messages
#
#   b) Compare (via diff) the actual destination directory with the
#      expected destination directory
#
# Last update: 20160608  Cornell Wright
#=============================================================================

#-----------------------------------------------------------------------------
# Convenience functions
#-----------------------------------------------------------------------------
synexit() {
  echo ""
  if [[ -n $* ]]; then echo $*; echo ""; fi
  echo "dw_simple_sub.sh - submit a simple DataWarp test job"
  echo ""
  echo "  Syntax:"
  echo "    dw_simple_sub.sh [-c] [-t]"
  echo "                     [-h]"
  echo ""
  echo "  Options:"   
  echo "    -c                  Use DataWarp transparent cache mode"
  echo "                        (Default is striped mode)"
  echo "    -t                  Test mode - bypass issuing msub"
  echo "    -h                  Display this help"
  echo ""
  echo "  Cornell Wright  cornell@lanl.gov"
  
  exit 8
}

msg_log=""
msg() {
  echo "$HOST $*"
  if [[ -n $msg_log ]]; then echo "$HOST $*" >> $msg_log; fi
}

cmd() {
  msg "---> $*"
  cmd_msg=$(eval "$*")
  return $?
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
ver="20160524"
dashes="---------------------------------------------------------------------"

dw_cache=0
mode="striped mode"
test_mode=0
while getopts "hct" optname; do
  case $optname in
    h ) synexit;;
    c ) dw_cache=1;;
    t ) test_mode=1;;
   \? ) synexit "Error: invalid option";;
  esac
done
shift $((OPTIND - 1 ))
if [[ -n $1 ]]; then synexit "Error: extra parameters"; fi
if [[ $dw_cache -eq 1 ]]; then mode="cached mode"; fi

msg "$dashes"

msg "$pkg version $ver starting ($mode)"

#-----------------------------------------------------------------------------
# Find a usable scratch directory
#-----------------------------------------------------------------------------
msg "Finding scratch directory"
find_scratch /scratch1/users/$LOGNAME
find_scratch /lustre/scratch4/$LOGNAME
find_scratch /lustre/scratch5/$LOGNAME
find_scratch /lscratch1/$LOGNAME
find_scratch ~/scratch-tmp

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
msg_log="$dir/${pkg}_job.start.log"
msg "Run directory is $dir"
msg "These messages now logged to $msg_log"

dir_si="$dir/stage_in"
if [[ $dw_cache -eq 0 ]]; then 
  dir_so="$dir/stage_out"
  dw_env="\$DW_JOB_STRIPED"
  dw_work="\$DW_JOB_STRIPED/dest"
else
  dw_env="\$DW_JOB_STRIPED_CACHED"
  dw_work="\$DW_JOB_STRIPED_CACHED/stage_in"
fi 
dir_ex="$dir/expect"
job_file="$dir/${pkg}_job.sh"
job_out="$dir/${pkg}_job.out"
newdata="New file $(date)."

#-----------------------------------------------------------------------------
# Build job script
#-----------------------------------------------------------------------------
echo "#! /bin/bash" > $job_file
echo "#MSUB -l nodes=1:ppn=1,walltime=10:00" >> $job_file
echo "#MSUB -o $job_out -joe" >> $job_file
if [[ $dw_cache -eq 0 ]]; then
  echo "#DW jobdw type=scratch access_mode=striped capacity=1GiB" >> $job_file
  echo "#DW stage_in destination=$dw_work source=$dir_si type=directory" >> $job_file
  echo "#DW stage_out source=$dw_work destination=$dir_so type=directory" >> $job_file
else
  echo "#DW jobdw type=cache access_mode=striped pfs=$dir capacity=1GiB" >> $job_file
fi
echo "echo \"\$(date) $pkg version $ver ($mode) start\"" >> $job_file
echo "set | egrep \"^PBS|^DW|^HOST\"" >> $job_file
echo "echo \"$dashes\"" >> $job_file
echo "echo \"CHECK: The following commands should complete without error\"" >> $job_file
echo "echo \"CHECK: The next find command should show 3 files: change_me, delete_me, keep_me\"" >> $job_file
echo "aprun -n 1 -b bash -xc \"find $dw_env -ls\"" >> $job_file
if [[ $dw_cache -eq 0 ]]; then
  echo "aprun -n 1 -b bash -xc \"diff -r $dw_work $dir_si\"" >> $job_file
fi
echo "aprun -n 1 -b bash -xc \"rm $dw_work/delete_me\"" >> $job_file
echo "aprun -n 1 -b bash -xc \"dd bs=1000 seek=128 count=256 status=none if=/dev/zero of=$dw_work/change_me\"" >> $job_file
echo "aprun -n 1 -b bash -xc \"echo \\\"$newdata\\\" > $dw_work/new_file\"" >> $job_file
echo "echo \"CHECK: The next find command should show 3 files: change_me, keep_me, new_file\"" >> $job_file
echo "aprun -n 1 -b bash -xc \"find $dw_env -ls\"" >> $job_file
echo "aprun -n 1 -b bash -xc \"diff -r $dw_work $dir_ex\"" >> $job_file
echo "echo \"$dashes\"" >> $job_file
echo "echo \"\$(date) $pkg version $ver ($mode) end\"" >> $job_file

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
if [[ $test_mode -eq 0 ]]; then
  cmd "msub $job_file"
else
  msg "msub $job_file"
  cmd_msg="dummy-1001 dummy-1002 dummy-1003"
fi
jobid=${cmd_msg#$'\n'}         # Strip leading newline
msg "msub returns: \"$jobid\""
if [[ -z $jobid ]]; then errx "msub failed; no job ID returned"; fi
jobid=($jobid)                 # Convert to array
if [[ ${#jobid[*]} -ne 3 ]]; then errx "msub did not return 3 job IDs, DataWarp probably not recognized"; fi
msg "DataWarp job submitted"
msg "$dashes"
msg "When job ${jobid[1]} completes, check for error messages in $job_out"
msg "When job ${jobid[2]} completes, compare directories stage_out and expect via:"
if [[ $dw_cache -eq 0 ]]; then
  msg "     diff -r $dir_so $dir_ex"
else
  msg "     diff -r $dir_si $dir_ex"
fi
msg "$dashes"
msg "$pkg version $ver done"
if [[ $test_mode -eq 1 ]]; then cat $job_file; fi
# --- end of dw_simple_sub.sh ---
