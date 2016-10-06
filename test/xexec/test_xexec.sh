#! /bin/bash
#-----------------------------------------------------------------------------
# test_xexec.sh - unit test driver for xexec.x
#
# Results logged to a date/time stamped file name.  Coverage is pretty
# good, but functional quality of tests is limited, since the only error
# detected is a non-zero exit code.
#-----------------------------------------------------------------------------
msg() {
  echo "`date \"$datefmt\"` $host test_xexec: $*" | tee -a $log
}

run_all() {
  # run_all - function to run and log tests with specified commands
  pre1=$1
  pre2=$2
  suf=$3
  declare -a act1=("${!4}")
  declare -a act2=("${!5}")
  for a in "${act1[@]}" "${act2[@]}"; do
    if [[ -n $a ]]; then
      cmd="$pre1 $bin $pre2 $a $suf"
      msg $bar
      msg "--> $cmd" 
      $cmd 2>&1 | tee -a $log
      rc=${PIPESTATUS[0]} 
      msg "rc: $rc" 
      if [[ $rc -ne 0 ]]; then
        msg="Error rc: $rc from $cmd"
        msg $msg 
        fail+=("$msg")
      fi
    fi
  done
}

bar="========================================================================================="
datefmt="+%Y-%m-%d %H:%M:%S"
host=`hostname -s`
bin=".libs/xexec.x"
log="./test_xexec.log.$(date +%Y%m%d.%H%M%S)"
dir="tmp.test_xexec.dir"
export DYLD_LIBRARY_PATH=$PWD/../../src/.libs:$DYLD_LIBRARY_PATH
mkdir $dir

hio="
  /@@ Read and write N-N test case with read data value checking @/
  name test_xexec
  dbuf RAND22P 20Mi
  hi MYCTX posix:$dir
  hdu NTNDS 97 ALL
  hda NTNDS 97 WRITE,CREAT UNIQUE hdo
  heo MYEL WRITE,CREAT,TRUNC
  hvp c. .
  lc 5
    hew 0 8192
  le
  hec hdc hdf hf
"

# The actions to be tested - serial, parallel or both
act_s+=("-h")
act_b+=("o 2 e 2 s 0.1 srr 27")
act_b+=("va 1Mi vt 4ki va 1Gi vt 1Mi vf vf")
act_b+=("hx 1 4095 20 50ki 2")
act_b+=("ni 4 4 nr 1 3 nf")
act_b+=("grep test_xexec $0")
act_b+=("fo fx_test w fw 0 22 fw 0 1 fc fo fx_test r fr 0 22 fr 0 1 fc")
act_b+=("$hio")
act_p+=("mb msr 1Mi 2")

# Test serial and both actions
run_all "" "" "" act_s[@] act_b[@] 
# Crank up verbosity and debug
run_all "" "v 3 d 4" "" act_b[@] 
# Test both and parallel actions
run_all "mpirun -n 2" "mi 1" "mgf mf" act_b[@] act_p[@]

# Output summary results messages
msg $bar
msg "Result Summary:" 
for i in "${fail[@]}"; do msg $i; done
msg "Logs in $PWD $log" 
msg "$0 done; failure count: ${#fail[@]}" 
exit ${#fail[@]}

# --- end of test_xexec.sh ---

