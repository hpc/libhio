# -*- Mode: sh; sh-basic-offset:2 ; indent-tabs-mode:nil -*-
#
# Copyright (c) 2017 Los Alamos National Security, LLC.  All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#
#-----------------------------------------------------------------------------
# cantest.py - tests the effects of cancelling various Moab/DataWarp sub-jobs at
# various points in the execution of the overall job.
# 
# See README.cantest for additional description and usage information.
#-----------------------------------------------------------------------------
import xml.etree.ElementTree as ET
import os, re, subprocess, sys, threading, time

#-----------------------------------------------------------------------------
# multi_join - return a string constructed by concatenating the contents of
# multiple arguments.  All positional arguments are used, each one may be a
# string, list, tuple or other object.  Lists or tuples can contain strings,
# lists, tuples or other objects nested to any depth.  Strings are concatenated
# together with an intervening blank.  Lists and tuples are iterated through to
# process their contents.  Other objects are converted to string with the str()
# function and concatenated to the result.
#-----------------------------------------------------------------------------
def multi_join(*args):
  ret = ''
  for arg in args:
      if isinstance(arg, (list, tuple)):
          for a in arg: ret = ret + ' ' + multi_join(a)
      else:
          ret = ret + ' ' + str(arg)
  if len(ret) > 0: ret = ret[1:]
  return ret

#-----------------------------------------------------------------------------
# MessageWriter - a class for producing messages with optional timestamps and
# module(line) annotations.
#-----------------------------------------------------------------------------
class MessageWriter(object):

  def __init__(self, **keys):
      self.timestamp = True
      self.timestampFormat = '%Y-%m-%d %H:%M:%S'
      self.writer = sys.stdout
      self.caller = True
      self.callerDepth = 1
      self.modify(**keys)

  def modify(self, timestamp=None, timestampFormat=None, stderr=None,
             caller=None, callerDepth=None, makeDefault=False):
      global defaultMessageWriter
      if timestamp != None: self.timestamp = timestamp
      if timestampFormat != None: self.timestampFormat = timestampFormat
      if stderr != None:
          if stderr: self.writer = sys.stderr
      if caller != None: self.caller = caller
      if callerDepth != None: self.callerDepth = callerDepth
      if makeDefault: defaultMessageWriter = self
      return self

  def write(self, *args, **keys):
      extra_depth = keys.pop('__extra_depth__', 0)
      if len(keys) > 0:
          copy.copy(self).modify(**keys).write(*args, __extra_depth__=1)
      else:
          if self.caller:
              frame = inspect.stack()[self.callerDepth + extra_depth]
              caller = re.sub('.*/', '', frame[1]) + '(' + str(frame[2]) + '): '
          else: caller = ''
          if self.timestamp: ts = time.strftime(self.timestampFormat) + ' '
          else: ts = ''
          text = multi_join(args) 
          if len(text) == 0 or text[-1] != '\n': lf = '\n'
          else: lf = ''
          self.writer.write(''.join([ts, caller, text, lf]))

#-----------------------------------------------------------------------------
# runcmd - run external command with optional echo and checking rc and stderr
#-----------------------------------------------------------------------------
def runcmd(tag, cmd, echo=True, shell=False, rcok=False, stderrok=False ):
  if isinstance(cmd, (list, tuple)):
    cmd_s = ' '.join(cmd)
  else:
    cmd_s = cmd
  if shell:
    cmd_p = cmd_s
  else:
    cmd_p = cmd
  if echo:
    msg(tag, '-->', cmd_s)

  p1 = subprocess.Popen(cmd_p, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE);
  (so, se) = p1.communicate()
  rc = p1.returncode
  if rc != 0 or len(se) != 0:
    cmd_0 = cmd_s.split(None, 1)[0]
    if not echo:
      msg(tag, '-->', cmd_s)
    msg(tag, cmd_0, 'rc =', str(rc))
    if len(se) != 0:
      msg(tag, cmd_0, 'stderr:', se.strip('\r\n'))
    if (len(se) > 0 and not stderrok) or (rc != 0 and not rcok):
      raise subprocess.CalledProcessError(rc, cmd=cmd_s, output=se)
  return so

#-----------------------------------------------------------------------------
# dump - full dump of an xml.etree element and its attributes
#-----------------------------------------------------------------------------
def dump(indent, element):
  print indent, 'tag:', element.tag
  attrib = element.attrib
  for a in attrib:
    print indent, '  ', a, ':', attrib[a]
  for child in element:
    dump(indent+'  ', child)

#-----------------------------------------------------------------------------
# addjob uses jobid string returned by msub to add an entry to tjob  
#-----------------------------------------------------------------------------
def addjob(idstr):
  global gstat
  global tjob
  global biglock
  jglegend = 'ITCO'
  s = idstr.split(' ')
  jobids = (s[0], s[0].split('.')[0], s[1], s[2])
  name = s[1]
  biglock.acquire()
  tjob[name] = [jobids, True]
  gstat[name] = '----' 
  biglock.release()
  msg('{' + name + '} Job IDs:', jglegend, jobids)
  return name

#-----------------------------------------------------------------------------
# bldstat - read job status from mdiag, update the gstat dictionary with
# the status of each active job group in tjob.  If none of the jobs in
# a job group are found, mark the job group from tjob inactive.
#-----------------------------------------------------------------------------
def bldstat():
  global gstat
  global tjob
  global biglock
  global quitflag
  #cmd = ('cat', 'run_combo.xml')
  cmd = ('mdiag', '--xml', '-j')
  root = ET.fromstring(runcmd('', cmd, echo=False))
  jobs = {}
  for child in root:
    jobid = child.get("JobID")
    qstat = child.get("QueueStatus")
    jobs[jobid] = qstat

  biglock.acquire()
  for jg in tjob:
    if tjob[jg][1]:
      stat = '' 
      for j in tjob[jg][0]:
        stat += jobs.get(j, '-')[0]
      oldstat = gstat.get(jg, '');
      if stat != oldstat:
        msg('{' + jg + '} status changed;', jglegend, 'was:', oldstat, ' now:', stat)
        gstat[jg] = stat
        if stat == '----':
          tjob[jg][1] = False
  biglock.release()

  if os.path.exists(quitfn): quitflag = True

#-----------------------------------------------------------------------------
# Status thread - call bldstat with 1 sec sleep
#-----------------------------------------------------------------------------
def th_stat():
  while True:
    bldstat()
    time.sleep(1)

#-----------------------------------------------------------------------------
# Job thread - submit and monitor a job
#-----------------------------------------------------------------------------
def th_job(cmd, regex, idx, expect):
  rec = re.compile(regex)
  so = runcmd('', cmd, shell=True, echo=True)
  msg(so)

  s1 = so.find('ID: ')
  if s1 >= 0:
    e1 = so.find('submitted', s1+4);
    if e1 > 0:
      id = addjob(so[s1+4:e1-1])
      idt = '{' + id + '}'
  if s1 < 0 or e1 < 0:
    msg('Error: job IDs not found in command output')

  s2 = so.find('---> msub ')
  if s2 >= 0:
    e2 = so.find('.sh\n', s2+9);
    if e2 >= 0: 
      outfn = so[s2+10:e2] + '.out'
  if s2 < 0 or e2 < 0:
    msg('Error: job file name not found in command output')

  msg(idt,  cmd, '/', regex, '/', idx) 
 
  # Wait for test to start
  while gstat[id] == '----' and not quitflag:
    time.sleep(1)  

  # Wait for specified condition
  stat = gstat[id] 
  while not rec.match(stat) and stat != '----' and not quitflag:
    time.sleep(1)
    stat = gstat[id]

  msg(idt, 'target status met:', jglegend, 'now:', stat)
  if stat != '----' and not quitflag:
    jid = tjob[id][0][idx]
    so = runcmd(idt, ('mjobctl', '-c', jid), rcok=True, stderrok=True) 
    if len(so) > 0: msg(idt, so.strip('\r\n'))
    cancelflag = True
  else:
    cancelflag = False

  # Wait until job ended
  while gstat[id] != '----' and not quitflag:
    time.sleep(1)

  if quitflag:
    msg(idt, 'status', jglegend, 'now:', gstat[id])
    msg(idt, 'quitting')

  check_job(idt, outfn, quitflag, expect if cancelflag else ('P',))

  msg(idt, 'done')
 
#-----------------------------------------------------------------------------
# check_job - examine the output file and categorize results:
#
# N - no output
# P - some output but no completion message
# S - job reaches completion with "RESULT: SUCCESS" message 
# F - job reaches completion with "RESULT: FAILURE" message
# k - an optional modifier, indicates failed, signal or killed message
# q - an optional modifier, indicates test terminated by quit  
#
# Valid expect codes are one of N, P, S or F followed by an optional k and/or q.
# i.e.: (N|P|S|F) [k][q] 
#
# If the result is one of the expect codes, then the test passes, else fails 
#-----------------------------------------------------------------------------
def check_job(tag, fn, quit, expect):
  msg(tag, 'Checking', fn)
  msg(tag, 'Expected:', expect)
  
  if not os.path.exists(fn): 
    res = 'N'
  else:
    so = runcmd(tag, 'egrep "RESULT:|failed|signal|killed" ' + fn, shell=True, echo=True, rcok=True)
    msg(tag, so)
    if so.find('RESULT: FAILURE') >= 0: res = 'F'
    elif so.find('RESULT: SUCCESS') >= 0: res = 'S'
    else: res = 'P'
    if so.find('failed') >= 0 or so.find('signal') >= 0 or so.find('killed') >= 0:
      res = res + 'k'
  
  if quit: res = res + 'q'
  
  if res in expect:
    word = 'Passed;'
  else:
    word = 'Failed;'

  msg(tag, 'Cantest result: Test', word, 'result actual:', res, 'expected:', expect)  

#-----------------------------------------------------------------------------
# st_job - launches some job threads
#
# Arguments:
#   cmd    - the command string to launch the test program
#   regex  - a python regular expression to match with the current job
#            group status prior to issuing a cancel
#   idxl   - a list or tuple of 1 or more job indices.  Each one causes
#            a test case to be launched that will cancel that job when the
#            status regex is matched.
#            0 = stage-in  1 = tracking 2 = compute 3 = stage-out
#   expect - a list or tuple of passing result codes.  See comments on 
#            check_job function for a description
#-----------------------------------------------------------------------------
def st_job(cmd, regex, idxl, expect):
  for idx in idxl:
    jt = threading.Thread(target=th_job, args=(cmd, regex, idx, expect) )
    jt.start()


#-----------------------------------------------------------------------------
# Globals
#-----------------------------------------------------------------------------
quitfn = 'cantest.quit'

mw = MessageWriter(timestamp=True, caller=False)
msg = mw.write

# tjob - dictionary of tracked job ID's.  Key is compute job, entry is
# list containing 0) a tuple of all 4 job IDs (stagein, tracking, compute, stageout)
# 1) job active flag, 2) time of last status change
tjob = {}
jglegend = 'ITCO'  # Add status legend to messages

# gstat - dictionary of job group status.  Key is tracking job, entry is
# first letter of QueueStatus for each job in tjob or '-'.
gstat = {}

biglock = threading.Lock()
quitflag=False

def main():
  if os.path.exists(quitfn): os.remove(quitfn)
  #-----------------------------------------------------------------------------
  # Start status thread
  #-----------------------------------------------------------------------------
  st = threading.Thread(target=th_stat)
  st.daemon = True # daemon meand terminate when all other threads are done
  st.start()

  #-----------------------------------------------------------------------------
  # Start test threads - see comments on st_job function for a description
  #-----------------------------------------------------------------------------

  # Initial test - cancel compute job when stage-in becomes active
  #st_job('./run02 -n 1 -s s -w 2 -b', 'abbb', (2,), 'N' )

  # Tests for all phases of job group progress -- not certain the expected 
  # result are correct.
  st_job('./run02 -n 1 -s s -w 2 -b', 'ebbb', (0, 1, 2, 3), ('N',) )
  st_job('./run02 -n 1 -s s -w 2 -b', 'abbb', (0, 1, 2, 3), ('N',) )
  st_job('./run02 -n 1 -s s -w 2 -b', '-aeb', (1, 2, 3),    ('S', 'P') )
  st_job('./run02 -n 1 -s s -w 2 -b', '-aab', (1, 2, 3),    ('S', 'P') )
  st_job('./run02 -n 1 -s s -w 2 -b', '-a-b', (1, 3),       ('S', 'P') )
  st_job('./run02 -n 1 -s s -w 2 -b', '---a', (3,),         ('S', 'Sk') )

main()

# --- end of cantest.py ---
