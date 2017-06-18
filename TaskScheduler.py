import re
import argparse
import time
import datetime

import importlib
from pytz import timezone
import pytz

import pymongo as pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import config as config

import os
import sys
import multiprocessing

DEFAULT_TIME_ZONE = 'America/Los_Angeles'
TS_PATTERN = "%Y-%m-%d %H:%M:%S"
CHECK_JOB_INTERVAL = 5

JOB_PREPARING = 0
JOB_RUNNING = 1
JOB_DONE = 2
JOB_DEPENDING = 3
JOB_READY = 4

JOB_STATUS = [
  'PREPARING',
  'RUNNING',
  'DONE',
  'DEPENDING',
  'READY',
]

SCHEDULERRUNNING = 0
SCHEDULERSTOPPED = 1
SCHEDULERSTATUS =[
  'RUNNING',
  'STOPPED',
]

TaskSchedulerName = 'TaskSchedulerName.py'

def isHelpOptionOn(paras):
  return '-h' in paras or '--help' in paras


  
def parseCMDAddTask(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd
  parser = argparse.ArgumentParser(prog=cmd_line)
  parser.add_argument('--entry', required=True)
  parser.add_argument('--name')
  parser.add_argument('--at',
                      metavar='HH:MM',
                      help='time to schedule a task, e.g., 07:00')
  parser.add_argument('--repeat',
                      default=1)
  parser.add_argument('--interval',
                      default=0)
  parser.add_argument('--days',
                      metavar='Mon,Tue,Wed,Thu,Fri,Sat,Sun',
                      default='Mon,Tue,Wed,Thu,Fri',
                      help='default:,Mon,Tue,Wed,Thu,Fri')
  parser.add_argument('--timezone',
                      metavar='e.g., America/Los_Angeles',
                      default='America/Los_Angeles',
                      help='default: America/Los_Angeles')
  parser.add_argument('--dependon',
                      metavar='e.g.,taskid1,taskid2,...',
                      help='e.g., taskid1,task2,...')

  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    args = parser.parse_args(paras)
    TaskScheduler().addTask(args)
  

def parseCMDRemoveJobs(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd  
  parser = argparse.ArgumentParser(prog=cmd_line)    
  parser.add_argument('--id',
                      default='all')
  parser.add_argument('--name', default=None)

  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    args = parser.parse_args(paras)
    if args.name is not None:
      TaskScheduler().removeJobByName(args.name)
    else:
      TaskScheduler().removeJobs(args.id)
  

def parseCMDRemoveTasks(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd  
  parser = argparse.ArgumentParser(prog=cmd_line)    
  parser.add_argument('--id',
                      default='all')
  parser.add_argument('--name', default=None)

  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    args = parser.parse_args(paras)
    if args.name is not None:
      TaskScheduler().removeTaskByName(args.name)
    else:
      TaskScheduler().removeTasks(args.id)


def parseCMDStart(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd  
  parser = argparse.ArgumentParser(prog=cmd_line)
  parser.add_argument('--dryrun', action='store_true')
  parser.add_argument('--logtype', default='stdout')
  
  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    args = parser.parse_args(paras)
    scheduler = TaskScheduler(logType=args.logtype)
    scheduler.start(args.dryrun)
    scheduler.cleanup()
  
  
def parseCMDShowTasks(cmd, paras):
  TaskScheduler().showTasks()  


def parseCMDShowJobs(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd  
  parser = argparse.ArgumentParser(prog=cmd_line)    
  parser.add_argument('--timezone',
                      metavar='e.g., America/Los_Angeles',
                      default='America/Los_Angeles',
                      help='default: America/Los_Angeles')
  
  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    args = parser.parse_args(paras)
    TaskScheduler().showJobs(args.timezone)

  
def parseCMDShowHistory(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd  
  parser = argparse.ArgumentParser(prog=cmd_line)    
  parser.add_argument('--timezone',
                      metavar='e.g., America/Los_Angeles',
                      default='America/Los_Angeles',
                      help='default: America/Los_Angeles')

  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    args = parser.parse_args(paras)
    TaskScheduler().showJobsHist(args.timezone)

  
def parseCMDStop(cmd, paras):
  cmd_line = TaskSchedulerName + ' ' + cmd  
  parser = argparse.ArgumentParser(prog=cmd_line)

  if isHelpOptionOn(paras):
    parser.print_usage()
  else:
    TaskScheduler().stop()      


COMMANDS = {
  'addtask'     : parseCMDAddTask,
  'removejobs'  : parseCMDRemoveJobs,
  'removetasks' : parseCMDRemoveTasks,
  # 'reset'       : parseCMDReset,
  'start'       : parseCMDStart,
  'showtasks'   : parseCMDShowTasks,
  'showjobs'    : parseCMDShowJobs,
  'showhistory' : parseCMDShowHistory,
  'stop'        : parseCMDStop,
}


def getUTCNow():
  return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

  
def getLocalNow(tz=DEFAULT_TIME_ZONE):
  return getUTCNow().astimezone(timezone(tz))
  

def convertUTC2Local(utcNow, tz=DEFAULT_TIME_ZONE):
  utcNow = utcNow.replace(tzinfo=pytz.utc)
  return utcNow.astimezone(timezone(tz))


def convertLocal2UTC(aTime):
  return aTime.astimezone(pytz.utc)
  

def getMidnight(aTime):
  return aTime.replace(hour=0, minute=0, second=0, microsecond=0)


def setHourMinSec(aTime, val, formatStr):
  try:
    hhmmss = datetime.datetime.strptime(val, formatStr)
    return aTime.replace(hour=hhmmss.hour, minute=hhmmss.minute,\
                         second=hhmmss.second, microsecond=0)
  except:
    return None
    

def getWeekday(aTime):
  return aTime.strftime('%a')

  
def prettyTime(aTime):
  return aTime.strftime(TS_PATTERN)


def mod_func_name(funcPath):
  mname = None
  fname = funcPath
  
  midx = funcPath.rfind('.')
  if midx != -1:
    mname = funcPath[:midx]
    fname = funcPath[midx+1:]
    
  return (mname, fname)


    
  
class Task(object):
  def __init__(self):
    self.message = None
    self.logPath = None

  def getMessage(self):
    return self.message
    
    
  def setMessage(self, val):
    self.message = val


  def appendMessage(self, val):
    self.message += val


  def getLogPath(self):
    return self.logPath


  def setLogPath(self, val):
    self.logPath = val
  



class Job(multiprocessing.Process):
  def __init__(self, spec):
    super(Job, self).__init__()
    self.spec = spec


  def run(self):
    dbMgr = TaskSchedulerDBMgr()
    if not dbMgr.checkConnection:
      print 'Error job {0} cannot be started due to db connection failure'.format(self.spec['entry'])
      return None

    startTime = getUTCNow()
    (m, f) = mod_func_name(self.spec['entry'])
    message = ""
    logPath = None
    normal = True
    
    try:
      my_module = importlib.import_module(m)
      inst = getattr(my_module, f)()
      inst.start()
      rst = dbMgr.setJobAttr(self.spec['_id'], 'status', JOB_DONE)
      if rst == 0:
        normal = False
        message = "Error: job update done failed {0} {1}".format(self.spec['_id'], self.spec['entry'])
        
      message += inst.getMessage()
      logPath = inst.getLogPath()
    except:
      print sys.exc_info()
      message = 'Error: msg={0}'.format(sys.exc_info())
      normal = False

    endTime = getUTCNow()
    dbMgr.saveJobHist(self.spec['_id'], startTime, endTime, normal, message, logPath)
    
    # check whether it has dependents
    dependents = dbMgr.getJobDependents(self.spec['_id'])
    for d in dependents:
      cnt = dbMgr.decrJobDependCount(d['_id'])
      if cnt == 0:
        dbMgr.setJobAttr(d['_id'], 'status', JOB_READY)


        
    
class TaskSchedulerDBMgr(object):
  def __init__(self):
    self.status = True
    self.db = None
    self.status = False
    self.initDBConnection()
    

  def __checkDBVital(self):
    for i in range(3):
      try:
        tsStatus = self.getTaskSchedulerStatus()
        self.status = True
        return True
      except:
        print 'Error: DB vital check failed'
        print sys.exc_info()
        self.initDBConnection()
        time.sleep(8)
    self.status = False
    return False

    
  def initDBConnection(self):
    try:
      client = MongoClient(config.MONGO_URL,
                           serverSelectionTimeoutMS=5)
      client.server_info()
      self.db = client.taskscheduler
      self.status = True
    except pymongo.errors.ServerSelectionTimeoutError as err:
      print err.message
      self.status = False
    

  def checkConnection(self):
    return self.status
    

  def getTaskSchedulerStatus(self):
    objs = self.db.taskscheduler.find()
    if objs.count() == 0:
      return SCHEDULERSTOPPED
    else:
      return objs[0]['status']


  def resetTaskScheduler(self, spec):
    if not self.__checkDBVital():
      return None

    self.db.taskscheduler.remove({})
    return self.db.taskscheduler.insert_one(spec).inserted_id
    
    
  def setTaskSchedulerStatus(self, status):
    if not self.__checkDBVital():
      return None

    objs = self.db.taskscheduler.find()
    if objs.count() == 0:
      return
      
    ts = objs[0]
    result = self.db.taskscheduler.update_one(
      {'_id': ts['_id']},
      {
        '$set': {'status': status}
      }
    )

    
  def addTask(self, taskInfo):
    if not self.__checkDBVital():
      return None

    return self.db.tasks.insert_one(taskInfo).inserted_id


  def getJobDependents(self, jobID):
    if not self.__checkDBVital():
      return None
    
    jobs = self.db.jobs.find({'status': JOB_DEPENDING, 'depend':jobID})
    if jobs.count() == 0:
      return []

    return [job for job in jobs]
    

  def getJobByStatus(self, status):
    if not self.__checkDBVital():
      return None

    jobs = self.db.jobs.find({'status' : status})
    if jobs.count() == 0:
      return []

    return [job for job in jobs]


  def getNumOfJobs(self):
    if not self.__checkDBVital():
      return None

    return self.db.jobs.find().count()
    

  def getNumOfWaitingJobs(self):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.find({
      'status' : {'$ne' : JOB_RUNNING}
    }).count()

    
  def getTaskbyID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    return self.db.tasks.find_one({'_id': ObjectId(str(taskID))})

    
  def existTaskID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    try:
      return self.db.tasks.find_one({'_id': ObjectId(str(taskID))}) != None
    except:
      return False
    
    
  def getTaskByName(self, name):
    if not self.__checkDBVital():
      return None
    
    return self.db.tasks.find_one({'name': name})


  def getJobByID(self, jobID):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.find_one({'_id': ObjectId(str(jobID))})

    
  def getNextJob(self):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.find_one(sort=[("at", 1)])


  def getNextPreparingJob(self):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.find_one({'status': JOB_PREPARING}, sort=[("at", 1)])

    
  def getActiveJobs(self):
    if not self.__checkDBVital():
      return None
    
    jobs = self.db.jobs.find({'status': {'$in': [JOB_PREPARING, JOB_RUNNING, JOB_DEPENDING]}})

    if jobs.count() == 0:
      return []
      
    return [job for job in jobs.sort('at', 1)]
    
    
  def getTasks(self):
    if not self.__checkDBVital():
      return None
    
    tasks = self.db.tasks.find()
    if tasks.count() == 0:
      return []
      
    return [task for task in tasks]


  def getTaskIDs(self):
    if not self.__checkDBVital():
      return None
    
    return [str(task['_id']) for task in self.db.tasks.find({}, {'_id': 1})]


  def addJob(self, jobInfo):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.insert_one(jobInfo).inserted_id


  def getJobs(self):
    if not self.__checkDBVital():
      return None
    
    jobs = self.db.jobs.find()
    if jobs.count() == 0:
      return []

    return [job for job in jobs.sort('at', 1)]


  def getJobsByTaskID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    jobs = self.db.jobs.find({'taskID': taskID})

    if jobs.count() == 0:
      return []

    return [job for job in jobs.sort('at', 1)]

    
  def getJobsHistByTaskID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    jobsHist = self.db.jobsHist.find({'taskID': ObjectId(str(taskID))})

    if jobsHist.count() == 0:
      return []

    return [jobHist for jobHist in jobsHist.sort('startTime: 1')]
      
    
  def getJobsHist(self):
    if not self.__checkDBVital():
      return None
    
    jobsHist = self.db.jobsHist.find()
    if jobsHist.count() == 0:
      return []

    return [jh for jh in jobsHist.sort('startTime')]

    
  def removeJobs(self):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.delete_many({}).deleted_count


  def removeJobByID(self, jobID):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.delete_one({'_id': ObjectId(jobID)}).deleted_count


  def removeJobByName(self, name):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.delete_many({'name': name}).deleted_count

    
  def removeJobByTaskID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.delete_many({'taskID': ObjectId(str(taskID))}).deleted_count


  def removeNonActiveJobByTaskID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobs.delete_many({
      '$and': [
        {'taskID': ObjectId(str(taskID))},
        {'status': JOB_PREPARING}]
    }).deleted_count
 
   
  def removeTasks(self):
    if not self.__checkDBVital():
      return None
    
    return self.db.tasks.delete_many({}).deleted_count


  def removeTaskByID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    return self.db.tasks.delete_one({'_id': ObjectId(taskID)}).deleted_count


  def removeTaskByName(self, name):
    if not self.__checkDBVital():
      return None
    
    return self.db.tasks.delete_one({'name': name}).deleted_count

    
  def removeJobsHist(self):
    if not self.__checkDBVital():
      return None
    
    return self.db.jobsHist.delete_many({}).deleted_count


  def removeJobHistByID(self, jobID):
    if not self.__checkDBVital():
      return None
    
    return self.jobsHist.delete_one({'_id': jobID}).deleted_count

    
  def removeJobHistByTaskID(self, taskID):
    if not self.__checkDBVital():
      return None
    
    return self.jobsHist.delete_many({'taskID': taskID}).deleted_count


  def setJobAttr(self, jobID, field, value):
    if not self.__checkDBVital():
      return None
    
    result = self.db.jobs.update_one(
      {'_id': jobID},
      {
        '$set': {field: value}
      }
    )
    return result.matched_count

    

  def decrJobDependCount(self, jobID):
    if not self.__checkDBVital():
      return None
    
    job = self.getJobByID(jobID)

    if job is None:
      return -1

    count = job['dependCount']

    if count == 0:
      return -1

    count -= 1
    result = self.db.jobs.update_one(
      {'_id': jobID},
      {
        '$set' : {'dependCount' : count}
      }
    )

    return count

    
  def saveJobHist(self, jobID, startTime, endTime, normal, message, \
                  logPath):
    if not self.__checkDBVital():
      return None
    
    job = self.getJobByID(jobID)
    self.removeJobByID(jobID)
    
    job['startTime'] = startTime
    job['endTime'] = endTime
    if startTime is None or endTime is None:
      job['duration'] = None
    else:
      job['duration'] = (endTime - startTime).seconds
    job['normal'] = normal
    job['message'] = message
    job['logpath'] = logPath
    return self.db.jobsHist.insert_one(job).inserted_id    


    
    
class TaskScheduler(object):
  def __init__(self, logType='stdout'):
    self.dbMgr = TaskSchedulerDBMgr()
    self.initJobList = False
    self.jobProcessList = set()
    self.lastResetTime = None
    self.id = None
    self.logType = logType
    self.logFile = None
    self.__setLogFileHandle()
    self.footprint_ts = getUTCNow()
    
    
  @staticmethod
  def usage():
    print 'usage: {0} [command]'.format(sys.argv[0])
    for cmd in COMMANDS.keys():
      print '  {0}'.format(cmd)
    exit()


  def __setLogFileHandle(self):
    if self.logType == 'stdout':
      self.logFile = sys.stdout
    else:
      if self.logFile is not None:
        self.logFile.close()
        
      fileName = os.environ['HOME'] + config.LOG_DIRECTORY + '/scheduler_' + \
                 datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S") + \
                 '.log'
      self.logFile = open(fileName, 'w')


  def __writeLog(self, val):
    self.logFile.write(val)


  def __writelnLog(self, val):
    self.logFile.write("{0}\n".format(val))
    
    
  def cleanup(self):
    if self.logType != 'stdout':
      self.logFile.close()
      
    
  def __checkDBVital(self):
    return self.dbMgr.checkConnection()

    
  def __resetChecking(self, tz=DEFAULT_TIME_ZONE):
    utcNow = getUTCNow()
    
    if self.lastResetTime and \
       (utcNow - self.lastResetTime).total_seconds() <= 3600:
      # donot do reset checking in an hour since the last reset time
      return False
      
    localNow = convertUTC2Local(utcNow, tz)
    localMidnight = getMidnight(localNow)
    seconds = (localNow - localMidnight).total_seconds()
    
    # it is important to reset the time after passing midnight. Otherwise,
    # schedule new job createJobs will get wrong timestamp
    if seconds < 120:
      self.__setLogFileHandle()
      self.lastResetTime = utcNow
      return True
    else:
      return False
      

  def __footprint(self):
    curr = getUTCNow()
    if (curr - self.footprint_ts).seconds > 60:
      self.__writelnLog("--{0}".format(convertUTC2Local(curr).strftime(TS_PATTERN)))
      self.footprint_ts = curr


  def __printTaskGraph(self, taskGraph):
    print '***task graph ***'
    for taskID in taskGraph.keys():
      task = self.getTaskByID(taskID)
      print task['entry'], '-->',

      children = taskGraph[taskID]
      for c in children:
        task = self.getTaskByID(c)
        print task['entry'],

      print
    print '*****************'
    
  def createJobs(self, taskIDs, dryrun=False):
    # Use linkedList to represent task dependent graphs
    taskGraph = {}
    noparents = []
    for taskID in taskIDs:
      if taskID not in taskGraph.keys():
        taskGraph[taskID] = []
      
      task = self.getTaskByID(taskID)
      if task['dependon'] is None:
        noparents.append(taskID)
      else:
        parents = task['dependon'].split(',')
        for p in parents:
          if p not in taskGraph.keys():
            taskGraph[p] = []
          taskGraph[p].append(taskID)

    taskScanOrder = []
    taskQueue = list(noparents)    
    while len(taskQueue) > 0:
      curr = taskQueue.pop(0)
      taskScanOrder.append(curr)

      children = taskGraph[curr]
      if len(children) != 0:
        for c in children:
          # if c is added by a parent at a smaller depth
          if c in taskScanOrder:
            taskScanOrder.remove(c)
            
          # if c has been added by another parent before
          if c in taskQueue:
            taskQueue.remove(c)
          taskQueue.append(c)

    # create task-job mapping only for non-repeated tasks. They are needed
    # for dependent tasks lookup
    task2Job = dict()
    for taskID in taskScanOrder:
      self.createJob4TaskID(taskID, task2Job, dryrun)
      
    
  def createJob4TaskID(self, taskID, task2Job, dryrun):
    task = self.dbMgr.getTaskbyID(taskID)
    
    if task['dependon'] is not None:
      parents = task['dependon'].split(',')
      parentJobs = []
      for p in parents:
        if p in task2Job.keys():
          parentJobs.append(task2Job[p])
        else:
          self.__writelnLog('--{0} missing parent job {1}'.format(task['entry'], p))
          return
        
      job = {
          'taskID': ObjectId(taskID),
          'at'    : None,
          'entry' : task['entry'],
          'status': JOB_DEPENDING,
          'depend': parentJobs,
          'dependCount': len(parents),
      }
      jobID = self.dbMgr.addJob(job)
      job['_id'] = jobID
      self.__writelnLog('--dependent job added ' + self.__prettyJob(job))
      task2Job[taskID] = jobID
    else:
      localTime = getLocalNow(task['timezone'])
      localTime = setHourMinSec(localTime, task['at'], '%H:%M:%S')
      
      if localTime is None:
        self.__writelnLog('Error: wrong time {0} for task {1}'.format(task['at'], task['name']))
        return 0
  
      if not dryrun and getWeekday(localTime) not in task['days'].split(','):
        self.__writelnLog('Skip task {0} on {1}'.format(task['name'], getWeekday(localTime)))
        return 0
  
      jobTime = convertLocal2UTC(localTime)
      currTime = getUTCNow()

      for i in range(task['repeat']):
        if not dryrun and jobTime < currTime:
          self.__writelnLog('--Skip job {0} [{1}] [{2}]'.format(task['name'], prettyTime(convertUTC2Local(jobTime)), prettyTime(convertUTC2Local(currTime))))
          jobTime += datetime.timedelta(minutes=task['interval'])
          continue
  
        if ',' in task['entry']:
          entries = task['entry'].split(',')
        else:
          entries = [task['entry']]
         
        root_entry = entries[0]
        job = {
          'taskID': ObjectId(taskID),
          'at'    : jobTime,
          'entry' : root_entry,
          'status': JOB_PREPARING,
          'depend': None,
          'dependCount': 0,
        }
        jobID = self.dbMgr.addJob(job)

        # only save the mapping for non-repeated jobs
        if task['repeat'] == 1:
          task2Job[taskID] = jobID
          
        job['_id'] = jobID
        self.__writelnLog('--job added ' + self.__prettyJob(job))
        jobTime += datetime.timedelta(minutes=task['interval'])
  
        # check whether we have dependent jobs
        entries.pop(0)
        dependOnJobID = jobID
        for entry in entries:
          job = {
            'taskID': ObjectId(taskID),
            'at'    : None,
            'entry' : entry,
            'status': JOB_DEPENDING,
            'depend': dependOnJobID,
            'dependCount' : 1
          }
          jobID = self.dbMgr.addJob(job)
          job['_id'] = jobID
          self.__writelnLog('--dependent job added ' + self.__prettyJob(job))
          dependOnJobID = jobID
  

  def runJob(self, job):
    rst = self.dbMgr.setJobAttr(job['_id'], 'status', JOB_RUNNING)
    
    if rst == 0:
      message = "Error: job update running failed {0} {1}".format(self.spec['_id'], self.spec['entry'])
      self.dbMgr.saveJobHist(job['_id'], None, None, False, message, None)
    else:
      self.__writelnLog('--run job {0} at {1}'.format(job['entry'], prettyTime(getLocalNow())))
      Job(job).start()
    
    
  def stop(self):
    self.dbMgr.setTaskSchedulerStatus(SCHEDULERSTOPPED)
    
      
  def start(self, dryrun=False):
    if not self.__checkDBVital():
      return None
      
    prevTaskIDs = set(self.dbMgr.getTaskIDs())
    ts = {
      'startTime': getUTCNow(),
      'status'   : SCHEDULERRUNNING
    }

    self.id = self.dbMgr.resetTaskScheduler(ts)

    while(self.dbMgr.getTaskSchedulerStatus() == SCHEDULERRUNNING):
      self.__footprint()
      if not self.__checkDBVital():
        self.__writelnLog('Error: db connection failure')
        break

      # initialize or reset job list
      if self.initJobList is False or self.__resetChecking():
        self.initJobList = True
        self.dbMgr.removeJobs()
        self.createJobs(prevTaskIDs, dryrun)
        continue

      currTaskIDs = set(self.dbMgr.getTaskIDs())
      if prevTaskIDs == currTaskIDs:
        # no newly added or deleted tasks
        job = self.dbMgr.getNextPreparingJob()

        if dryrun and job is None and \
           self.dbMgr.getNumOfWaitingJobs() == 0:
          self.__writelnLog('dryrun no more job. Wait until active jobs finish')
          while self.dbMgr.getNumOfJobs() > 0 and \
                self.dbMgr.getTaskSchedulerStatus() == SCHEDULERRUNNING:
            jobs = self.dbMgr.getJobs()
            self.__writelnLog('{0} job[s] active {1}'.format(len(jobs), [x['entry'] for x in jobs]))
            time.sleep(CHECK_JOB_INTERVAL)

          self.dbMgr.setTaskSchedulerStatus(SCHEDULERSTOPPED)
          self.__writelnLog('Dryrun done')
          break

        if job:
          self.__writelnLog('--curr: {0}'.format(self.__prettyJob(job)))
          currTime = getUTCNow()
          jobTime = job['at'].replace(tzinfo=pytz.utc)
          delta = abs(currTime - jobTime).total_seconds()

          if dryrun:
            currTime = jobTime
            delta = 0

          if currTime >= jobTime or delta <= 1:
            if (delta > 10):
              message = 'delayed job {0} curr={1} job={2} delta={3}'.format(job['entry'], convertUTC2Local(currTime), convertUTC2Local(jobTime), delta)
              self.dbMgr.saveJobHist(job['_id'], None, None, False, message, None)
            else:
              self.runJob(job)
          else:
            sleepTime = min(delta, CHECK_JOB_INTERVAL)
            time.sleep(sleepTime)
          # end if job
      else:
        # get removed tasks
        removedTaskIDs = prevTaskIDs - currTaskIDs
        for taskID in removedTaskIDs:
          self.__writelnLog('remove task {0} jobs'.format(taskID))
          rst = self.dbMgr.removeNonActiveJobByTaskID(taskID)
          self.__writelnLog('remove ' + str(rst))

        # add jobs for new tasks
        newTaskIDs = currTaskIDs - prevTaskIDs
        self.__writelnLog('new' + str(newTaskIDs))

        for taskID in newTaskIDs:
          self.__writelnLog('add new task {0}'.format(taskID))
          task = self.dbMgr.getTaskbyID(taskID)
          assert task is not None
          task2Job = dict()
          if task['dependon'] is not None:
            parents = task['dependon'].split(',')
            for p in parents:
              jobs = self.getJobsByTaskID(p)
              assert len(jobs) <= 1
              if len(jobs) == 1:
                task2Job[p] = jobs[0]['_id']
          self.createJob4TaskID(taskID, task2Job, dryrun)

        time.sleep(CHECK_JOB_INTERVAL)
      # end else

      # check whether any dependent jobs are ready to run
      readyJobs = self.dbMgr.getJobByStatus(JOB_READY)
      for rj in readyJobs:
        self.runJob(rj)
        
      prevTaskIDs = currTaskIDs
    # end while
      

  def addTask(self, args):
    name = args.entry if args.name is None else args.name

    if args.at is None:
      if args.dependon is None:
        self.__writelnLog("Error: specify a time for your task")
        return False
    else:
      pattern = re.compile('^[0-2][0-9]:[0-9][0-9]$')      
      if pattern.match(args.at):
        args.at += ":00"
      else:
        pattern = re.compile('^[0-2][0-9]:[0-9][0-9]:[0-9][0-9]$')
        if not pattern.match(args.at):
          self.__writelnLog("Error: unrecognized time pattern {0}".format(args.at))
          return False
      
    if args.repeat > 1:
      if args.interval <= 0:
        self.__writelnLog("Error: when --repeat, --interval is mandatory")
        return False

    try:
      tz = timezone(args.timezone)
    except:
      self.__writelnLog('unknown timezone {0}'.format(task['timezone']))
      return False

    if args.name is None:
      args.name = name
      
    if ',' in args.entry:
      entries = args.entry.split(',')
    else:
      entries = [args.entry]

    # check whether m.f() can be found in python PATH
    for entry in entries:
      (m, f) = mod_func_name(entry)
      if m is None:
        self.__writelnLog("Error: missing module name")
        return False
  
      try:
        aModule = importlib.import_module(m)
      except:
        self.__writelnLog("Error: cannot find module {0}".format(m))
        return False
  
      if not hasattr(aModule, f):
        self.__writelnLog("Error: cannot find function {0} inside module {1}".format(f, m))
        return False
  
    task = self.dbMgr.getTaskByName(name)
    if task:
      self.__writelnLog("Error: task name {0} exists".format(name))
      return False

    # check conditions for dependent tasks
    if args.dependon is not None:
      parents = args.dependon.split(',')
      invalid_parents = []
      for taskID in parents:
        if not self.dbMgr.existTaskID(taskID):
          invalid_parents.append(taskID)

      if len(invalid_parents) != 0:
        self.__writelnLog("Error: parent task[s] {0} not found".format(', '.join(invalid_parents)))
        return False

    taskInfo = vars(args)
    taskInfo['valid'] = True
    taskInfo['repeat'] = int(taskInfo['repeat'])
    taskInfo['interval'] = int(taskInfo['interval'])

    self.dbMgr.addTask(taskInfo)


  def getTaskByName(self, name):
    return self.dbMgr.getTaskByName(name)

    
  def getTaskByID(self, taskID):
    if not self.__checkDBVital():
      return None

    return self.dbMgr.getTaskbyID(taskID)

    
  def getTasks(self):
    if not self.__checkDBVital():
      return []
    
    return self.dbMgr.getTasks()
    
    
  def showTasks(self):
    taskList = self.getTasks()
    if len(taskList) == 0:
      self.__writelnLog('No tasks found')
      return
      
    for task in taskList:
      self.__writelnLog("{0} {1} {2} {3} {4} {5} {6} {7} {8}".format(
        task['_id'],
        task['name'],
        task['entry'],
        task['at'],
        task['repeat'],
        task['interval'],
        task['days'],
        task['timezone'],
        task['dependon']))


  def __prettyJob(self, job, tz=DEFAULT_TIME_ZONE, verbose=False):
    if job is None:
      return None
    at = None
    if job['at'] is not None:
      at = convertUTC2Local(job['at'], tz).strftime(TS_PATTERN);
    
    return "[{0}, {1}, {2}, {3}, {4}, {5}]".format(
      job['_id'],
      JOB_STATUS[job['status']],
      job['taskID'],
      at,
      job['depend'],
      job['dependCount'])



  def __prettyJobHist(self, jobHist, tz=DEFAULT_TIME_ZONE):
    if jobHist is None:
      return None

    at = None
    if jobHist['at'] is not None:
      at = convertUTC2Local(jobHist['at'], tz)

    startTime = None
    if jobHist['startTime'] is not None:
      startTime = convertUTC2Local(jobHist['startTime'], tz).strftime(TS_PATTERN),

    endTime = None
    if jobHist['endTime'] is not None:
      endTime = convertUTC2Local(jobHist['endTime'], tz).strftime(TS_PATTERN),
      
    return "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}".format(
      jobHist['_id'],
      jobHist['taskID'],
      at,
      jobHist['entry'],
      startTime,
      endTime,
      jobHist['duration'],
      jobHist['normal'],
      jobHist['message'],
      jobHist['logpath'])


  def getJobs(self):
    return self.dbMgr.getJobs()
    
    
  def showJobs(self, tz):
    jobList = self.getJobs()
    if len(jobList) == 0:
      self.__writelnLog('No job found')
      return
      
    for job in jobList:
      self.__writelnLog(self.__prettyJob(job, tz))


  def getJobsByTaskID(self, taskID):
    return self.dbMgr.getJobsByTaskID(taskID)

    
  def getJobsHistByTaskID(self, taskID):
    return self.dbMgr.getJobsHistByTaskID(taskID)

    
  def getJobsHist(self):
    return self.dbMgr.getJobsHist()


  def showJobsHist(self, tz):
    jobHistList = self.getJobsHist()
    
    if len(jobHistList) == 0:
      self.__writelnLog('No job history found')
      return
      
    for jobHist in jobHistList:
      self.__writelnLog(self.__prettyJobHist(jobHist, tz))
      

  def removeJobs(self, jobID='all'):
    if jobID == 'all':
      rst = self.dbMgr.removeJobs()
      self.__writelnLog('Info: {0} job[s] removed'.format(rst))
    else:
      if ObjectId.is_valid(jobID):
        rst = self.dbMgr.removeJobByID(jobID)
        self.__writelnLog('Info: {0} job removed'.format(rst))
      else:
        self.__writelnLog('Error: invalid job ID {0}'.format(jobID))


  def removeJobByName(self, name):
    return self.dbMgr.removeJobByName(name)

    
  def removeTasks(self, taskID='all'):
    if taskID == 'all':
      self.dbMgr.removeJobs()
      rst = self.dbMgr.removeTasks()
      self.__writelnLog('Info: {0} task[s] removed'.format(rst))
    else:
      if ObjectId.is_valid(taskID):
        self.dbMgr.removeJobByTaskID(taskID)
        rst = self.dbMgr.removeTaskByID(taskID)
        self.__writelnLog('Info: {0} task removed'.format(rst))
      else:
        self.__writelnLog('Error: invalid task ID {0}'.format(taskID))
  

  def removeTaskByName(self, name):
    return self.dbMgr.removeTaskByName(name)
    
    
  def removeJobsHist(self):
    self.dbMgr.removeJobsHist()


  def hasActiveJjobs(self):
    flag = True
    activeJobs = self.dbMgr.getActiveJobs()
    if len(activeJob) == 0:
      flag = False
    return flag

    
  def hasJobs(self):
    flag = True
    jobs = self.dbMgr.getJobs()
    if len(jobs) == 0:
      flag = False
    return flag


  @staticmethod
  def main(args):
    if len(args) == 1:
      TaskScheduler.usage()
      
    cmd = args[1]
    if cmd not in COMMANDS:    
      print 'Invalid command: ' + cmd
      TaskScheduler.usage()
  
    paras = args[2:]
    cmdFunc = COMMANDS[cmd]
    cmdFunc(cmd, paras)
  


    

if __name__ == '__main__':
  TaskScheduler.main(sys.argv)
