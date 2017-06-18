import unittest
import datetime
import threading
import time

from pytz import timezone
from TaskScheduler import TaskScheduler, getLocalNow, getWeekday


def stopWhenNoJobs(delay=0):
  time.sleep(delay)
  ts = TaskScheduler()
  
  while(ts.hasJobs()):
    time.sleep(3)
    
  args = "TaskScheduler.py stop".split()
  TaskScheduler.main(args)
  

def addNewTask(name, entry, delta):
  localNow = getLocalNow()
  local1 = localNow + datetime.timedelta(seconds=delta)
  hhmmss = local1.strftime('%H:%M:%S')
  day = getWeekday(local1)
    
  args = "TaskScheduler.py addtask --name {0} --entry {1} --at {2} --day {3}".format(name, entry, hhmmss, day).split()    
  TaskScheduler.main(args)


def removeTask(name, delay):
  time.sleep(delay)
  args = "TaskScheduler.py removetasks --name {0}".format(name).split()
  TaskScheduler.main(args)
  


class TestTaskScheduler(unittest.TestCase):
  def setUp(TestTaskScheduler):
    ts = TaskScheduler()
    ts.removeTasks()
    ts.removeJobsHist()
    

  def testAddOneTaskLong(self):
    print '**testAddOneTaskLong**'
    
    args = "TaskScheduler.py addtask --entry tasks.FooLong --at 07:00".split()
    TaskScheduler.main(args)

    ts = TaskScheduler()
    task = ts.getTaskByName('tasks.FooLong')
    self.assertIsNotNone(task)

    ts.start(dryrun=True)

    jobsHist = ts.getJobsHistByTaskID(task['_id'])    
    self.assertEqual(len(jobsHist), 1)


  def testAddMultipleTaskLong(self):
    print '**testAddMultipleTaskLong**'
    
    args = "TaskScheduler.py addtask --name Foo1 --entry tasks.Foo --at 07:00".split()
    TaskScheduler.main(args)

    args = "TaskScheduler.py addtask --name Foo2 --entry tasks.Foo --at 06:55".split()
    TaskScheduler.main(args)

    ts = TaskScheduler()
    
    task1 = ts.getTaskByName('Foo1')
    self.assertIsNotNone(task1)

    task2 = ts.getTaskByName('Foo2')
    self.assertIsNotNone(task2)

    ts.start(dryrun=True)
    
    jobsHist = ts.getJobsHist()
    self.assertEqual(len(jobsHist), 2)

    jobsHist1 = jobsHist[0]
    jobsHist2 = jobsHist[1]

    # Make sure the second job scheduled first
    self.assertEqual(jobsHist1['taskID'], task2['_id'])
    self.assertEqual(jobsHist2['taskID'], task1['_id'])

    
  def testAddMultipleTasks(self):
    print '**testAddMultipleTasks**'
    
    args = "TaskScheduler.py addtask --entry tasks.Foo --at 07:00".split()
    TaskScheduler.main(args)

    args = "TaskScheduler.py addtask --entry tasks.Bar --at 12:00".split()
    TaskScheduler.main(args)

    ts = TaskScheduler()
    task1 = ts.getTaskByName('tasks.Foo')
    self.assertIsNotNone(task1)

    task2 = ts.getTaskByName('tasks.Bar')
    self.assertIsNotNone(task2)

    ts.start(dryrun=True)
    
    jobsHist = ts.getJobsHistByTaskID(task1['_id'])    
    self.assertEqual(len(jobsHist), 1)

    jobsHist = ts.getJobsHistByTaskID(task2['_id'])    
    self.assertEqual(len(jobsHist), 1)


  def testAddTasksWithRepeat(self):
    print '**testAddTasksWithRepeat**'
    
    args = "TaskScheduler.py addtask --name Foo1 --entry tasks.Foo --at 07:00 --repeat 3 --interval 2".split()
    TaskScheduler.main(args)

    ts = TaskScheduler()
    task = ts.getTaskByName('Foo1')
    self.assertIsNotNone(task)

    ts.start(dryrun=True)
    jobsHist = ts.getJobsHistByTaskID(task['_id'])
    self.assertEqual(len(jobsHist), 3)
   

  def testTaskWithError(self):
    print '**testTaskWithError**'
    
    args = "TaskScheduler.py addtask --entry tasks.BarError --at 07:00".split()
    TaskScheduler.main(args)

    ts = TaskScheduler()
    task = ts.getTaskByName('tasks.BarError')
    self.assertIsNotNone(task)

    ts.start(dryrun=True)
    jobsHist = ts.getJobsHistByTaskID(task['_id'])
    self.assertEqual(len(jobsHist), 1)
    
    jobHist = jobsHist[0]
    self.assertTrue('exceptions.ZeroDivisionError' in jobHist['message'])

    
  def testDynamicTasks(self):
    print '**testDynamicTasks**'

    localNow = getLocalNow()
    local1 = localNow + datetime.timedelta(seconds=10)
    hhmmss = local1.strftime('%H:%M:%S')
    day = getWeekday(local1)
    
    args = "TaskScheduler.py addtask --entry tasks.FooLong --at {0} --day {1}".format(hhmmss, day).split()    
    TaskScheduler.main(args)

    ts = TaskScheduler()
    task = ts.getTaskByName('tasks.FooLong')
    self.assertIsNotNone(task)

    t1 = threading.Thread(target=stopWhenNoJobs,
                          args=(20,))
    t1.start()
    
    t2 = threading.Thread(target=addNewTask,
                          args=('BarNew', 'tasks.Bar', 20,))
    t2.start()


    t3 = threading.Thread(target=addNewTask,
                          args=('BarOld', 'tasks.Bar', -20,))
    t3.start()


    t4 = threading.Thread(target=addNewTask,
                          args=('FooDeleted', 'tasks.FooLong', 40,))
    t4.start()


    t5 = threading.Thread(target=removeTask,
                          args=('FooDeleted', 10))
    t5.start()
    
    # start taskscheduler
    ts.start()

    tasks = ts.getTasks()
    self.assertEqual(len(tasks), 3)

    jobsHist = ts.getJobsHist()
    self.assertEqual(len(jobsHist), 2)
    
    
  def testSingleParentDependentTasks(self):
    print '**testSingleParentDependentTasks**'

    args = "TaskScheduler.py addtask --name oneparent --entry tasks.Bar,tasks.Foo,tasks.BarError --at 07:00".split()
    
    TaskScheduler.main(args)
    ts = TaskScheduler()
    task = ts.getTaskByName('oneparent')
    self.assertIsNotNone(task)
    
    ts.start(dryrun=True)
    jobsHist = ts.getJobsHistByTaskID(task['_id'])
    self.assertEqual(len(jobsHist), 3)
    
    jh = ts.getJobsHistByTaskID(task['_id'])

    entries = [x['entry'] for x in jh]
    self.assertTrue('tasks.Bar' in entries)
    self.assertTrue('tasks.Foo' in entries)
    self.assertTrue('tasks.BarError' in entries)


  def testMultiParentDependentTasks(self):
    print '**testMultiParentDependentTasks**'

    args = "TaskScheduler.py addtask --entry tasks.Foo --at 07:00".split()
    TaskScheduler.main(args)

    args = "TaskScheduler.py addtask --entry tasks.Foo1 --at 07:00".split()
    TaskScheduler.main(args)

    ts = TaskScheduler()
    taskFoo = ts.getTaskByName('tasks.Foo')
    taskFoo1 = ts.getTaskByName('tasks.Foo1')
    
    args = "TaskScheduler.py addtask --entry tasks.Bar --dependon {0},{1}".format(taskFoo['_id'], taskFoo1['_id']).split()
    TaskScheduler.main(args)
    taskBar = ts.getTaskByName('tasks.Bar')
    
    ts.start(dryrun=True)

    self.assertTrue(ts.getJobsHistByTaskID(taskFoo['_id']))
    self.assertTrue(ts.getJobsHistByTaskID(taskFoo1['_id']))    
    self.assertTrue(ts.getJobsHistByTaskID(taskBar['_id']))

    
    
    
if __name__ == '__main__':
  tests = [
    'testAddMultipleTasks',
    'testAddTasksWithRepeat', 
    'testAddOneTaskLong',
    'testAddMultipleTaskLong',
    'testTaskWithError',
    'testDynamicTasks',
    'testSingleParentDependentTasks',
    'testMultiParentDependentTasks'
  ]
  # tests = ['testSingleParentDependentTasks']
  # tests = ['testAddMultipleTasks']
  # tests = ['testDynamicTasks']
  # tests = ['testMultiParentDependentTasks']

  suite = unittest.TestSuite(map(TestTaskScheduler, tests))
  runner = unittest.TextTestRunner()
  runner.run(suite)
  








