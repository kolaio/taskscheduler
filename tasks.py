import time

from TaskScheduler import Task

class Bar(Task):
  def __init__(self):
    super(Bar, self).__init__()


  def start(self):
    print "Bar"
    self.setMessage('Bar')



class BarError(Task):
  def __init__(self):
    super(BarError, self).__init__()


  def start(self):
    print "BarError"
    self.setMessage('BarError')
    x = 100/0

    

class Foo(Task):
  def __init__(self):
    super(Foo, self).__init__()

    
  def start(self):
    print "Foo"
    time.sleep(3)
    self.setMessage('Foo')
    self.setLogPath('/tmp/foo.log')



class Foo1(Task):
  def __init__(self):
    super(Foo1, self).__init__()

    
  def start(self):
    print "Foo1"
    time.sleep(3)
    self.setMessage('Foo1')
    self.setLogPath('/tmp/foo1.log')


class FooLong(Task):
  def __init__(self):
    super(FooLong, self).__init__()

    
  def start(self):
    print "FooLong start"
    time.sleep(100)
    print "FooLong end"    
    self.setMessage('FooLong')

