
import sys
import time
import itertools
import inspect
import threading
import multiprocessing
import cProfile
import pycallgraph


def _process_one_step(pipeline):
    u"Process a step of the pipeline, return sub-pipelines and/or wrapped values"
    
    state_changed = False
    results = []
    
    #replace args FutureResults with values
    #TODO do the same for kwargs
    args, kwargs = list(pipeline.args), dict(pipeline.kwargs)
    frs = filter(lambda x:isinstance(x, FutureResult), args)
    dependencies_status = map(lambda x:x.ready(), frs)
    
    if not all(dependencies_status):
        assert "not all dependencies ready yet"
    else:
        for idx, x in enumerate(args):
            if isinstance(x, FutureResult):
                if x.ready():
                    args[idx] = x.value
                else:
                    assert False
                    return "not all dependencies ready yet"

        fr = None
        generator = pipeline.run(*args, **kwargs)
        try:
            #print "yahoo"

            while generator is not None:
                result = generator.send(fr)
                if isinstance(result, Pipeline):
                    fr = FutureResult(result)
                else:
                    fr = FutureResult(None)
                    fr.set(result)
                results.append(fr)
                if fr.pipeline == None:
                    fr = None
                state_changed = True
        
        except StopIteration:
            #print 'StopIteration'
            pass
            
    return results

class FutureResultState:
    NEW = 0
    POPULATING = 1
    POPULATED = 2


class Tree(object):
    def __init__(self, text=''):
        self.children = []
        self._text = text
        
    def __iter__(self):
        u"implement the iterator protocol"
        return itertools.chain(self._isingle(self.value), *map(iter, self.children))

    def iternodes(self):
        return itertools.chain(self._isingle(self), *map(Tree.iternodes, self.children))

    def append(self, value):
        self.children.append(value)

        return self.last_child()

    def last_child(self):
        if len(self.children):
            return self.children[-1]
        else:
            return None

    @property
    def text(self):
        return self._text

    @staticmethod
    def _isingle(item):
        u"iterator that yields only a single value then stops, for chaining"
        yield item

class FutureResult(Tree):
    def __init__(self, pipeline, note=''):
        super(FutureResult, self).__init__()
        self.pipeline = pipeline
        self.value = None
        self._ready = False
        self._state = FutureResultState.NEW
        self._note = note
        #self.debug_frameinfo = inspect.getframeinfo(inspect.currentframe())
        #self.debug_stack = inspect.stack()

    #def __repr__(self):
    #    return "%s, [%s]" % (self._ready, self.value)

    #def materialize(self):
    #    self.value = self.pipeline.process()
    #    self._ready = True
    #    return self.value

    def set(self, value):
        self.value = value
        self._ready = True

    def ready(self):
        return self._ready

    def count_dependencies(self):
        if self._ready:
            return 0

        dependencies = list(self.pipeline.args) + self.pipeline.kwargs.values()
        frs = filter(lambda x: isinstance(x, FutureResult), dependencies)
        frs = filter(lambda x: False == x.ready(), frs)

        #recurse
        sub_dependencies_count = map(FutureResult.count_dependencies, frs)
        return len(frs) + sum(sub_dependencies_count)
    
    def note(self):
        return self._note
    
    @property
    def state(self):
        return self._state
    
    @state.setter
    def state(self, value):
        'setting'
        self._state = value


    
def square_me(x):
    return x*x    

class AsyncResult(object):
    def __init__(self):
        self._result = None

    def get(self, timeout=0):
        return self._result

    def wait(self, timeout):
        return

    def ready(self):
        return True

    def successful(self):
        return True
    
    def _set(self, result):
        self._result = result
            
    
class FakePool(object):
    def __init__(self):
        pass
    
    def apply_async(self, func, args):
        result = AsyncResult()
        result._set(func(*args))
        return result

class Pipeline(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def run(self, *args, **kwargs):
        """
        yeilds sub-pipelines or result, the *last* yield value is processed as result
        """
        assert False
        pass

    def on_finished(self):
        pass

    def process(self, pool=FakePool()):
        
        processing_tree = FutureResult(self, note='root')
        
        state_changed = True
        while state_changed:
            state_changed = False

            for node in processing_tree.iternodes():
                if isinstance(node, FutureResult):
                    if isinstance(node.pipeline, ComplexReductionPipeline):
                        pass

                    if node.state == FutureResultState.NEW:
                        args, kwargs = list(node.pipeline.args), dict(node.pipeline.kwargs)
                        frs = filter(lambda x:isinstance(x, FutureResult), args)
                        dependencies_status = map(lambda x:x.ready(), frs)
    
                        if all(dependencies_status):
                            #node.results = _process_one_step(node.pipeline)
                            node.async_result = pool.apply_async(_process_one_step, (node.pipeline,))
                        
                            #for r in results:
                            #    child = node.append(r)
                            #    if r.pipeline == None:
                            #        child.populated(True)

                            node.state = FutureResultState.POPULATING
                            state_changed = True
                        
                    if node.state == FutureResultState.POPULATING:
                        node.async_result.wait(0.1)
                        if node.async_result.ready():
                            results = node.async_result.get()

                            for r in results:
                                child = node.append(r)
                                if r.pipeline is None:
                                    child.state = FutureResultState.POPULATED
                                else:
                                    #iterate over all arguments and dependencies as needed
                                    for arg in r.pipeline.args:
                                        if isinstance(arg, FutureResult):
                                            sub_child = child.append(arg)
                                            if arg.pipeline is None:
                                                sub_child.state = FutureResultState.POPULATED

                        #if r.pipeline == None:
                            node.state = FutureResultState.POPULATED
                            state_changed = True
                    
                    if node.state == FutureResultState.POPULATED:
                        if not node.ready():
                            #node.async_result.wait(3)
                            #if True: #node.async_result.ready():
                                #state_changed, results = node.async_result.get()
                                #state_changed, results = node.results

                            dependencies_status = map(lambda x: x.ready(), node.children)
                            if len(dependencies_status) == 0 or (len(dependencies_status) > 0 and all(dependencies_status)):
                                node.set(node.last_child().value)
                                state_changed = True
                                node.pipeline.on_finished()
                else:
                    print 'Unexpected'

        return processing_tree.value

class Sum(Pipeline):
    def run(self, *values):
        dummy = yield sum(values)


class Max(Pipeline):
    def run(self, *values):
        dummy = yield max(*values)
        pass


class Multiply(Pipeline):
    def run(self, *values):
        result = 1
        for v in values:
            result *= v
        dummy = yield result
        pass


class CompositePipeline(Pipeline):
    def run(self, *values):
        s = yield Sum(*values)
        m = yield Multiply(*values)
        dummy = yield Max(s, m)
        pass


class DoubleCompositePipeline(Pipeline):
    def run(self, *values):
        s = yield CompositePipeline(*values)
        m = yield CompositePipeline(*values)
        dummy = yield Max(s, m)
        pass


class ComplexPipeline(Pipeline):
    def __init__(self, width, depth, value_to_return):
        return super(ComplexPipeline, self).__init__(width, depth, value_to_return)
    
    def run(self, width, depth, value_to_return):
        if depth > 0:
            for _ in xrange(width):
                dummy = yield ComplexPipeline(width, depth - 1, value_to_return)
        else:
            dummy = yield value_to_return
        pass

class ComplexReductionPipeline(Pipeline):
    def __init__(self, width, depth, value_to_return, reduction_class):
        super(ComplexReductionPipeline, self).__init__(width, depth, value_to_return)
        self.reduction_class = reduction_class
    
    def run(self, width, depth, value_to_return):
        if depth > 0:
            subvalues = []
            for _ in xrange(width):
                subvalues += [(yield ComplexReductionPipeline(width, depth - 1, value_to_return, self.reduction_class))]
            
            yield self.reduction_class(*subvalues)
        else:
            dummy = yield value_to_return
        pass

    def on_finished(self):
        pass

class SleepPipeline(Pipeline):
    def run(self, sleep_time):
        print time.time(), self, "started sleeping for", sleep_time, "seconds"
        time.sleep(sleep_time)
        print time.time(), self, "done sleeping"

        yield None
        pass


class LongProcessingPipeline(Pipeline):
    def run(self, task_number, processing_time):
        for _ in xrange(task_number):
            dummy = yield SleepPipeline(processing_time)
            pass
        pass


def gen_upper():
    value = yield
    while True:
        value = yield value.upper()


def gen_xxxx():
    _ = yield 1
    _ = yield 2
    pass

class StopWatch(object):
    def __init__(self, autostart=False):
        self.start_time = None
        self.stop_time  = None

        if autostart:
            self.start()

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.stop_time = time.time()
    
    def duration(self):
        duration = self.stop_time - self.start_time
        return duration

def main():
    #pycallgraph.start_trace()
    test_fanout()
    #pycallgraph.make_dot_graph(r'C:\Users\iokulist\workspace\pycopipe\test.png')
    #cProfile.run('test1()')
    
    #s = "Hello World"

    #g = gen_upper()
    #dummy = g.send(None)

    #for w in s.split():
    #    print g.send(w)

    #f = gen_xxxx()
    #try:
    #    print f.send(None)
    #    print f.send('a')
    #    f.send('b')
    #except StopIteration:
    #    pass

    #c = CompositePipeline(1, 2, 3)
    #result = c.process()
    #print result

    #c = DoubleCompositePipeline(1, 2, 3)
    ##c.pool = multiprocessing.Pool(2)
    #result = c.process()
    #print result

    return 0


def test1():
    # basic stuff
    assert 6 == Sum(1, 2, 3).process()
    assert 6 == Multiply(1, 2, 3).process()
    assert 3 == Max(1, 2, 3).process()

    fr1 = FutureResult(Sum(1, 2, 3))
    assert 0 == fr1.count_dependencies()
    fr2 = FutureResult(Sum(fr1))
    assert 1 == fr2.count_dependencies()

    assert 127 == ComplexPipeline(1, 100, 127).process()
    assert 127 == ComplexPipeline(2, 2, 127).process()

def test2():
    width = 1
    depth = 0
    sleep_time = 4
    pool = None
    sw = StopWatch(True)
    assert None == ComplexPipeline(width, depth, SleepPipeline(sleep_time)).process()
    sw.stop()
    print 'Duration', sw.duration()
    assert (sw.duration() - 0.1) < width*sleep_time

def test3():
    
    pools = [FakePool(), multiprocessing.Pool()]
    durations = []
    
    for pool in pools:
        width = 2
        depth = 1
        sleep_time = 4
        sw = StopWatch(True)
        assert None == ComplexPipeline(width, depth, SleepPipeline(sleep_time)).process(pool)
        sw.stop()
        
        durations.append(sw.duration())
        
         
    print durations 
    
    #suppose to run in parallel so just sleep_time, giving some slack for debugger/etc
    scale = 1.8
    assert durations[0] > durations[1] * scale

def test_fanout():
    width = 4
    depth = 3
    result = ComplexReductionPipeline(width, depth, 1, Sum).process()
    print result
    pass

if __name__ == '__main__':
    multiprocessing.freeze_support()
    sys.exit(main())


