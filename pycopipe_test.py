import sys
import time
import itertools
import inspect 
import multiprocessing

class FutureResult(object):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self._materialized = False
        self.value = None
        self.debug_frameinfo = inspect.getframeinfo(inspect.currentframe())
        self.debug_stack = inspect.stack()

    def __repr__(self):
        return "%s, [%s]" % (self.materialized, self.value)

    def materialize(self):
        self.value = self.pipeline.process()
        self._materialized = True
        return self.value

    def set(self, value):
        self.value = value
        self._materialized = True

    def ismaterialized(self):
        return self._materialized

    def count_dependencies(self):
        if self._materialized:
            return 0

        dependencies = list(self.pipeline.args) + self.pipeline.kwargs.values()
        frs = filter(lambda x: isinstance(x, FutureResult), dependencies)
        frs = filter(lambda x: False == x.ismaterialized(), frs)

        #recurse
        sub_dependencies_count = map(FutureResult.count_dependencies, frs)
        return len(frs) + sum(sub_dependencies_count)


def future_result_materialize_helper(x):
    return x.materialize()

class Tree:
    def __init__(self, value, populated=False):
        self.value = value
        self.populated = populated
        self.children = []
        
    def __iter__(self):
        u"implement the iterator protocol"
        return itertools.chain(self._isingle(self.value), *map(iter, self.children))

    def iternodes(self):
        return itertools.chain(self._isingle(self), *map(Tree.iternodes, self.children))

    def append(self, value):
        self.children.append(Tree(value))
        return self.last_child()

    def last_child(self):
        if len(self.children):
            return self.children[-1]
        else:
            return None

    @staticmethod
    def _isingle(item):
        u"iterator that yields only a single value then stops, for chaining"
        yield item


class Pipeline(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.pool = None

    def run(self, *args, **kwargs):
        """
        yeilds sub-pipelines or result, the *last* yield value is processed as result
        """
        assert False
        pass



    def _process_one_step(self, pipeline):
        u"Process a step of the pipeline, return sub-pipelines and/or wrapped values"
        
        state_changed = False
        results = []
        
        #replace args FutureResults with values
        #TODO do the same for kwargs
        args, kwargs = list(pipeline.args), dict(pipeline.kwargs)
        frs = filter(lambda x:isinstance(x, FutureResult), args)
        dependencies_status = map(lambda x:x.ismaterialized(), frs)
        for idx, x in enumerate(args):
            if isinstance(x, FutureResult):
                if x.ismaterialized():
                    args[idx] = x.value
        
        if all(dependencies_status):
            fr = None
            generator = pipeline.run(*args, **kwargs)
            try:
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
                
        return state_changed, results

    def process(self, pool=None):
        processing_tree = Tree(FutureResult(self))
        
        state_changed = True
        while state_changed:
            state_changed = False
            for node in processing_tree.iternodes():
                if isinstance(node.value, FutureResult):
                    if not node.populated:
                        state_changed, results  = self._process_one_step(node.value.pipeline)
    
                        for r in results:
                            child = node.append(r)
                            if r.pipeline == None:
                                child.populated = True

                        node.populated = True
                    elif not node.value.ismaterialized():
                        frs = filter(lambda x: isinstance(x.value, FutureResult),  node.children)
                        dependencies_status = map(lambda x: x.value.ismaterialized(), frs)
                        if all(dependencies_status):
                            node.value.set(node.last_child().value.value)
                            state_changed = True
                else:
                    print 'Unexpected'
        return processing_tree.value.value

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


class SleepPipeline(Pipeline):
    def run(self, sleep_time):
        print self, "started sleeping"
        time.sleep(sleep_time)
        print self, "done sleeping"
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
    test()
    s = "Hello World"

    g = gen_upper()
    dummy = g.send(None)

    for w in s.split():
        print g.send(w)

    f = gen_xxxx()
    try:
        print f.send(None)
        print f.send('a')
        f.send('b')
    except StopIteration:
        pass

    c = CompositePipeline(1, 2, 3)
    result = c.process()
    print result

    c = DoubleCompositePipeline(1, 2, 3)
    #c.pool = multiprocessing.Pool(2)
    result = c.process()
    print result

    width = 2
    depth = 1
    sleep_time = 2
    pool = multiprocessing.Pool()
    sw = StopWatch(True)
    result = ComplexPipeline(width, depth, SleepPipeline(sleep_time)).process(pool)
    sw.stop()
    print result

    return 0


def test():

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


    width = 2
    depth = 1
    sleep_time = 2
    pool = None
    sw = StopWatch(True)
    assert None == ComplexPipeline(width, depth, SleepPipeline(sleep_time)).process()
    sw.stop()
    assert abs(sw.duration() - width*sleep_time) < 0.1

    return 

    width = 2
    depth = 1
    sleep_time = 2
    pool = multiprocessing.Pool()
    sw = StopWatch(True)
    assert None == ComplexPipeline(width, depth, SleepPipeline(sleep_time)).process(pool)
    sw.stop()
    assert abs(sw.duration() - sleep_time) < 0.1

if __name__ == '__main__':
    sys.exit(main())
