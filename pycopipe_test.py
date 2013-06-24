import sys
import time
import multiprocessing

class FutureResult(object):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.materialized = False

    def materialize(self):
        self.value = self.pipeline.process()
        self.materialized = True
        return self.value

    def ismaterialized(self):
        return self.materialized

    def count_dependencies(self):
        if self.materialized:
            return 0

        dependencies = list(self.pipeline.args) + self.pipeline.kwargs.values()
        frs = filter(lambda x: isinstance(x, FutureResult), dependencies)
        frs = filter(lambda x: False == x.ismaterialized(), frs)

        #recurse
        sub_dependencies_count = map(FutureResult.count_dependencies, frs)
        return len(frs) + sum(sub_dependencies_count)

def future_result_materialize_helper(x):
    return x.materialize()


class Pipeline(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.pool = None

    def run(self, *args, **kwargs):
        """
        yeilds sub-pipelines or result, the *last* yield value is processed as result
        """
        pass

    def process(self):
        """
        calls run() and executes yielded pipelines (as needed)
        :return:
        """
        args = list(self.args)
        kwargs = dict(self.kwargs)

        # Future result must have been materialized by now, replace them with actual values
        args = [v.value if isinstance(v, FutureResult) else v for v in args]
        # todo -- add the same for kwargs

        g = self.run(*args, **kwargs)

        fr = None
        results = [None] if g is None else []
        try:
            while g is not None:
                result = g.send(fr)
                fr = FutureResult(result) if isinstance(result, Pipeline) else None
                results += [result] if fr is None else [fr]  
        except StopIteration:
            pass

        frs = filter(lambda x: isinstance(x, FutureResult),  results)
        
        if self.pool is None:
            frs = map(future_result_materialize_helper, frs)
        else:
            while any(not x.ismaterialized() for x in frs):
                parallel = filter(lambda x: 0 == x.count_dependencies(), frs)
                parallel_results  = self.pool.map(future_result_materialize_helper, parallel)

                for idx, p in enumerate(parallel):
                    p.value = parallel_results[idx]
                    p.materialized = True

        result = results[-1]
        if isinstance(result, FutureResult):
            return result.value
        else:
            return result


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

class RecursivePipeline(Pipeline):
    def run(self, levels, value_to_return):
        if levels > 0:
            dummy = yield RecursivePipeline(levels - 1, value_to_return)
        else:
            dummy = yield value_to_return
        pass


class ComplexPipeline(Pipeline):
    def __init__(self, width, depth, value_to_return):
        return super(ComplexPipeline, self).__init__(width, depth, value_to_return)
    
    
    def run(self, width, depth, value_to_return):
        if depth > 0:
            for i in xrange(width):
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
        for i in xrange(task_number):
            dummy = yield SleepPipeline(processing_time)
            pass
        pass


def gen_upper():
    value = yield
    while True:
        value = yield value.upper()


def gen_xxxx():
    value = yield 1
    value = yield 2
    pass



def main():
    assert 127 == ComplexPipeline(2, 2, 127).process()


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

    c = LongProcessingPipeline(2, 3)
    #c.pool = multiprocessing.Pool(2)
    result = c.process()
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


    assert 127 == RecursivePipeline(100, 127).process()

    assert 127 == ComplexPipeline(2, 2, 127).process()

if __name__ == '__main__':
    sys.exit(main())
