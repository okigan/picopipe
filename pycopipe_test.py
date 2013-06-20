import sys
import time

class FutureResult(object):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.materialized = False

    def materialize(self):
        self.value = self.pipeline.process()
        self.materialized = True
        return self.value


class Pipeline(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def run(self, *args, **kwargs):
        """
        yeilds sub-pipelines or result, the *last* yield value is processed as result
        """
        pass

    def process(self):
        """
        returns result
        :return:
        """
        args = list(self.args)
        kwargs = self.kwargs

        # Future result must be realized by now, replace them with actual values
        args = [v.value if isinstance(v, FutureResult) else v for v in args]

        g = self.run(*args, **kwargs)
        fr = None

        results = []
        if g is None:
            results += [None]
        else:
            try:
                while True:
                    result = g.send(fr)
                    fr = FutureResult(result) if isinstance(result, Pipeline) else None
                    results += [result] if fr is None else [fr]  
            except StopIteration:
                pass

        frs = filter(lambda x: isinstance(x, FutureResult),  results)
        map(FutureResult.materialize, frs)

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
    dummy = c.process()
    print dummy

    c = LongProcessingPipeline(2, 10)
    dummy = c.process()
    print dummy

    return 0


if __name__ == '__main__':
    sys.exit(main())


def test():
    assert 6 == Sum(1, 2, 3).process()
    assert 6 == Multiply(1, 2, 3).process()
    assert 3 == Max(1, 2, 3).process()


