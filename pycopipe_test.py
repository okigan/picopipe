from pycopipe import *


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
    width = 1
    depth = 1
    result = ComplexReductionPipeline(width, depth, 1, Sum).process()
    pass

if __name__ == '__main__':
    test3()
    test_fanout()

