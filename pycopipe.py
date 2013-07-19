import sys
import time
import itertools
import inspect
import threading
import multiprocessing
from multiprocessing.managers import SyncManager
import Queue
#import cProfile
#import pycallgraph
import uuid
import pickle
import logging
import re
import functools 

 
def _replace_future_result_with_value(fr):
    if isinstance(fr, FutureResult):
        return fr.value if fr.ready() else None
    elif isinstance(fr, list):
        for idx,i in enumerate(fr):
            fr[idx] = _replace_future_result_with_value(i)
        return fr
    elif isinstance(fr, tuple):
        temp = _replace_future_result_with_value(list(fr))
        return tuple(temp)
    else:
        return fr

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
        args = _replace_future_result_with_value(args)
        #for idx, x in enumerate(args):
        #    if isinstance(x, FutureResult):
        #        if x.ready():
        #            args[idx] = x.value
        #        else:
        #            assert False
        #            return "not all dependencies ready yet"
        #    elif isinstance(x, list):
        #        for idx,y in enumerate(x):
        #            if isinstance(y, FutureResult):
        #                if y.ready():
        #                    x[idx] = y.value
        #                else:
        #                    assert False
        #                    return "not all dependencies ready yet"
        #    elif isinstance(x, tuple):
        #        z = list(x)
        #        for idx,y in enumerate(z):
        #            if isinstance(y, FutureResult):
        #                if y.ready():
        #                    z[idx] = y.value
        #                else:
        #                    assert False
        #                    return "not all dependencies ready yet"
        #        args[idx] = tuple(z)

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

    def isleaf():
        return 0 == len(self.children)

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
        self._successful = None

    def get(self, timeout=0):
        return self._result

    def wait(self, timeout):
        return

    def ready(self):
        return True

    def successful(self):
        assert self.ready()
        return self._successful
    
    def _set(self, result, successful):
        self._result = result
        self._successful = successful
            
    
class FakePool(object):
    def __init__(self):
        pass
    
    def apply_async(self, func, args):
        result = AsyncResult()
        result._set(func(*args), True)
        return result

    def close(self):
        pass

    def join(self):
        pass

    def terminate(self):
        pass


class Job(object):
    _id = 0

    @staticmethod
    def _next_job_id():
        Job._id += 1
        return Job._id
        #return uuid.uuid4()

    def __init__(self, function, args=(), kwargs={}, timeout=0, callback=None):
        self._uuid = Job._next_job_id()
        self._retry_count = 0
        self._start_time = 0
        self._timeout = 2
        self._callback = callback
        self.successful = None
        self.function = function
        self.args = args
        self.kwargs = kwargs

    @property
    def id(self):
        return self._uuid
    
    def process(self):
        self._start_time = time.time()
        result = self.function(*self.args, **self.kwargs)
        self.successful = True

        return result

    def __str__(self):
        return str(self._uuid) + ',' + str(self.function)


def _read_result_q(pool, result_q, job_id_event_map_lock, job_id_event_map, job_id_job_map):
    while True:
        try: 
            job_id, result = result_q.get()
            if job_id is None:
                logging.info('Sentinel recieved -- exiting _read_result_q')
                #fail all jobs that stil waiting for results
                with job_id_event_map_lock:
                    failed_job_ids = job_id_job_map.keys()
                    for failed_job_id in failed_job_ids:
                        pool.fail_job(failed_job_id)
                #break whole looop
                break
            else:
                #process result for job_id
                logging.info('Got result for %s job' % job_id)

                with job_id_event_map_lock:
                    async_result = job_id_event_map.pop(job_id, None)
                    job = job_id_job_map.pop(job_id, None)
                    if async_result is not None: 
                        async_result._set(result, True)
                    else:
                        msg = 'Jobid %{job_id}s does not exists or result was already submitted'
                        logging.warning(msg  % { 'job_id' : job.id})
        except Queue.Empty:
            with job_id_event_map_lock:

                #fail or resubmit timedout jobs
                failed_job_ids = []

                for k,v in job_id_job_map.iteritems():
                    if v._timeout > 0:
                        if (time.time() - v._start_time) > v._timeout:
                            if v._retry_count < 3:
                                pool.resubmit_job(v.id)
                            else:
                                failed_job_ids.append(v.id)

                for failed_job_id in failed_job_ids:
                    pool.fail_job(failed_job_id)

class QueueAsyncResult:
    def __init__(self, set_callback=None):
        self._result = None
        self._event = threading.Event()
        self._successful = None
        self._on_set_callback = set_callback
    
    def get(self, timeout=0):
        self.wait(timeout)
        return self._result
    
    def wait(self, timeout):
        self._event.wait(timeout)
        return
    
    def ready(self):
        return self._event.is_set()
    
    def successful(self):
        assert self.ready()
        return self._successful
    
    def _set(self, result, successful):
        assert self._event.is_set() == False
        self._result = result
        self._successful = successful
        self._event.set()
        
        if self._on_set_callback is not None:
            self._on_set_callback()

class QueueBasedPool(object):
    def __init__(self, job_q, result_q):
        self._job_q = job_q
        self._result_q = result_q

        self._command_q = Queue.Queue() # for resubmit thread

        self._job_id_event_map_lock = threading.RLock()
        self._job_id_event_map = {}
        self._job_id_job_map = {}

        args = self, self._result_q, self._job_id_event_map_lock, self._job_id_event_map, self._job_id_job_map
        self._read_result_q_worker_thread = None
        self._read_result_q_worker_thread = threading.Thread(target=_read_result_q, args=args)
        self._read_result_q_worker_thread.start()

    def apply_async(self, func, args=(), kwargs={}, timeout=0, callback=None):
        job = Job(func, args, kwargs, timeout=timeout)
        async_result = QueueAsyncResult(set_callback=callback)
        with self._job_id_event_map_lock:
            self._job_id_event_map[job.id] = async_result
            self._job_id_job_map[job.id] = job
            self._job_q.put(job)
        return async_result

    def resubmit_job(self, job_id):
        with self._job_id_event_map_lock:
            job = self._job_id_job_map[job_id]
            async_result = self._job_id_event_map[job.id]
            job._retry_count += 1
            self._job_q.put(job)
            return async_result

    def fail_job(self, job_id):
        with self._job_id_event_map_lock:
            async_result = self._job_id_event_map.pop(job_id, None)
            if async_result is not None: 
                async_result._set(None, False)

            job = self._job_id_job_map.get(job_id, None)
            if job is not None:
                if job.successful is None:
                    self._job_q.task_done()
                    job.successful = False
            else: 
                logging.warning('Could not delete failed job')



    def close(self):
        pill = None, None
        self._result_q.put(pill)

        pill = None
        self._command_q.put(pill)
        pass

    def join(self, timeout=None):
        if self._read_result_q_worker_thread is not None:
            self._read_result_q_worker_thread.join(timeout)

    def terminate(self):
        print "Terminating"

        #python cannot actually terminate threads
        #self._read_result_q_worker_thread.terminate()

        #at least try to close
        self.close()
        self.join(timeout=3)

def _set_state_changed_event(event):
    event.set()

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
        node_to_async_result_map = {}
        #TODO: add toposort

        state_changed_event = threading.Event()
        
        state_changed = True
        while not processing_tree.ready():
            state_changed = False

            for node in processing_tree.iternodes():
                if isinstance(node, FutureResult):
                    if node.state == FutureResultState.NEW:
                        #for debugging 
                        #if isinstance(node.pipeline, Sum):
                            #node.results = _process_one_step(node.pipeline)
                            #print node.pipeline.args[0].pipeline
                            #print node.pipeline.args
                            #print dir(node.pipeline.args[0])
                            #break
                        
                        args, kwargs = list(node.pipeline.args), dict(node.pipeline.kwargs)
                        frs = filter(lambda x:isinstance(x, FutureResult), args)
                        dependencies_status = map(lambda x:x.ready(), frs)
    
                        if all(dependencies_status):
                            callback = functools.partial(_set_state_changed_event, state_changed_event)
                            node_to_async_result_map[node] = pool.apply_async(_process_one_step, (node.pipeline,)) #, callback=callback)
                            node.state = FutureResultState.POPULATING
                            state_changed = True
                        
                    if node.state == FutureResultState.POPULATING:
                        async_result = node_to_async_result_map[node]
                        #async_result.wait(1)
                        if async_result.ready() and async_result.successful():
                            results = async_result.get()
                            del node_to_async_result_map[node]
                            
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
                        #keep the loop running while a job is thinking (could add timeout here)
                        #state_changed = True
                    
                    if node.state == FutureResultState.POPULATED:
                        if not node.ready():
                            dependencies_status = map(lambda x: x.ready(), node.children)
                            if len(dependencies_status) == 0 or (len(dependencies_status) > 0 and all(dependencies_status)):
                                node.set(node.last_child().value)
                                state_changed = True
                                node.pipeline.on_finished()
                else:
                    print 'Unexpected pipeline state'

            if not state_changed:
                state_changed_event.wait(5)
                state_changed = True
                state_changed_event.clear()

            items = sum(1 for x in processing_tree.iternodes())
            todo =  sum(1 for x in processing_tree.iternodes() if not x.ready())
            logging.info('Total items in processing tree %s, still to process %s' % (items, todo))


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
        super(ComplexReductionPipeline, self).__init__(width, depth, value_to_return, reduction_class)
    
    def run(self, width, depth, value_to_return, reduction_class):
        if depth > 0:
            subvalues = []
            for _ in xrange(width):
                subvalues += [(yield ComplexReductionPipeline(width, depth - 1, value_to_return, reduction_class))]
            
            yield reduction_class(*subvalues)
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

# This is based on the examples in the official docs of multiprocessing.
# get_{job|result}_q return synchronized proxies for the actual Queue
# objects.
class JobQueueManager(SyncManager):
    pass

class Returner(object):
    def __init__(self, value):
        self._value = value
    def __call__(self):
        return self._value
    

def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = multiprocessing.JoinableQueue()
    result_q = multiprocessing.Queue()
    shutdown_e = multiprocessing.Event()

    manager = JobQueueManager(address=('127.0.0.1', port), authkey=authkey)

    manager.register('get_job_q', callable=Returner(job_q))
    manager.register('get_result_q', callable=Returner(result_q))
    manager.register('get_shutdown_e', callable=Returner(shutdown_e))

    manager.start()
    logging.info('Server started at port %s' % port)
    return manager

class ServerQueueManager(SyncManager):
    pass
    
def make_client_manager(address, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
        """
    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')
    ServerQueueManager.register('get_shutdown_e')
    
    manager = ServerQueueManager(address, authkey=authkey)
    manager.connect()
    
    logging.info('Connecting to: %(address)s' % {'address' : manager.address} )

    return manager

def process_jobs(address, authkey):
    logger = multiprocessing.log_to_stderr()
    logger.setLevel(logging.INFO)
    manager = make_client_manager(address, authkey)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    shutdown_e = manager.get_shutdown_e()

    def clamp(minimum, x, maximum):
        return max(minimum, min(x, maximum))

    def calc_sleep_time(sleep_power):
        return clamp(0, 2**sleep_power, 120)

    sleep_power = 0

    while not shutdown_e.is_set():
        try:
            timeout = calc_sleep_time(sleep_power)
            logger.info("Waiting up to {timeout} seconds for a new job".format(timeout=timeout))
            job = job_q.get(True, timeout)
            logger.info("Got job: {job}".format(job=job))
            result = job.process()
            logger.info("Done processing job: {job}".format(job=job))
            comb = job.id, result
            result_q.put(comb)
            job_q.task_done()
            sleep_power = 0
        except Queue.Empty:
            #logger.info("Got empty queue -- sleeping for {0} seconds".format(sleeps[sleep_index]))
            sleep_power += 1
    
    logger.info('Worker done -- existing')

def run_client(address, authkey):
    processors = multiprocessing.cpu_count() - 1

    if True:
        process_jobs(address, authkey)
    else:
        pool = multiprocessing.Pool(processes = processors)

        for p in xrange(processors):
            pool.apply_async(process_jobs, args=(address, authkey)) 

        pool.close()
        pool.join()

def main():
    import optparse
    
    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose', dest='verbose', action='count')
    parser.add_option('--mode', help='Mode of the script: standalone/server/client/test', default='standalone')
    parser.add_option('--ip', help='IP of server', default='127.0.0.1')
    parser.add_option('--port', default=65001, type=int)
    parser.add_option('--auth', default='changeme')
    parser.add_option('--test_name', default='test_basic')

    options_obj, args = parser.parse_args()
    
    log_level = logging.WARNING # default
    if options_obj.verbose == 1:
        log_level = logging.INFO
    elif options_obj.verbose >= 2:
        log_level = logging.DEBUG

    # Set up basic configuration, out to stderr with a reasonable default format.
    logging.basicConfig(level=log_level)
 
    options = vars(options_obj)
    print 'Options:', options
    
    option_mode = options['mode']

    if 'standalone' == option_mode:
        test_basic()
    elif 'server' == option_mode:
        manager = make_server_manager(options['port'], options['auth'])
        shared_job_q = manager.get_job_q()
        shared_result_q = manager.get_result_q()
        shared_shutdown_e = manager.get_shutdown_e()
        shared_job_q.put(Job(square_me, (1,)))
        shared_job_q.put(Job(square_me, (2,)))
        
        outdict = shared_result_q.get()
        print outdict
        outdict = shared_result_q.get()
        print outdict

        shared_shutdown_e.set()
        shared_job_q.join()
        manager.shutdown()
    elif 'client' == option_mode:
        run_client(address=(options['ip'], options['port']), authkey=options['auth'])
    elif 'test' == option_mode:
        test_function_name = options['test_name']

        module = sys.modules[__name__]
        regex = re.compile(test_function_name)

        for attrib in dir(module):
            if re.match(regex, attrib):
                function = getattr(module, attrib)
                if callable(function):
                    print 'Calling', attrib
                    function()

        return 0
    elif 'test_pool_server' == option_mode:
        #test_fanout()
        test_pool_results(False)
    elif 'test_pool_client' == option_mode:
        port = options['port']
        authkey = options['auth']
        run_client(address =('localhost', port), authkey=authkey)
    else:
        print 'Uknown mode specified'
        return -1

    #pycallgraph.start_trace()

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


def test_basic():
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

def test_sleep_pipeline():
    width = 1
    depth = 0
    sleep_time = 4
    pool = None
    sw = StopWatch(True)
    assert None == ComplexPipeline(width, depth, SleepPipeline(sleep_time)).process()
    sw.stop()
    print 'Duration', sw.duration()
    assert (sw.duration() - 0.1) < width*sleep_time

def test_pool_time():
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
    scale = 1.4
    assert durations[0] > (durations[1] * scale)

def test_pool_results(start_client=True):
    logger = multiprocessing.log_to_stderr()
    #logger.setLevel(logging.DEBUG)
    logger.info('Staring')    

    port = 65001
    authkey='hi'
    manager = make_server_manager(port=port, authkey=authkey)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()

    qb_pool = QueueBasedPool(job_q, result_q)
    
    pools = [qb_pool, FakePool()]#, multiprocessing.Pool(),multiprocessing.pool.ThreadPool(), multiprocessing.Pool()]
    durations = []
    results = []

    client_p = None
    
    if True: 
        client_p = multiprocessing.Process(target=run_client, args=(), kwargs={'address':('localhost', port), 'authkey' : authkey})
        client_p.start()

    for pool in pools:
        width = 4
        depth = 3
        sleep_time = 4
        sw = StopWatch(True)
        result = ComplexReductionPipeline(width, depth, 1, Sum).process(pool)
        sw.stop()
        
        durations.append(sw.duration())
        results.append(result)

        pool.close();
        pool.join()
    
    logging.info("Shutting down manager")

    shutdown_e = manager.get_shutdown_e()
    shutdown_e.set()
    logging.info("Sleeping to allow clients to shutdown down")
    time.sleep(4)
    job_q.close()
    job_q.join()
    manager.shutdown()

    print durations
    print results

    assert all(x == results[0] for x in results)
    pass

def test_job_resubmit(start_client=True):
    logger = multiprocessing.log_to_stderr()
    logger.setLevel(logging.DEBUG)
    logger.info('Staring')    

    port = 65001
    authkey='hi'
    manager = make_server_manager(port=port, authkey=authkey)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    shutdown_e = manager.get_shutdown_e()

    #job_q = Queue.Queue()
    #result_q = Queue.Queue()

    qb_pool = QueueBasedPool(job_q, result_q)
    qb_pool.apply_async(square_me, (2,), {}, 1)

    qb_pool.close()
    qb_pool.join()

    time.sleep(3)
    
    with qb_pool._job_id_event_map_lock:
        assert 1 == len(qb_pool._job_id_job_map.keys())
        assert False == qb_pool._job_id_job_map.values()[0].successful
        #assert 0 == qb_pool._job_q.unfinished_tasks

    shutdown_e.set()
    job_q.join()
    manager.shutdown()

def test_fanout():
    pool = FakePool()
    pool = multiprocessing.Pool(1)
    #pool = multiprocessing.pool.ThreadPool()
    width = 4
    depth = 3
    result = ComplexReductionPipeline(width, depth, 1, Sum).process(pool)
    #result = ComplexPipeline(width, depth, SleepPipeline(1)).process(pool)
    print result
    pass

if __name__ == '__main__':
    multiprocessing.freeze_support()
    sys.exit(main())
