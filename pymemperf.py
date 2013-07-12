#!/usr/bin/python
#

import random
import string
import os, sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/libs")

from threading import Thread
import time
import memcache
from exception import MemcachedTimeoutException
from optparse import OptionParser
from util import ProgressBar, StringUtil

class SharedProgressBar(object):
    def __init__(self, number_of_items):
        self.bar = ProgressBar(0, number_of_items, 77)
        self.number_of_items = number_of_items
        self.counter = 0
        self.old_bar_string = ""

    def update(self):
        self.counter += 1
        if self.old_bar_string != str(self.bar):
            sys.stdout.write(str(self.bar) + '\r')
            sys.stdout.flush()
            self.old_bar_string = str(self.bar)
        self.bar.updateAmount(self.counter)

    def flush(self):
        self.bar.updateAmount(self.number_of_items)
        sys.stdout.write(str(self.bar) + '\r')
        sys.stdout.flush()

class SmartLoader(object):
    def __init__(self, options, server, sharedProgressBar, thread_id):
        self._options = options
        self._server = server
        self._thread = None
        self.shut_down = False
        self._stats = {"total_time": 0, "max": 0, "min": 1 * 1000 * 1000, "samples": 0, "timeouts": 0}
        self._bar = sharedProgressBar
        self._thread_id = thread_id

    def start(self):
        self._thread = Thread(target=self._run)
        self._thread.start()

    def _run(self):
        v = None
        try:
            options = self._options
            v = memcache.Client(["{0}".format(self._server)])
            number_of_items = (int(options.items) / int(options.num_of_threads))
            for i in range(1, number_of_items):
                if self.shut_down:
                    break
                key = "{0}".format(i)
                if options.load_json:
                    document = document + ",\"xyz\":\"{0}\"".format(''.join((random.choice(string.letters) for _ in xrange(random.randint(5, 10)))))
                    document = "{" "\"data\":{"+ document + "}}"
                    try:
                        self._profile_before()
                        v.set(key, document)
                        self._profile_after()
                        print v.get(key)
                    except MemcachedTimeoutException:
                        self._stats["timeouts"] += 1
                self._bar.update()
            v.disconnect_all()
            v = None
        except:
            if v:
                v.disconnect_all()

    def print_stats(self):
        msg = "Thread {0} - average set time : {1} seconds , min : {2} seconds , max : {3} seconds , operation timeouts {4}"
        if self._stats["samples"]:
            print msg.format(self._thread_id, self._stats["total_time"] / self._stats["samples"],
                             self._stats["min"], self._stats["max"], self._stats["timeouts"])

    def wait(self, block=False):
        if block:
            self._thread.join()
        else:
            return not self._thread.is_alive()


    def stop(self):
        self.shut_down = True
        if v:
            v.done()

    def _profile_before(self):
        self.start = time.time()

    def _profile_after(self):
        self.end = time.time()
        diff = self.end - self.start
        self._stats["samples"] += 1
        self._stats["total_time"] = self._stats["total_time"] + diff
        if self._stats["min"] > diff:
            self._stats["min"] = diff
        if self._stats["max"] < diff:
            self._stats["max"] = diff

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--node", dest="node",
                      help="node ns_server ip:port", metavar="10.135.1.1:11211")
    parser.add_option("-j", "--json", dest="load_json", help="insert json data",
                      default=False, metavar="True")

    parser.add_option("-v", "--verbose", dest="verbose", help="run in verbose mode",
                      default=False, metavar="False")

    parser.add_option("-i", "--items", dest="items", help="number of items to be inserted",
                      default=100, metavar="100")

    parser.add_option("--size", dest="value_size", help="value size,default is 1kb",
                      default=1024, metavar="100")

    parser.add_option("--threads", dest="num_of_threads", help="number of threads to run load",
                      default=1, metavar="100")

    options, args = parser.parse_args()

    node = options.node
    
    if not node:
        parser.print_help()
        exit()
    if node.find(":") == -1:
        hostname = node
        port = 11211
    else:
        hostname = node[:node.find(":")]
        port = node[node.find(":") + 1:]
    server = "{0}:{1}".format(hostname, port)
    v = None
    workers = []
    try:
        no_threads = options.num_of_threads
        number_of_items = int(options.items)
        sharedProgressBar = SharedProgressBar(number_of_items)
        for i in range(0, int(no_threads)):
            worker = SmartLoader(options, server, sharedProgressBar, i)
            worker.start()
            workers.append(worker)
        while True:
            all_finished = True
            for worker in workers:
                all_finished &= worker.wait()
            if all_finished:
                break
            else:
                time.sleep(0.5)
        sharedProgressBar.flush()
        for worker in workers:
            worker.print_stats()
    except :
        print ""
        for worker in workers:
            worker.stop()
