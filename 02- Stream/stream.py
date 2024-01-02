import threading
import queue

class Stream:
    def __init__(self):
        self.shouldRun = True
        self.applyFunction = None
        self.forEachFunction = None
        self.newStream = None
        self.data_queue = queue.Queue()
        self.thread = threading.Thread(target=self.wait)
        self.thread.start()

    def wait(self):
        while self.shouldRun or not self.data_queue.empty():
            try:
                x = self.data_queue.get(timeout=0.1)  # Timeout to check shouldRun periodically
                self.process_item(x)
            except queue.Empty:
                continue

    def process_item(self, x):
        if self.applyFunction is not None:
            self.handleApply(x)
        elif self.forEachFunction is not None:
            self.forEachFunction(x)

    def handleApply(self, item):
        applyFunctionResult = self.applyFunction(item)
        if applyFunctionResult and type(applyFunctionResult) is bool:
            self.newStream.add(item)
        elif type(applyFunctionResult) is not bool:
            self.newStream.add(applyFunctionResult)

    def apply(self, applyFunction):
        self.applyFunction = applyFunction
        self.newStream = Stream()
        return self.newStream

    def forEach(self, forEachFunction):
        self.forEachFunction = forEachFunction

    def add(self, x):
        self.data_queue.put(x)

    def stop(self):
        self.shouldRun = False
        self.thread.join()
        if self.newStream:
            self.newStream.stop()
