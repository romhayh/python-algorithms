import threading
import time

class Stream:
    def wait(self):
        while self.shouldRun:
            if self.list:
                for x in self.list:
                    # Apply function
                    if self.applyFunction is not None:
                        self.handleApply(x)
                    
                    # Foreach function
                    elif self.forEachFunction is not None:
                        self.forEachFunction(x)
                self.list = []
            else: 
                time.sleep(0.2)



    def __init__(self):
        self.shouldRun = True
        self.applyFunction = None
        self.forEachFunction = None
        self.newStream = None
        self.list = []
        self.thread = threading.Thread(target=self.wait)
        self.thread.start()


    def handleApply(self, item):
        applyFunctionResult = self.applyFunction(item)

        # Filter apply function
        if applyFunctionResult and type(applyFunctionResult) is bool:
            self.newStream.add(item)
        # Map apply function
        elif type(applyFunctionResult) is not bool:
            self.newStream.add(applyFunctionResult)

    def apply(self, applyFunction):
        self.applyFunction = applyFunction
        self.newStream = Stream()

        if self.list:
            for item in self.list:
                self.handleApply(item)

            self.list = []

        return self.newStream

    def forEach(self, forEachFunction):
        self.forEachFunction = forEachFunction

        if self.list:
            for item in self.list:
                self.forEachFunction(item)

            self.list = []

    def add(self, x):
        self.list.append(x)


    def stop(self):
        self.shouldRun = False
        if self.newStream:
            self.newStream.stop()
        self.thread.join()