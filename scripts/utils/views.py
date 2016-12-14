import time

class ConsoleViewer:
    def __init__(self):
        self.mLogHead   = "[LOG] "
        self.mWarnHead  = "(Warning) "
        self.mErrorHead = "<ERROR> "
        self.mSectionBegin = "=======BGN======="
        self.mSectionEnd   = "=======END======="

    def secBegin(self):
        print self.mSectionBegin

    def secEnd(self):
        print self.mSectionEnd

    def log(self, string, showTime = False):
        p = self.mLogHead + string
        if showTime:
            p = "[%s]%s" %(self.getCurrentTimeString(), p)
        print p
        return p

    def warn(self, string, showTime = False):
        p = self.mWarnHead + string
        if showTime:
            p = "[%s]%s" %(self.getCurrentTimeString(), p)
        print p
        return p

    def err(self, string, showTime = False):
        p = self.mErrorHead + string
        if showTime:
            p = "[%s]%s" %(self.getCurrentTimeString(), p)
        print p
        return p

    def printLines(self, lines):
        self.secBegin()
        if lines:
            total = 0
            for line in lines:
                print line
                total += 1
            self.log("Total row: %d" %(total))
        else:
            self.err("Null")
        self.secEnd()

    def getCurrentTimeString(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

cv = ConsoleViewer()