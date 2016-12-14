#coding:utf-8

from scripts.utils.views import *
import threading
import time

# Time event define
TE_ROUNDSECOND = 0
TE_ROUNDMINUTE = 1
TE_ROUNDHOUR   = 2
TE_ROUNDDAY    = 3
TE_ROUNDMONTH  = 4
TE_COUNT       = 5

class TimeEventReciever():
    def __init__(self, name, teType = [], count = []):
        self.mName = name
        if len(teType) != len(count):
            raise ValueError("%s with Event and %s with count" %(len(teType), len(count)))
        self.mType = teType
        self.mMaxCount = {}
        self.mCurCount = {}
        i = 0
        for t in self.mType:
            self.mMaxCount[t] = count[i]
            self.mCurCount[t] = 0
            i += 1

    def getName(self):
        return self.mName

    def getType(self):
        return self.mType

    def calCount(self, teType, count):
        if teType in self.mMaxCount and teType in self.mCurCount:
            self.mCurCount[teType] += count
            if self.mCurCount[teType] >= self.mMaxCount[teType]:
                self.mCurCount[teType] -= self.mMaxCount[teType]
                return True
        return False

    def getCurCount(self, teType):
        if teType in self.mCurCount:
            return self.mCurCount[teType]
        else:
            return -1

    def getMaxCount(self, teType):
        if teType in self.mMaxCount:
            return self.mMaxCount[teType]
        else:
            return -1

    def notify(self, tm, count, teType):
        return True

class TimeEventDispatcher(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, name = "TimeEventDispatcher")
        self.mInterval = 0
        self.mCanRun = False
        self.mHasInit = False
        self.mRecverGroup = [{} for i in xrange(TE_COUNT)]
        self.mHasTerminated = True

    def new(self, num, tdDeamon):
        self.mInterval = 60
        self.mHasInit = True
        self.mCanRun = True
        self.setDaemon(tdDeamon)
        return self

    def registerReciever(self, recver, override = False):
        teType = recver.getType()
        name = recver.getName()
        for t in teType:
            if t >= TE_ROUNDSECOND and t < TE_COUNT:
                group = self.mRecverGroup[t]
                if name not in group:
                    group[name] = recver
                    cv.log("Register recver handler [" + name + "] with type [" + str(t) + "] successful!", True)
                elif override:
                    group[name] = recver
                    cv.log("Register recver handler [" + name + "] with type [" + str(t) + "] successful by replace!", True)
                else:
                    cv.err("Register recver handler [" + name + "] with type [" + str(t) + "] has repeated!", True)

    def run(self):
        if self.mHasInit:
            cv.log("Start TimeEventDispatcher...", True)
            cv.log("Correcting time to round minute and waiting...", True)
            tm = time.localtime(time.time())
            if tm.tm_sec != 0:
                time.sleep(60 - tm.tm_sec)
            self.mHasTerminated = False
            while self.mCanRun:
                begin = time.time()
                tm = time.localtime(begin)
                if tm.tm_min == 0 and tm.tm_hour == 0 and tm.tm_mday == 1:
                    self.handleRoundMonth(tm)
                elif tm.tm_min == 0 and tm.tm_hour == 0:
                    self.handleRoundDay(tm)
                elif tm.tm_min == 0:
                    self.handleRoundHour(tm)
                else:
                    self.handleRoundMinute(tm)
                tm = time.localtime(time.time())
                slpTime = 60 - tm.tm_sec
                cv.log("TimeEventDispatcher running and sleep [%.3f] seconds" % (slpTime), True)
                time.sleep(slpTime)
            cv.log("TimeEventDispatcher shutdown...", True)
            self.mHasTerminated = True
        else:
            cv.err("TimeEventDispatcher has failed to initialize, shutdown TimeEventDispatcher!", True)

    def stop(self):
        self.mCanRun = False

    def isTerminated(self):
        return self.mHasTerminated

    def handleRoundSecond(self, tm):
        group = self.mRecverGroup[TE_ROUNDSECOND]
        for name in group:
            handler = group[name]
            if handler.calCount(TE_ROUNDSECOND, self.mInterval):
                cv.log("Task handler [%s] on count [%d/%d] has been notified on round second!" % (name, handler.getCurCount(TE_ROUNDSECOND), handler.getMaxCount(TE_ROUNDSECOND)), True)
                handler.notify(tm, self.mInterval, TE_ROUNDSECOND)

    def handleRoundMinute(self, tm):
        self.handleRoundSecond(tm)
        group = self.mRecverGroup[TE_ROUNDMINUTE]
        for name in group:
            handler = group[name]
            if handler.calCount(TE_ROUNDMINUTE, self.mInterval / 60):
                cv.log("Task handler [%s] on count [%d/%d] has been notified on round minute!" % (name, handler.getCurCount(TE_ROUNDMINUTE), handler.getMaxCount(TE_ROUNDMINUTE)), True)
                handler.notify(tm, self.mInterval / 60, TE_ROUNDMINUTE)

    def handleRoundHour(self, tm):
        self.handleRoundMinute(tm)
        group = self.mRecverGroup[TE_ROUNDHOUR]
        for name in group:
            handler = group[name]
            if handler.calCount(TE_ROUNDHOUR, 1):
                cv.log("Task handler [%s] on count [%d/%d] has been notified on round hour!" % (name, handler.getCurCount(TE_ROUNDHOUR), handler.getMaxCount(TE_ROUNDHOUR)), True)
                handler.notify(tm, 1, TE_ROUNDHOUR)

    def handleRoundDay(self, tm):
        self.handleRoundHour(tm)
        group = self.mRecverGroup[TE_ROUNDDAY]
        for name in group:
            handler = group[name]
            if handler.calCount(TE_ROUNDDAY, 1):
                cv.log("Task handler [%s] on count [%d/%d] has been notified on round day!" % (name, handler.getCurCount(TE_ROUNDDAY), handler.getMaxCount(TE_ROUNDDAY)), True)
                handler.notify(tm, 1, TE_ROUNDDAY)

    def handleRoundMonth(self, tm):
        self.handleRoundDay(tm)
        group = self.mRecverGroup[TE_ROUNDMONTH]
        for name in group:
            handler = group[name]
            if handler.calCount(TE_ROUNDMONTH, 1):
                cv.log("Task handler [%s] on count [%d/%d] has been notified on round mouth!" % (name, handler.getCurCount(TE_ROUNDMONTH), handler.getMaxCount(TE_ROUNDMONTH)), True)
                handler.notify(tm, 1, TE_ROUNDMONTH)