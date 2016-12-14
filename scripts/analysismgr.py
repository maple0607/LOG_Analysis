#coding:utf-8

from scripts.utils.tabfile import TabFile
from scripts.utils.views import *
from scripts.timeeventdispatcher import *
from scripts import handler
from scripts.dataslmgr import DataSLMgr
import os
import json

class AnalysisMgr():
    def __init__(self):
        self.mDBHandlers = {}
        self.mType2DBHandlers = {}
        self.mNeedReloadT2DBH = False
        self.mEXEHandlers = {}
        self.mNeedReloadT2EXEH = False
        self.mType2EXEHandlers = {}
        self.mTaskHandlers = {}
        self.mTEDispatcher = None
        self.mSelfPath = ""
        self.mDataMgr = None
        self.mTDDeamon = False

    def startup(self, tdDeamon = False):
        cv.log("Startup Analysis Manager...", True)
        cfgHandler = open("configs/conf.json")
        cfgdata = cfgHandler.read()
        cfgHandler.close()
        jsonData = json.loads(cfgdata)
        self.mSelfPath = jsonData["SystemPath"]

        self.mDataMgr = DataSLMgr().new(
            name = jsonData["DB"]["HandlerName"],
            serverId = jsonData["DB"]["ServerID"],
            dbType = jsonData["DB"]["DBType"],
            host = jsonData["DB"]["Hostname"],
            port = jsonData["DB"]["Hostport"],
            user = jsonData["DB"]["Username"],
            psswd = jsonData["DB"]["Password"],
            dbname = jsonData["DB"]["Database"]
            )

        self.mTDDeamon = tdDeamon
        self.initEventDispatcher(tdDeamon)
        self.loadSystemDBHandler()
        self.loadCustomDBHandler()
        self.loadSystemEXEHandler()
        self.loadCustomEXEHandler()
        self.loadSystemTaskHandler()
        self.loadCustomTaskHandler()
        if not os.path.isdir(self.mSelfPath + 'output'):
            os.mkdir(self.mSelfPath + 'output')
        self.addTaskToDispatcher()
        self.mTEDispatcher.start()
        return self

    def shutdown(self):
        if self.mTEDispatcher:
            self.mTEDispatcher.stop()
            while not self.mTEDispatcher.isTerminated() and not self.mTDDeamon:
                cv.log("+", True)
                time.sleep(1)
        cv.log("Shutdown Analysis Manager...", True)

    def getDBHandler(self, name):
        if name in self.mDBHandlers:
            return self.mDBHandlers[name]
        return None

    def getDBHandlersWithType(self, hType):
        if self.mNeedReloadT2DBH:
            newT2H = {}
            for name in self.mDBHandlers:
                handler = self.mDBHandlers[name]
                ht = handler.getType()
                if ht in newT2H:
                    newT2H[ht].append(handler)
                else:
                    newT2H[ht] = [handler,]
            self.mType2DBHandlers = newT2H
            self.mNeedReloadT2DBH = False
        hType = hType.lower()
        if hType in self.mType2DBHandlers:
            return self.mType2DBHandlers[hType]
        return []

    def getEXEHandler(self, name):
        if name in self.mEXEHandlers:
            return self.mEXEHandlers[name]
        return None

    def getEXEHandlersWithType(self, hType):
        if self.mNeedReloadT2EXEH:
            newT2H = {}
            for name in self.mEXEHandlers:
                handler = self.mEXEHandlers[name]
                ht = handler.getType()
                if ht in newT2H:
                    newT2H[ht].append(handler)
                else:
                    newT2H[ht] = [handler,]
            self.mType2EXEHandlers = newT2H
            self.mNeedReloadT2EXEH = False
        hType = hType.lower()
        if hType in self.mType2EXEHandlers:
            return self.mType2EXEHandlers[hType]
        return []

    def getTaskHandler(self, name):
        if name in self.mTaskHandlers:
            return self.mTaskHandlers[name]
        return None

    def initEventDispatcher(self, tdDeamon):
        self.mTEDispatcher = TimeEventDispatcher().new(1, tdDeamon)

    def loadSystemDBHandler(self):
        cv.log("Loading system db Handler...", True)
        import scripts.handler.dbhandler
        cv.log("Done!", True)

    def loadCustomDBHandler(self):
        cv.log("Loading custom db Handler...", True)
        dataFile = self.mSelfPath + "dbhandlers.txt"
        try:
            hdCfg = open(dataFile, "r")
            lines = hdCfg.readlines()
            hdCfg.close()
            for idx in xrange(len(lines)):
                line = lines[idx]
                params = line.replace("\r", '').replace("\n", '').split("\t")
                if len(params) >= 9:
                    if params[0] not in self.mDBHandlers:
                        hdr = handler.dbhandler.DBHandler(
                            params[0],
                            params[1],
                            params[2],
                            params[3],
                            params[4],
                            params[5],
                            int(params[6]),
                            params[7],
                            params[8])
                        self.registerDBHandler(params[0], hdr)
                    else:
                        cv.warn("A duplicate key like %s in line %d" %(params[0], idx), True)
                else:
                    cv.warn("There has some error in line %d" %(idx), True)
            return True
        except:
            cv.err("Cannot load custom db handler configs!", True)
            return False
        cv.log("Done!", True)

    def loadSystemEXEHandler(self):
        cv.log("Loading system exe Handler...", True)
        import scripts.handler.exehandler
        cv.log("Done!", True)

    def loadCustomEXEHandler(self):
        cv.log("Loading custom exe Handler...", True)
        dataFile = self.mSelfPath + "exehandlers.txt"
        try:
            hdCfg = open(dataFile, "r")
            lines = hdCfg.readlines()
            hdCfg.close()
            for idx in xrange(len(lines)):
                line = lines[idx]
                params = line.replace("\r", '').replace("\n", '').split("\t")
                if len(params) >= 3:
                    if params[0] not in self.mEXEHandlers:
                        self.registerEXEHandler(params[0], handler.exehandler.ExecutionHandlerBase(params[0], params[1], params[2]))
                    else:
                        cv.warn("A duplicate key like %s in line %d" %(params[0], idx), True)
                else:
                    cv.warn("There has some error in line %d" %(idx), True)
            return True
        except:
            cv.err("Cannot load custom exe handler configs!", True)
            return False
        cv.log("Done!", True)

    def loadSystemTaskHandler(self):
        cv.log("Loading system task Handler...", True)
        import scripts.handler.tasks
        cv.log("Done!", True)

    def loadCustomTaskHandler(self):
        cv.log("Loading custom task Handler...", True)
        cv.log("Done!", True)

    def registerDBHandler(self, name, handler, override = False):
        if name not in self.mDBHandlers:
            self.mDBHandlers[name] = handler
            self.mNeedReloadT2DBH = True
            cv.log("Register db handler [" + name + "] successful!", True)
        elif override:
            self.mDBHandlers[name] = handler
            self.mNeedReloadT2DBH = True
            cv.log("Register db handler [" + name + "] successful by replace!", True)
        else:
            cv.log("Register db handler [" + name + "] faild!", True)

    def registerEXEHandler(self, name, handler, override = False):
        if name not in self.mEXEHandlers:
            self.mEXEHandlers[name] = handler
            self.mNeedReloadT2EXEH = True
            cv.log("Register exe handler [" + name + "] successful!", True)
        elif override:
            self.mEXEHandlers[name] = handler
            self.mNeedReloadT2EXEH = True
            cv.log("Register exe handler [" + name + "] successful by replace!", True)
        else:
            cv.log("Register exe handler [" + name + "] faild!", True)

    def registerTaskHandler(self, name, handler, override = False):
        if name not in self.mTaskHandlers:
            self.mTaskHandlers[name] = handler
            cv.log("Register task handler [" + name + "] successful!", True)
        elif override:
            self.mTaskHandlers[name] = handler
            cv.log("Register task handler [" + name + "] successful by replace!", True)
        else:
            cv.log("Register task handler [" + name + "] faild!", True)

    def addTaskToDispatcher(self):
        cv.log("Add task to TimeEventDispatcher...", True)
        for taskName in self.mTaskHandlers:
            self.mTEDispatcher.registerReciever(self.mTaskHandlers[taskName])
        cv.log("Done!", True)

    def convResultToLines(self, title, result, lines = []):
        lines.append("#^%s^#\t%s" %(title, cv.getCurrentTimeString()))
        for rowData in result:
            title = ""
            values = ""
            for key in rowData:
                if len(lines) < 1:
                    try:
                        title += key.encode("UTF-8")
                    except:
                        title += str(key).encode("UTF-8")
                    title += '\t'
                try:
                    values += rowData[key].encode("UTF-8")
                except:
                    values += str(rowData[key]).encode("UTF-8")
                values += '\t'
            if len(lines) < 2:
                lines.append(title)
            lines.append(values)
        return lines

    def saveQueryResult(self, filename, lines):
        newLines = []
        for line in lines:
            newLines.append(line + '\n')
        newFileName = "output/%s.txt" % (filename)
        newFile = open(newFileName, "a")
        newFile.writelines(newLines)
        newFile.close()

    def getZeroTimeStamp(self, tm = None):
        if tm == None:
            tm = time.localtime(time.time())
        timeStamp = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', tm),'%Y-%m-%d %H:%M:%S'))
        return int(timeStamp)

    def saveToDB(self, dtType, recordTS, serverId, params):
        self.mDataMgr.updateData(dtType, recordTS, serverId, params)

AnaMgr = AnalysisMgr()