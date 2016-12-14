#coding:utf-8

from scripts.timeeventdispatcher import *
from scripts.analysismgr import *
from scripts.utils.views import *

TEType_Intervals = {
    TE_ROUNDSECOND : 1,
    TE_ROUNDMINUTE : 60,
    TE_ROUNDHOUR   : 3600,
    TE_ROUNDDAY    : 86400,
    TE_ROUNDMONTH  : 2592000
}

def getIntervalTime(count, teType):
    if teType in TEType_Intervals:
        return count * TEType_Intervals[teType]
    return 0

class TaskBase(TimeEventReciever):
    def __init__(self, name, teType = [], count = [], dbHandlers = [], dbType = "", exeHandlers = [], exeType = ""):
        TimeEventReciever.__init__(self, name, teType, count)
        self.mTargetDBHandlers = dbHandlers
        self.mTargetDBType = dbType.lower()
        self.mTargetEXEHandlers = exeHandlers
        self.mTargetEXEType = exeType.lower()

    def getValidDBHandlers(self):
        validHD = []
        validHD += self.mTargetDBHandlers
        validHD += AnaMgr.getDBHandlersWithType(self.mTargetDBType)
        return validHD

    def getValidEXEHandlers(self):
        validHD = []
        validHD += self.mTargetEXEHandlers
        validHD += AnaMgr.getEXEHandlersWithType(self.mTargetEXEType)
        return validHD

    def setDBType(self, dbType):
        self.mTargetDBType = dbType.lower()

    def setExeType(self, exeType):
        self.mTargetEXEType = exeType.lower()

    def notify(self, tm, count, teType):
        pass

class TaskRecharge(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "Recharge", [TE_ROUNDMINUTE, TE_ROUNDDAY], [10, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("Recharge")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    params = {}
                    queryResult = {}

                    params["ExeType"] = 1
                    exeStr = exeHandler.makeExecutionString(tableName, params)
                    result = hd.executeSql(exeStr)
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["Sum"] = 0
                        else:
                            queryResult["Sum"] = result[0][0]

                    params["ExeType"] = 2
                    exeStr = exeHandler.makeExecutionString(tableName, params)
                    result = hd.executeSql(exeStr)
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["AccountCount"] = 0
                        else:
                            queryResult["AccountCount"] = result[0][0]

                    params["ExeType"] = 3
                    exeStr = exeHandler.makeExecutionString(tableName, params)
                    result = hd.executeSql(exeStr)
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["Count"] = 0
                        else:
                            queryResult["Count"] = result[0][0]

                    AnaMgr.saveToDB(1, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("Recharge", TaskRecharge())

class TaskConsumeGold(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "ConsumeGold", [TE_ROUNDHOUR, TE_ROUNDDAY], [1, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("ConsumeGold")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["Count"] = 0
                        else:
                            queryResult["Count"] = result[0][0]
                    AnaMgr.saveToDB(2, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("ConsumeGold", TaskConsumeGold())

class TaskDailyActive(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "DailyActive", [TE_ROUNDMINUTE, TE_ROUNDDAY], [10, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("DailyActive")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["ActiveCount"] = 0
                        else:
                            queryResult["ActiveCount"] = result[0][0]
                    AnaMgr.saveToDB(3, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("DailyActive", TaskDailyActive())

class TaskConsumeGoldByAct(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "ConsumeGoldByAct", [TE_ROUNDHOUR, TE_ROUNDDAY], [1, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("ConsumeGoldByAct")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) > 0:
                        dataStr = ""
                        for pair in result:
                            try:
                                dataStr += "%s:%s;" %(pair[0], pair[1])
                            except:
                                dataStr += "Unknown:Unknown;"
                                cv.warn("Data error in task ConsumeGoldByAct", True)
                        queryResult["DataByActID"] = dataStr
                    AnaMgr.saveToDB(4, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("ConsumeGoldByAct", TaskConsumeGoldByAct())

class TaskDailyCreate(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "DailyCreate", [TE_ROUNDMINUTE, TE_ROUNDDAY], [10, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("DailyCreate")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["Count"] = 0
                        else:
                            queryResult["Count"] = result[0][0]
                    AnaMgr.saveToDB(5, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("DailyCreate", TaskDailyCreate())

class TaskVipLevel(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "VipLevel", [TE_ROUNDHOUR, TE_ROUNDDAY], [1, 1])
        self.setDBType("data")
        self.setExeType("data")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("VipLevel")
            if exeHandler != None:
                tableName = "Player"
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    dataList = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                    #TODO
                    for dataItem in result:
                        if len(dataItem) == 2:
                            dataList[dataItem[0]] = dataItem[1]
                    dataStr = ""
                    for value in dataList:
                        dataStr += str(value) + ";"
                    queryResult["Data"] = dataStr
                    AnaMgr.saveToDB(6, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("VipLevel", TaskVipLevel())

class TaskSubsistence(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "Subsistence", [TE_ROUNDHOUR, TE_ROUNDDAY], [12, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("Subsistence")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    showTableStr = "show tables"
                    result = hd.executeSql(showTableStr)
                    tables = []
                    for i in xrange(1, len(result)):
                        tables.append(result[i][0])
                    if tableName not in tables:
                        continue
                    tStamps = self.getRelativeTimeStamps(AnaMgr.getZeroTimeStamp(tm))
                    for tsType in tStamps:
                        ts = tStamps[tsType]
                        tmpTM = time.localtime(ts)
                        relativeTable = "Logs_%.4d_%.2d_%.2d" % (tmpTM.tm_year, tmpTM.tm_mon, tmpTM.tm_mday)
                        if relativeTable in tables:
                            params = {}
                            params["Relative"] = relativeTable
                            exeStr = exeHandler.makeExecutionString(tableName, params)
                            result = hd.executeSql(exeStr)
                            queryResult = {}
                            try:
                                queryResult["Count"] = result[0][0]
                            except:
                                queryResult["Count"] = 0
                            queryResult["Type"] = tsType
                            AnaMgr.saveToDB(7, ts, hd.getServerID(), queryResult)

    def getRelativeTimeStamps(self, ts):
        tss = {
        2:ts - 86400,
        3:ts - 86400 * 2,
        7:ts - 86400 * 6,
        15:ts - 86400 * 14,
        30:ts - 86400 * 29,
        }
        return tss

AnaMgr.registerTaskHandler("Subsistence", TaskSubsistence())


class TaskItemSoldInMall(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "ItemSoldInMall", [TE_ROUNDHOUR, TE_ROUNDDAY], [1, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("ItemSoldInMall")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) > 0:
                        dataStr = ""
                        for pair in result:
                            try:
                                dataStr += "%s:%s;" %(pair[0], pair[1])
                            except:
                                dataStr += "Unknown:Unknown;"
                                cv.warn("Data error in task ItemSoldInMall", True)
                        queryResult["DataByItemID"] = dataStr
                    AnaMgr.saveToDB(8, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("ItemSoldInMall", TaskItemSoldInMall())

class TaskLevelDistribution(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "LevelDistribution", [TE_ROUNDHOUR, TE_ROUNDDAY], [4, 1])
        self.setDBType("data")
        self.setExeType("data")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("LevelDistribution")
            if exeHandler != None:
                tableName = "Player"
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) > 0:
                        dataStr = ""
                        for pair in result:
                            try:
                                dataStr += "%s:%s;" %(pair[0], pair[1])
                            except:
                                dataStr += "Unknown:Unknown;"
                                cv.warn("Data error in task LevelDistribution", True)
                        queryResult["DataByLevel"] = dataStr
                    AnaMgr.saveToDB(9, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("LevelDistribution", TaskLevelDistribution())

class TaskGoldMoneySurplus(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "GoldMoneySurplus", [TE_ROUNDHOUR, TE_ROUNDDAY], [4, 1])
        self.setDBType("data")
        self.setExeType("data")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("GoldMoneySurplus")
            if exeHandler != None:
                tableName = "Player"
                lines = []
                params = { "LoginTime" : int(time.mktime(tm) - 86400)}
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName, params)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["Count"] = 0
                        else:
                            queryResult["Count"] = result[0][0]
                    AnaMgr.saveToDB(10, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("GoldMoneySurplus", TaskGoldMoneySurplus())

class TaskTotalOnlineTime(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "TotalOnlineTime", [TE_ROUNDHOUR, TE_ROUNDDAY], [4, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("TotalOnlineTime")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) == 1 and len(result[0]) == 1:
                        if result[0][0] == None:
                            queryResult["Count"] = 0
                        else:
                            queryResult["Count"] = result[0][0]
                    AnaMgr.saveToDB(11, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("TotalOnlineTime", TaskTotalOnlineTime())

class TaskConsumeGoldBySys(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "ConsumeGoldBySys", [TE_ROUNDHOUR, TE_ROUNDDAY], [1, 1])
        self.setDBType("log")
        self.setExeType("log")

    def notify(self, tm, count, teType):
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("ConsumeGoldBySys")
            if exeHandler != None:
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(time.mktime(tm) - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                lines = []
                for hd in dbHandlers:
                    exeStr = exeHandler.makeExecutionString(tableName)
                    result = hd.executeSql(exeStr)
                    queryResult = {}
                    if len(result) > 0:
                        dataStr = ""
                        for pair in result:
                            try:
                                dataStr += "%s:%s;" %(pair[0], pair[1])
                            except:
                                dataStr += "Unknown:Unknown;"
                                cv.warn("Data error in task ConsumeGoldBySys", True)
                        queryResult["DataBySysUseID"] = dataStr
                    AnaMgr.saveToDB(12, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)

AnaMgr.registerTaskHandler("ConsumeGoldBySys", TaskConsumeGoldBySys())

class TaskDailyCreateCountByTime(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "DailyCreateCountByTime", [TE_ROUNDMINUTE, TE_ROUNDDAY], [5, 1])
        self.setDBType("log")
        self.setExeType("log")
        self.mDataInDay = {}
        self.initDataInDay()

    def notify(self, tm, count, teType):
        if tm.tm_min < 5 and tm.tm_hour == 0 and teType != TE_ROUNDDAY:
            return
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("DailyCreateCountByTime")
            if exeHandler != None:
                ts = int(time.mktime(tm))
                beg, end, idx = self.getTripleParams(tm, ts, teType == TE_ROUNDDAY)
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(ts - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                if teType == TE_ROUNDDAY:
                    self.fixDataInDay(ts - 86400, tableName, dbHandlers, exeHandler)
                lines = []
                params = {
                    "begin" : beg,
                    "end" : end
                }
                for hd in dbHandlers:
                    hdName = hd.getName()
                    if self.mDataInDay and hdName in self.mDataInDay:
                        dataOfHD = self.mDataInDay[hdName]
                        exeStr = exeHandler.makeExecutionString(tableName, params)
                        result = hd.executeSql(exeStr)
                        queryResult = {}
                        if len(result) == 1 and len(result[0]) == 1:
                            if result[0][0] == None:
                                dataOfHD[idx] = 0
                            else:
                                dataOfHD[idx] = result[0][0]
                        else:
                            dataOfHD[idx] = -1
                        dataStr = self.convertDataToString(hdName)
                        if len(dataStr) > 1:
                            queryResult["CountByTime"] = dataStr
                            AnaMgr.saveToDB(13, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)
                    else:
                        cv.err("Handler name is not in mDataInDay", True)

        if teType == TE_ROUNDDAY:
            self.initDataInDay()

    def initDataInDay(self):
        dbHandlers = self.getValidDBHandlers()
        for hd in dbHandlers:
            self.mDataInDay[hd.getName()] = [-1 for i in xrange(288)] # 1440 / 5 每5分一个段 结构不兼容 暂时写常量

    def convertDataToString(self, hdName):
        dataStr = ""
        if self.mDataInDay and hdName in self.mDataInDay:
            dataOfHD = self.mDataInDay[hdName]
            index = 0
            for val in dataOfHD:
                dataStr += str(val)
                if index < 287:
                    dataStr += ';'
                    index += 1
        return dataStr

    def getTripleParams(self, tm, ts, useTailIndex = False):
        if useTailIndex:
            index = 287
        else:
            index = int(tm.tm_hour * 12 + tm.tm_min / 5 - 1)
        end = int(ts - ts % 300)
        begin = end - 300
        return begin, end, index

    def fixDataInDay(self, ts, tableName, dbHandlers, exeHandler):
        for hd in dbHandlers:
            hdName = hd.getName()
            if self.mDataInDay and hdName in self.mDataInDay:
                dataOfHD = self.mDataInDay[hdName]
                for idx in xrange(288):
                    if dataOfHD[idx] == -1:
                        params = {
                            "begin" : int(ts + idx * 300),
                            "end" : int(ts + (idx + 1) * 300)
                        }
                        exeStr = exeHandler.makeExecutionString(tableName, params)
                        result = hd.executeSql(exeStr)
                        queryResult = {}
                        if len(result) == 1 and len(result[0]) == 1:
                            if result[0][0] == None:
                                dataOfHD[idx] = 0
                            else:
                                dataOfHD[idx] = result[0][0]
                        else:
                            dataOfHD[idx] = 0
            else:
                cv.err("Handler name is not in mDataInDay in [FUN:fixDataInDay]", True)


AnaMgr.registerTaskHandler("DailyCreateCountByTime", TaskDailyCreateCountByTime())

class TaskCurOnlineCountByTime(TaskBase):
    def __init__(self):
        TaskBase.__init__(self, "CurOnlineCountByTime", [TE_ROUNDMINUTE, TE_ROUNDDAY], [5, 1])
        self.setDBType("log")
        self.setExeType("log")
        self.mDataInDay = {}
        self.initDataInDay()

    def notify(self, tm, count, teType):
        if tm.tm_min < 5 and tm.tm_hour == 0 and teType != TE_ROUNDDAY:
            return
        dbHandlers = self.getValidDBHandlers()
        if len(dbHandlers) >= 1:
            exeHandler = AnaMgr.getEXEHandler("CurOnlineCountByTime")
            if exeHandler != None:
                ts = int(time.mktime(tm))
                beg, end, idx = self.getTripleParams(tm, ts, teType == TE_ROUNDDAY)
                if teType == TE_ROUNDDAY:
                    tm = time.localtime(ts - 86400)
                tableName = "Logs_%.4d_%.2d_%.2d" % (tm.tm_year, tm.tm_mon, tm.tm_mday)
                if teType == TE_ROUNDDAY:
                    self.fixDataInDay(ts - 86400, tableName, dbHandlers, exeHandler)
                lines = []
                params = {
                    "begin" : beg,
                    "end" : end
                }
                for hd in dbHandlers:
                    hdName = hd.getName()
                    if self.mDataInDay and hdName in self.mDataInDay:
                        dataOfHD = self.mDataInDay[hdName]
                        exeStr = exeHandler.makeExecutionString(tableName, params)
                        result = hd.executeSql(exeStr)
                        queryResult = {}
                        if len(result) >= 1:
                            if result[0][0] == None:
                                dataOfHD[idx] = 0
                            else:
                                countOnline = 0
                                countMem = 0
                                maxCount = 0
                                for rdata in result:
                                    countOnline += rdata[0]
                                    countMem    += rdata[1]
                                    maxCount    += 1
                                dataOfHD[idx] = int(countOnline / maxCount)
                        else:
                            dataOfHD[idx] = -1
                        dataStr = self.convertDataToString(hdName)
                        if len(dataStr) > 1:
                            queryResult["CountByTime"] = dataStr
                        AnaMgr.saveToDB(14, AnaMgr.getZeroTimeStamp(tm), hd.getServerID(), queryResult)
                    else:
                        cv.err("Handler name is not in mDataInDay", True)

        if teType == TE_ROUNDDAY:
            self.initDataInDay()

    def initDataInDay(self):
        dbHandlers = self.getValidDBHandlers()
        for hd in dbHandlers:
            self.mDataInDay[hd.getName()] = [-1 for i in xrange(288)] # 1440 / 5 每5分一个段 结构不兼容 暂时写常量

    def convertDataToString(self, hdName):
        dataStr = ""
        index = 0
        if self.mDataInDay and hdName in self.mDataInDay:
            dataOfHD = self.mDataInDay[hdName]
            for val in dataOfHD:
                dataStr += str(val)
                if index < 287:
                    dataStr += ';'
                    index += 1
        return dataStr

    def getTripleParams(self, tm, ts, useTailIndex = False):
        if useTailIndex:
            index = 287
        else:
            index = int(tm.tm_hour * 12 + tm.tm_min / 5 - 1)
        end = int(ts - ts % 300)
        begin = end - 300
        return begin, end, index

    def fixDataInDay(self, ts, tableName, dbHandlers, exeHandler):
        for hd in dbHandlers:
            hdName = hd.getName()
            if self.mDataInDay and hdName in self.mDataInDay:
                dataOfHD = self.mDataInDay[hdName]
                for idx in xrange(288):
                    if dataOfHD[idx] == -1:
                        params = {
                            "begin" : int(ts + idx * 300),
                            "end" : int(ts + (idx + 1) * 300)
                        }
                        exeStr = exeHandler.makeExecutionString(tableName, params)
                        result = hd.executeSql(exeStr)
                        queryResult = {}
                        if len(result) >= 1:
                            if result[0][0] == None:
                                dataOfHD[idx] = 0
                            else:
                                countOnline = 0
                                countMem = 0
                                maxCount = 0
                                for rdata in result:
                                    countOnline += rdata[0]
                                    countMem    += rdata[1]
                                    maxCount    += 1
                                dataOfHD[idx] = int(countOnline / maxCount)
                        else:
                            dataOfHD[idx] = 0
            else:
                cv.err("Handler name is not in mDataInDay in [FUN:fixDataInDay]", True)

AnaMgr.registerTaskHandler("CurOnlineCountByTime", TaskCurOnlineCountByTime())
