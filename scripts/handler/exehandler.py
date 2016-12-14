#coding:utf-8
from scripts.utils.views import *
import time
from scripts.analysismgr import *

class ExecutionHandlerBase():
    def __init__(self, name, exetype, template = ""):
        self.mName = name
        self.mType = exetype.lower()
        self.mTemplate = template

    def makeWithTemplate(self, params = []):
        sql = self.mTemplate
        for i in xrange(len(params)):
            sql = sql.replace('#' + str(i + 1), params[i])
        return sql

    def makeExecutionString(self, params = {}):
        return ""

    def getName(self):
        return self.mName

    def getType(self):
        return self.mType

    def getTemplate(self):
        return self.mTemplate
#=============================================================
class Handler_Recharge(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"Recharge", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            exeType = params["ExeType"]
            #startTime = int(params["BeginTime"])
            #endTime = int(params["EndTime"])
            baseExe = ""
            if exeType == 1:
                baseExe = "select sum(Param2) from %s where LogType = 12 and Param0 = 12" %(tableName)
            elif exeType == 2:
                baseExe = "select count(distinct(Account)) from %s where LogType = 12 and Param0 = 12" %(tableName)
            elif exeType == 3:
                baseExe = "select count(1) from %s where LogType = 12 and Param0 = 12" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("Recharge", Handler_Recharge())
#=============================================================
class Handler_ConsumeGold(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"ConsumeGold", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select sum(Param1) from %s where LogType = 18" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("ConsumeGold", Handler_ConsumeGold())
#=============================================================
class Handler_DailyActive(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"DailyActive", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select count(distinct(Account)) from %s where LogType = 4" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("DailyActive", Handler_DailyActive())
#=============================================================
class Handler_ConsumeGoldByAct(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"ConsumeGoldByAct", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select ParamDescription, sum(Param1) from %s where LogType = 18 and Param0 = 11 group by ParamDescription" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("ConsumeGoldByAct", Handler_ConsumeGoldByAct())
#=============================================================
class Handler_DailyCreate(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"DailyCreate", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select count(*) from %s where LogType = 3" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("DailyCreate", Handler_DailyCreate())
#=============================================================
class Handler_VipLevel(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"VipLevel", "Data")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select VipLevel, count(*) from %s group by VipLevel" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("VipLevel", Handler_VipLevel())
#=============================================================
class Handler_Subsistence(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self, "Subsistence", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select count(distinct(Account)) from %s where LogType = 4 and Account in (select distinct(Account) from %s where LogType = 3);"
            baseExe = baseExe %(tableName, params["Relative"])
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("Subsistence", Handler_Subsistence())
#=============================================================
class Handler_ItemSoldInMall(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"ItemSoldInMall", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select param2, sum(param3) from %s where LogType = 14 and param0 = 9 and ParamDescription = '23' group by param2" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("ItemSoldInMall", Handler_ItemSoldInMall())

#=============================================================
class Handler_LevelDistribution(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"LevelDistribution", "Data")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select Level, count(*) from %s group by Level" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("LevelDistribution", Handler_LevelDistribution())

#=============================================================
class Handler_GoldMoneySurplus(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"GoldMoneySurplus", "Data")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select sum(GoldMoney) from %s where LoginTime > %s" %(tableName, params["LoginTime"])
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("GoldMoneySurplus", Handler_GoldMoneySurplus())

#=============================================================
class Handler_TotalOnlineTime(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"TotalOnlineTime", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select sum(param0) from %s where LogType = 5" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("TotalOnlineTime", Handler_TotalOnlineTime())

#=============================================================
class Handler_ConsumeGoldBySys(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self, "ConsumeGoldBySys", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select Param0, sum(Param1) from %s where LogType = 18 and Param0 != 11 group by Param0" %(tableName)
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("ConsumeGoldBySys", Handler_ConsumeGoldBySys())

#=============================================================
class Handler_DailyCreateCountByTime(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"DailyCreateCountByTime", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select count(*) from %s where LogType = 3 and LogTime >= %s and LogTime < %s" %(tableName, params["begin"], params["end"])
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("DailyCreateCountByTime", Handler_DailyCreateCountByTime())

#=============================================================
class Handler_CurOnlineCountByTime(ExecutionHandlerBase):
    def __init__(self):
        ExecutionHandlerBase.__init__(self,"CurOnlineCountByTime", "Log")

    def makeExecutionString(self, tableName, params = {}):
        try:
            baseExe = "select param0, param1 from %s where LogType = 2 and LogTime >= %s and LogTime < %s" %(tableName, params["begin"], params["end"])
            return baseExe
        except:
            cv.err("Parameters has some error!", True)
            return ""

AnaMgr.registerEXEHandler("CurOnlineCountByTime", Handler_CurOnlineCountByTime())

