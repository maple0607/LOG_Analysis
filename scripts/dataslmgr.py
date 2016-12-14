#coding:utf-8

from scripts.handler import dbhandler
from scripts.utils.views import *

# data type
DT_Pay                    = 1
DT_ConsumeGold            = 2
DT_DailyActive            = 3
DT_CsmGoldByAct           = 4
DT_DailyCreate            = 5
DT_VipLevel               = 6
DT_Subsistence            = 7
DT_ItemSoldInMall         = 8
DT_LevelDistribution      = 9
DT_GoldMoneySurplus       = 10
DT_TotalOnlineTime        = 11
DT_CsmGoldBySys           = 12
DT_DailyCreateCountByTime = 13
DT_CurOnlineCountByTime   = 14


class DataSLMgr():
    def __init__(self):
        self.mDBHandler = None
        self.mSLHandler = None
        self.mDBNames = []
        self.mTabNames = []

    def new(self, 
        name = "ResultDB", 
        serverId = -1, 
        dbType = "system", 
        user = "root", 
        psswd = "123456", 
        host = "192.168.0.248", 
        port = 3306, 
        dbname = "", 
        charset = "utf8"):
        self.mDBHandler = dbhandler.DBHandler(name, serverId, dbType, user, psswd, host, port, dbname, charset)
        self.initDB()
        self.initSLHandlers()
        return self

    def initDB(self):
        cv.log("Init databases...", True)
        sqlStr = "show databases"
        result = self.mDBHandler.executeSql(sqlStr)
        for value in result:
            self.mDBNames.append(value[0])
        if "QueryResult" not in self.mDBNames:
            cv.log("Create database QueryResult", True)
            sqlStr = "create database if not exists `QueryResult` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci"
            self.mDBHandler.executeSql(sqlStr)
        self.mDBHandler.setDBName("QueryResult")

        sqlStr = "show tables"
        result = self.mDBHandler.executeSql(sqlStr)
        for value in result:
            self.mTabNames.append(value[0])

        if "PayData" not in self.mTabNames:
            cv.log("Create table PayData", True)
            sqlStr =  "CREATE TABLE if not exists `PayData` (\
                      `Date` int(11) NOT NULL COMMENT '数据时间',\
                      `ServerID` int(11) NOT NULL COMMENT '服务器ID',\
                      `Sum` int(11) NOT NULL COMMENT '总充值额度',\
                      `Count` int(11) NOT NULL COMMENT '总充值次数',\
                      `AccountCount` int(11) NOT NULL COMMENT '总充值人数',\
                      PRIMARY KEY (`Date`,`ServerID`)\
                      ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "ConsumeGold" not in self.mTabNames:
            cv.log("Create table ConsumeGold", True)
            sqlStr = "CREATE TABLE if not exists `ConsumeGold` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `Count` int(11) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "DailyActive" not in self.mTabNames:
            cv.log("Create table DailyActive", True)
            sqlStr = "CREATE TABLE if not exists `DailyActive` (\
                      `Date` int(11) NOT NULL COMMENT '记录时间',\
                      `ServerID` int(11) NOT NULL,\
                      `ActiveCount` int(11) NOT NULL COMMENT '活跃',\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "ConsumeGoldByAct" not in self.mTabNames:
            cv.log("Create table ConsumeGoldByAct", True)
            sqlStr = "CREATE TABLE if not exists `ConsumeGoldByAct` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `DataByActID` varchar(1024) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "DailyCreate" not in self.mTabNames:
            cv.log("Create table DailyCreate", True)
            sqlStr = "CREATE TABLE if not exists`DailyCreate` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `Count` int(11) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "VipLevel" not in self.mTabNames:
            cv.log("Create table VipLevel", True)
            sqlStr = "CREATE TABLE if not exists `VipLevel` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `Data` varchar(256) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "Subsistence" not in self.mTabNames:
            cv.log("Create table Subsistence", True)
            sqlStr = "CREATE TABLE if not exists `Subsistence` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `Data` varchar(256) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "ItemSoldInMall" not in self.mTabNames:
            cv.log("Create table ItemSoldInMall", True)
            sqlStr = "CREATE TABLE if not exists `ItemSoldInMall` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `DataByItemID` varchar(2048) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "LevelDistribution" not in self.mTabNames:
            cv.log("Create table LevelDistribution", True)
            sqlStr = "CREATE TABLE if not exists `LevelDistribution` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `DataByLevel` varchar(2048) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "GoldMoneySurplus" not in self.mTabNames:
            cv.log("Create table GoldMoneySurplus", True)
            sqlStr = "CREATE TABLE if not exists `GoldMoneySurplus` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `Count` int(11) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "TotalOnlineTime" not in self.mTabNames:
            cv.log("Create table TotalOnlineTime", True)
            sqlStr = "CREATE TABLE if not exists `TotalOnlineTime` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `Count` int(11) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "ConsumeGoldBySys" not in self.mTabNames:
            cv.log("Create table ConsumeGoldBySys", True)
            sqlStr = "CREATE TABLE if not exists `ConsumeGoldBySys` ( \
                     `Date` int(11) NOT NULL, \
                     `ServerID` int(11) NOT NULL,\
                     `DataBySysUseID` varchar(1024) NOT NULL,\
                     PRIMARY KEY (`Date`,`ServerID`)\
                     ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "DailyCreateCountByTime" not in self.mTabNames:
            cv.log("Create table DailyCreateCountByTime", True)
            sqlStr = "CREATE TABLE if not exists `DailyCreateCountByTime` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `CountByTime` varchar(2048) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        if "CurOnlineCountByTime" not in self.mTabNames:
            cv.log("Create table CurOnlineCountByTime", True)
            sqlStr = "CREATE TABLE if not exists `CurOnlineCountByTime` (\
                      `Date` int(11) NOT NULL,\
                      `ServerID` int(11) NOT NULL,\
                      `CountByTime` varchar(2048) NOT NULL,\
                      PRIMARY KEY (`Date`,`ServerID`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"
            self.mDBHandler.executeSql(sqlStr)

        cv.log("Init databases has done!", True)

    def initSLHandlers(self):
        self.mSLHandler = {
        DT_Pay                    : self.updatePay,
        DT_ConsumeGold            : self.updateConsumeGold,
        DT_DailyActive            : self.updateDailyActive,
        DT_CsmGoldByAct           : self.updateCsmGoldByAct,
        DT_DailyCreate            : self.updateDailyCreate,
        DT_VipLevel               : self.updateVipLevel,
        DT_Subsistence            : self.updateSubsistence,
        DT_ItemSoldInMall         : self.updateItemSoldInMall,
        DT_LevelDistribution      : self.updateLevelDistribution,
        DT_GoldMoneySurplus       : self.updateGoldMoneySurplus,
        DT_TotalOnlineTime        : self.updateTotalOnlineTime,
        DT_CsmGoldBySys           : self.updateCsmGoldBySys,
        DT_DailyCreateCountByTime : self.updateDailyCreateCountByTime,
        DT_CurOnlineCountByTime   : self.updateCurOnlineCountByTime
        }

    def updateData(self, dtType, recordTS, serverId, params):
        if dtType in self.mSLHandler:
            self.mSLHandler[dtType](recordTS, serverId, params)
        else:
            cv.err("Data SL type is not existing!", True)

    def readData(self):
        pass

    def writeData(self):
        pass

    def updatePay(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from PayData where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update PayData set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "Sum" in params:
                updateSQL += ",Sum = %s " % (params["Sum"])
            if "Count" in params:
                updateSQL += ",Count = %s " % (params["Count"])
            if "AccountCount" in params:
               updateSQL += ",AccountCount = %s " % (params["AccountCount"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "Sum" in params:
                Sum = params["Sum"]
            else:
                Sum = 0
            if "Count" in params:
                Count = params["Count"]
            else:
                Count = 0
            if "AccountCount" in params:
                AccountCount = params["AccountCount"]
            else:
                AccountCount = 0
            updateSQL = "insert into PayData values(%s, %s, %s, %s, %s)" %(recordTS, serverId, Sum, Count, AccountCount)
        self.mDBHandler.executeSql(updateSQL)

    def updateConsumeGold(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from ConsumeGold where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update ConsumeGold set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "Count" in params:
                updateSQL += ",Count = %s " % (params["Count"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "Count" in params:
                Count = params["Count"]
            else:
                Count = 0
            updateSQL = "insert into ConsumeGold values(%s, %s, %s)" %(recordTS, serverId, Count)
        self.mDBHandler.executeSql(updateSQL)

    def updateDailyActive(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from DailyActive where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update DailyActive set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "ActiveCount" in params:
                updateSQL += ",ActiveCount = %s " % (params["ActiveCount"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "ActiveCount" in params:
                ActiveCount = params["ActiveCount"]
            else:
                ActiveCount = 0
            updateSQL = "insert into DailyActive values(%s, %s, %s)" %(recordTS, serverId, ActiveCount)
        self.mDBHandler.executeSql(updateSQL)

    def updateCsmGoldByAct(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from ConsumeGoldByAct where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update ConsumeGoldByAct set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "DataByActID" in params:
                updateSQL += ", DataByActID = '%s' " % (params["DataByActID"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "DataByActID" in params:
                DataByActID = params["DataByActID"]
            else:
                DataByActID = ""
            updateSQL = "insert into ConsumeGoldByAct values(%s, %s, '%s')" %(recordTS, serverId, DataByActID)
        self.mDBHandler.executeSql(updateSQL)

    def updateDailyCreate(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from DailyCreate where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update DailyCreate set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "Count" in params:
                updateSQL += ", Count = %s " % (params["Count"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "Count" in params:
                Count = params["Count"]
            else:
                Count = 0
            updateSQL = "insert into DailyCreate values(%s, %s, %s)" %(recordTS, serverId, Count)
        self.mDBHandler.executeSql(updateSQL)

    def updateVipLevel(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from VipLevel where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update VipLevel set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "Data" in params:
                updateSQL += ", Data = '%s' " % (params["Data"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "Data" in params:
                Data = params["Data"]
            else:
                Data = "0;\
                0;0;0;0;0;\
                0;0;0;0;0;\
                0;0;0;0;0;"
            updateSQL = "insert into VipLevel values(%s, %s, '%s')" %(recordTS, serverId, Data)
        self.mDBHandler.executeSql(updateSQL)

    def updateSubsistence(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from Subsistence where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update Subsistence set Date = %s, ServerID = %s " %(recordTS, serverId)
            try:
                Count = params["Count"]
                Type = params["Type"]
                dataDict = self.protoData(dataInDB[0][2])
                dataDict[Type] = Count
                Data = ""
                for t in dataDict:
                    Data += "%s:%s;" %(t, dataDict[t])
                updateSQL += ", Data = '%s' " % (Data)
            except:
                cv.err("Failed to write down data in updateSubsistence", True)
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            try:
                Count = params["Count"]
                Type = params["Type"]
                dataDict = self.protoData()
                dataDict[Type] = Count
                Data = ""
                for t in dataDict:
                    Data += "%s:%s;" %(t, dataDict[t])
            except:
                cv.log("Using default data in updateSubsistence!", True)
                Data = "2:0;3:0;7:0;15:0;30:0;"
            updateSQL = "insert into Subsistence values(%s, %s, '%s')" %(recordTS, serverId, Data)
        self.mDBHandler.executeSql(updateSQL)

    def protoData(self, dataStr = None):
        if dataStr:
            data = {}
            pairs = dataStr.split(";")
            for pair in pairs:
                kv = pair.split(":")
                if len(kv) == 2:
                    data[int(kv[0])] = kv[1]
            return data
        else:
            return {2:0, 3:0, 7:0, 15:0, 30:0,}

    def updateItemSoldInMall(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from ItemSoldInMall where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update ItemSoldInMall set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "DataByItemID" in params:
                updateSQL += ", DataByItemID = '%s' " % (params["DataByItemID"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "DataByItemID" in params:
                DataByItemID = params["DataByItemID"]
            else:
                DataByItemID = ""
            updateSQL = "insert into ItemSoldInMall values(%s, %s, '%s')" %(recordTS, serverId, DataByItemID)
        self.mDBHandler.executeSql(updateSQL)

    def updateLevelDistribution(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from LevelDistribution where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update LevelDistribution set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "DataByLevel" in params:
                updateSQL += ", DataByLevel = '%s' " % (params["DataByLevel"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "DataByLevel" in params:
                DataByLevel = params["DataByLevel"]
            else:
                DataByLevel = ""
            updateSQL = "insert into LevelDistribution values(%s, %s, '%s')" %(recordTS, serverId, DataByLevel)
        self.mDBHandler.executeSql(updateSQL)

    def updateGoldMoneySurplus(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from GoldMoneySurplus where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update GoldMoneySurplus set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "Count" in params:
                updateSQL += ", Count = %s " % (params["Count"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "Count" in params:
                Count = params["Count"]
            else:
                Count = 0
            updateSQL = "insert into GoldMoneySurplus values(%s, %s, %s)" %(recordTS, serverId, Count)
        self.mDBHandler.executeSql(updateSQL)

    def updateTotalOnlineTime(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from TotalOnlineTime where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update TotalOnlineTime set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "Count" in params:
                updateSQL += ", Count = %s " % (params["Count"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "Count" in params:
                Count = params["Count"]
            else:
                Count = 0
            updateSQL = "insert into TotalOnlineTime values(%s, %s, %s)" %(recordTS, serverId, Count)
        self.mDBHandler.executeSql(updateSQL)

    def updateCsmGoldBySys(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from ConsumeGoldBySys where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update ConsumeGoldBySys set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "DataBySysUseID" in params:
                updateSQL += ", DataBySysUseID = '%s' " % (params["DataBySysUseID"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "DataBySysUseID" in params:
                DataBySysUseID = params["DataBySysUseID"]
            else:
                DataBySysUseID = ""
            updateSQL = "insert into ConsumeGoldBySys values(%s, %s, '%s')" %(recordTS, serverId, DataBySysUseID)
        self.mDBHandler.executeSql(updateSQL)

    def updateDailyCreateCountByTime(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from DailyCreateCountByTime where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update DailyCreateCountByTime set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "CountByTime" in params:
                updateSQL += ", CountByTime = '%s' " % (params["CountByTime"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "CountByTime" in params:
                CountByTime = "%s" % (params["CountByTime"])
            else:
                CountByTime = ""
            updateSQL = "insert into DailyCreateCountByTime values(%s, %s, '%s')" %(recordTS, serverId, CountByTime)
        self.mDBHandler.executeSql(updateSQL)

    def updateCurOnlineCountByTime(self, recordTS, serverId, params):
        dataInDB = self.mDBHandler.executeSql("select * from CurOnlineCountByTime where Date = %s and ServerID = %s " %(recordTS, serverId))
        if len(dataInDB) > 0:
            updateSQL = "update CurOnlineCountByTime set Date = %s, ServerID = %s " %(recordTS, serverId)
            if "CountByTime" in params:
                updateSQL += ", CountByTime = '%s' " % (params["CountByTime"])
            updateSQL = updateSQL + "where Date = %s and ServerID = %s " %(recordTS, serverId)
        else:
            if "CountByTime" in params:
                CountByTime = "%s" % (params["CountByTime"])
            else:
                CountByTime = ""
            updateSQL = "insert into CurOnlineCountByTime values(%s, %s, '%s')" %(recordTS, serverId, CountByTime)
        self.mDBHandler.executeSql(updateSQL)
