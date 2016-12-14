#coding:utf-8

import os
import platform
import sys
import time
from scripts.analysismgr import *
from scripts.utils.views import *

if __name__ == "__main__":
    try:
        if len(sys.argv) == 3 and sys.argv[2] == "cmd":
                pass
        else:
            if platform.uname()[0] == "Windows":
                os.system('cls')
            elif platform.uname()[0] == "Linux":
                os.system('clear')
            cv.warn("Analysis Manager will be activated in [10] seconds!")
            time.sleep(4)
            cv.warn("Active Analysis Manager, in...")
            time.sleep(1)
            cv.warn("Five...")
            time.sleep(1)
            cv.warn("Four...")
            time.sleep(1)
            cv.warn("Three...")
            time.sleep(1)
            cv.warn("Two...")
            time.sleep(1)
            cv.warn("one...")
            time.sleep(1)
            cv.log("Analysis Manager is Activated!", True)
            if platform.uname()[0] == "Windows":
                AnaMgr.startup(True)
                while True:
                    op = raw_input("")
            else:
                AnaMgr.startup()
    except:
        cv.warn("Interupted by keyboard!", True)
        cv.warn("Analysis Manager will be terminated in [5] seconds!", True)
        time.sleep(2)
        for i in xrange(3):
            cv.log("[%s]" %(3 - i), True)
            time.sleep(1)
        AnaMgr.shutdown()