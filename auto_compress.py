# -*- coding: utf-8 -*-

import os
import datetime
import tarfile
import logging


# 获取文件路径集合
def getFile(logsPath):
    logsFile = []
    fileList = os.listdir(logsPath)
    for fileName in fileList:
        path = os.path.join(logsPath, fileName)
        # 过滤子文件的文件被压缩
        if os.path.isdir(path):
            continue
        logsFile.append(path)
    return logsFile

#单位MB
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024)
    return round(fsize,2)

# 检查文件,备份文件
def checkFile(logsFiles):
    logging.info('Start Compressing Logs...')
    
    for f in logsFiles:
        # 过滤压缩文件检查
        if f.endswith(".tar.gz"):
            continue
        elif get_FileSize(f) > 1024:
            continue
        
        nowDate = datetime.datetime.now().strftime('%Y-%m-%d')
#        # 已经压缩完成的日志文件,删除原日志文件,保留压缩文件
#        for apath in logsFiles:
#            if apath.endswith(".tar.gz") and apath.endswith("%s.tar.gz" % mat.group()):
#                os.remove(apath)
#                break
        # 没有压缩的文件,压缩后,删除原文件
        tar = tarfile.open("%s_%s.tar.gz" % (f, nowDate), "w:gz")
        tar.add(f)
        tar.close()
        os.remove(f)

    logging.info('Compressing Logs Finish...')

if __name__ == "__main__":
    # 请修改日志路径,注意路径一定要完整,后面要使用str切割,不要省略路径某位的/
    # windows: F://application_logs//  linux: /opt/applog/
    logsPath = "/log"
    logsFiles = getFile(logsPath)
    checkFile(logsFiles)