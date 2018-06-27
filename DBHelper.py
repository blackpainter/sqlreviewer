#!/usr/bin/python2.6
# -- coding: utf-8 --

import datetime, time
import re, hashlib
import os, sys, socket

class Stack(object):
	items = []

	def __init__(self):
		self.items = []

	def is_empty(self):
		return self.items == []

	def peek(self):
		return self.items[-1]

	def size(self):
		return len(self.items)

	def push(self, item):
		self.items.append(item)

	def pop(self):
		return self.items.pop()

	def __repr__(self):
		return str(self.items)

#当前log文件大于100M时生成新的log
def new_file_when_toobig(filepath, size):
	fsize = os.popen("ls -l %s | awk '{print $5}'" %  filepath).readline().strip()
	if fsize.isdigit() and int(fsize) >= size:
		renamecmd = "mv %s %s_%s.log" % (filepath, filepath[:filepath.rfind('.')], time.strftime('%Y%m%d%H%M%S',time.localtime(time.time())))
		os.system(renamecmd)
		os.system("touch %s" % filepath)
	
#获取本机mysql实例信息
def get_mysql_instances_local(conf = ["port", "socket", "basedir", "datadir"]):
	mysql_instances = {}

	for line in os.popen('ps -aux | grep "mysqld "'):
		ins = {}
		ll = line.split(" ")
		if ll[0] == "mysql" or ll[0] == "root" and len(ll)>11:
			for l in ll:
				if "=" in l:
					l = l.replace("--", "")
					if l[:l.index("=")] in conf:
						ins[l[:l.index("=")]] = l[l.index("=")+1:].strip()
				if "=" not in l and '/' in l and 'mysqld' in l:
					ins["mysql_ext"] =l
			if "port" in ins.keys() and (ins["port"] not in mysql_instances.keys()) and ins["port"].isdigit():
				mysql_instances[ins["port"]] = ins
			elif len(ins.keys())>0 and len(mysql_instances.keys())==0:
				mysql_instances["3306"] = ins
	return mysql_instances

#获得本地IP地址
def get_local_ipadd():
	ip = ""
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.connect(('8.8.8.8', 80))
		ip = s.getsockname()[0]
	finally:
		s.close()
	return ip

#检查字符串是否为合法ip地址
def isIpadd(ipStr):
	compile_ip=re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
	if compile_ip.match(ipStr):
		return True
	else:
		return False

#检查本机备份盘剩余空间
def get_backupdisk_free():
	largest_disk = 0
	largest_disk_free = 0
	backup_disk_free = 0

	for line in os.popen("df"):
		ll = line.split(" ")
		if len(ll)>=5 and ll[-1][0] == '/':
			lastc = ll[-1]
			if len(lastc)>=6 and lastc[len(lastc)-6:] == "backup" and ll[-2][:len(ll[-2])-1].isdigit():
				backup_disk_free = int(ll[-2][:len(ll[-2])-1])
	
			if ll[-5].isdigit() and int(ll[-5])>largest_disk and ll[-2][:len(ll[-2])-1].isdigit():
				largest_disk = int(ll[-5])
				largest_disk_free = int(ll[-2][:len(ll[-2])-1])

	if backup_disk_free <= 0:
		backup_disk_free = largest_disk_free

	return backup_disk_free

def genearteMD5(str):
	hl = hashlib.md5()
	hl.update(str.encode(encoding='utf-8'))

	return hl.hexdigest()


