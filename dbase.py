#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymysql as mdb
import re, os, sys

conn_dict = {}

MDB_IP = '10.19.32.40'
MDB_PORT = 3306
MDB_USER = 'smartqb'
MDB_PWD = 'notsmart@All'
MDB_DB = 'sqlreviewer'

def getDateString(timer,format="%Y-%m-%d"):
	return timer.strftime(format)

#通过sql语句得到结果集
def fetchall(sql, ip = None, port = None, user = None, pwd = None, db = None):
	#print(sql)
	MyDB = DBSource(ip, port, user, pwd, db)
	return MyDB.get_result(sql)

def fetchone(sql, ip = None, port = None, user = None, pwd = None, db = None):
	#print(sql)
	MyDB = DBSource(ip, port, user, pwd, db)
	rowset = MyDB.get_result(sql)
	for row in rowset:
		return row

#判断结果集是否为None
def isfull(rowset):
	if not rowset or not rowset[0]:
		return False
	else:
		return True

#将结果集转化成想要的字符串
def tostring(rowset,name,type=1):
	if not rowset[0] :
		return "None"
	index = rowset.index(name) 
	string = ""
	if type == 1:
		for row in rowset:
			string += "'%s',"%(str(row[index]).replace("\\","\\\\").replace("'","\\'"))
	else :
		for row in rowset:
			string += "%d,"%(int(row[index]))
	string = string.rstrip(",")
	return string 

#保存
def save(sql, ip = None, port = None, user = None, pwd = None, db = None):
	MyDB = DBSource(ip, port, user, pwd, db)
	#print(sql)
	MyDB.save(sql)


class DBSource:

	_Host = MDB_IP
	_DB = MDB_DB
	_Port = MDB_PORT
	_User = MDB_USER
	_Passwd = MDB_PWD

	_conn = None
	_cursor = None

	def __init__(self,ip, port, user, pwd, db):
		if ip is not None:
			self._Host = ip
		if port is not None:
			self._Port = port
		if user is not None:
			self._User = user
		if pwd is not None:
			self._Passwd = pwd
		if db is not None:
			self._DB = db

		self.get_conn(self._Host, self._DB, self._User, self._Passwd, self._Port)
		self.get_cursor()

	def get_conn(self, host=MDB_IP, db=MDB_DB, user=MDB_USER, passwd=MDB_PWD, port=MDB_PORT):
		try:
			key = host + "_" + db + "_" + str(port)
			#print key
			if key in conn_dict and conn_dict[key] != None :
				self._conn = conn_dict[key]
			else:
				self._conn = mdb.Connect(host=host, db=db, user=user, passwd=passwd, port=port, charset="utf8")
				conn_dict[key] = self._conn
		except Exception as e:
			raise RuntimeError("Error in connection " + str(e))

	def get_cursor(self):
		try :
			self._cursor=self._conn.cursor()
			self._cursor.execute("set autocommit = 1")
		except Exception as e:
			raise RuntimeError("Error in cursor")
	
	#关闭连接	
	def close(self):
		self._conn.close()
		self._conn = None
		self._cursor = None

	#连接数据库
	def get_connect(self):
		#print self._Host,self._DB,self._User,self._Passwd,self._Port
		self.get_conn(self._Host, self._DB, self._User, self._Passwd, self._Port)
		self.get_cursor()

	#保存
	def save(self, sql):
		try:
			self.get_connect()
			self._cursor.execute(sql)
		except Exception as ex:
			conn_dict[self._Host+"_"+self._DB+"_"+str(self._Port)] = None
			self.get_connect()
			self._cursor.execute(sql)

	#query
	def get_result(self, sql):
		try:
			self.get_connect()		
			self._cursor.execute(sql)
		except Exception as ex:
			conn_dict[self._Host+"_"+self._DB+"_"+str(self._Port)] = None
			self.get_connect()
			self._cursor.execute(sql)
		rowset = self._cursor.fetchall()
		if rowset and len(rowset)>0 :
			record = RecordSet([list(x) for x in rowset],[x[0].lower( ) for x in self._cursor.description])
		else:
			record = [None,]
		return record


#将结果集转化成自定义的recordset的结果	
class RecordSet:
	def __init__(self, tableData, columnNames):
		self.data = tableData
		self.columns = columnNames
		self.columnMap = {}

		for name,n in zip(columnNames, range(0,10000)):
			self.columnMap[name] = n

	def __getitem__(self, n):
		return Record(self.data[n], self.columnMap)

	def __setitem__(self, n, value):
		self.data[n] = value

	def __delitem__(self, n):
		del self.data[n]

	def __len__(self):
		return len(self.data)

	def __str__(self):
		return '%s: %s' % (self.__class__,self.columns)

	def index(self,name):
		i = 0
		for colnum in self.columns:
			if colnum == name:
				return i
			i += 1
		return i

	def __nonzero__(self):
		if len(self.data) == 0 :
			return False
		else:
			return True

	def getcolumn(self):
		return self.columns

	def getdata(self):
		return self.data

class Record:
	def __init__(self, rowData, columnMap):
		self.__dict__['_data_'] = rowData
		self.__dict__['_map_'] = columnMap

	def __getattr__(self, name):
		return self._data_[self._map_[name]]

	def __setattr__(self, name, value):
		try:
			n = self._map_[name]
		except KeyError:
			self.__dict__[name] = value
		else:
			self._data_[n] = value

	def __getitem__(self, n):
		return self._data_[n]

	def __setitem__(self, n, value):
		self._data_[n] = value

	def __getslice__(self, i, j):
		return self._data_[i:j]

	def __setslice__(self, i, j, slice):
		self._data_[i:j] = slice

	def __len__(self):
		return len(self._data_)

	def __str__(self):
		return '%s: %s' % (self.__class__,repr(self._data_))

	def __nonzero__(self):
		if len(self._data_) == 0:
			return False
		else:
			return True

	def __iter__(self):
		return iter(self._data_)

	def getdata(self):
		return self._data_

