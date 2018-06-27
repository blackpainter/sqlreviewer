#!/usr/bin/python
# -- coding: utf-8 --

import datetime, time
import os, sys
import dbase
import sqlparse, re
import DBHelper as dbh
from sqlparse.tokens import Keyword, Whitespace
from DBHelper import Stack

sql_info = {"h":"", "P":"3306", "D":"", "e":""}

CARDINALITY_LEAST = 100
TABLE_ROWCNT_NEED_IDX = 100

'''
TABLES = []
WHERES = None
JOINONS = None

MSGS = []

#例：{'TABLENAME.CREATE_TIME':['2018-04-01', '2018-05-01']}
TIME_RANGES = {}

def addMSG(msg):
	if msg not in MSGS:
		print(msg)
		MSGS.append(msg)

#def allMSG():
#	return MSGS

def reset():
	TABLES = []
	WHERES = None
	JOINONS = None
	MSGS = []
'''

class ParseResult:

	def __init__(self):
		self.dbInfo = None
		self.sqls_parsed = []
		self.MSGS = []
		self.dbmeta_source = '暂缺'

		self.TABLES = []
		self.WHERES = None
		self.TIME_RANGES = {}
		self.SELECTS = []

		self.rows = 0

	def setValues(self, dbInfo, msgs):
		self.dbInfo = dbInfo
		#self.sqls_parsed = sqls_parsed
		self.msgs = msgs
		self.dbmeta_source = '暂缺'

		if dbInfo is not None:
			self.dbmeta_source = '%s:%d/%s' % (self.dbInfo.ip, self.dbInfo.port, self.dbInfo.db)
	
	def addMSG(self, msg):
		if msg not in self.MSGS:
			print(msg)
			self.MSGS.append(msg)	

	def reset(self):
		self.dbmeta_source = '暂缺'
		self.msgs = []
		self.dbInfo = None
		self.sql_parsed
		self.TABLES = []
		self.WHERES = None
		self.JOINONS = None
		self.TIME_RANGES = {}
		self.SELECTS = []
		self.rows = 0

class DBInfo:
	def __init__(self, db_id, ip, port, db):
		self.db_id = 0
		self.ip = ip
		self.port = port
		self.db = db

#数据表
class DBTable:

	def __init__(self, schema, ips, tableToken, tableType, result):
		self.tableName = tableToken.get_real_name().replace('`', '')
		self.alias = ''
		if tableToken.has_alias():
			self.alias = tableToken.get_alias().replace('`', '')
		
		ip_ports = []

		for ip in ips:
			port = '3306'
			if ':' in ip:
				port = ip[ip.index(':')+1:]
				ip = ip[:ip.index(':')]
			ip_ports.append('(g.ip = "%s" and g.port = %s)' % (ip, port))

		#self.parentDb = db_info
		#通过from还是left/right/inner join
		self.tableType = tableType
		
		self.tableId = 0
		self.rows_cnt = 0
		self.createStr = ''
		self.parentDb = None
		self.update_time = ''
		
		self.forceIndex = []
		
		#元素：[列名, ESC/DESC]
		self.orderBy = []
		self.groupBy = []
		
		#key为 表名.列名，value为DBColumn
		self.columns = {}
		
		#key为索引名, value为[列名1, 列名2], 有序
		self.indexes = []
		
		#被使用的index
		self.index_used = []
		
		#分层级存储在该表上的where条件，结构 {层级0: [Where0, where1], 层级1: [where0, where1]}
		self.whereStrs = {}
		
		#table_sql = "select t.id, t.db_id, t.rows_cnt, t.create_str from table_info t where t.db_id = %d and t.table_name = '%s'" % (self.parentDb.db_id, self.tableName)
		table_sql = 'select dbi.id, dbi.ip, dbi.port, t.id, t.update_time, t.rows_cnt, t.create_str from db_info dbi, table_info t, db_group_info g where %s and dbi.db="%s" and t.table_name="%s" and g.info_id=dbi.id and t.db_id=dbi.id order by t.update_time desc limit 1' % ('(%s)' % (' OR '.join(s for s in ip_ports) if len(ip_ports)>0 else '1=1'), schema, self.tableName)
		table_rs = dbase.fetchone(table_sql)
		if dbase.isfull(table_rs):
			self.parentDb = DBInfo(table_rs[0], table_rs[1], table_rs[2], schema)
			self.tableId = table_rs[3]
			self.update_time = table_rs[4]
			self.rows_cnt = table_rs[5]
			self.createStr = table_rs[6]

			self.init_Columns(result)

		else:
			#raise RuntimeError('No table `%s` found in database %s/%d:%s' % (self.tableName, self.parentDb.ip, self.parentDb.port, self.parentDb.db))
			sql = 'select dbi.id, dbi.ip, dbi.port, dbi.update_time from db_info dbi, db_group_info g where %s and dbi.db="%s" and g.info_id=dbi.id order by dbi.update_time desc limit 1;' % ('(%s)' % (' OR '.join(s for s in ip_ports) if len(ip_ports)>0 else '1=1'), schema)
			db_rs = dbase.fetchall(sql)
			if dbase.isfull(db_rs):
				result.addMSG("* 未能在数据库%s中找到表%s，最后更新时间:%s，如果数据库元数据有误，请联系管理员" % (schema, self.tableName, db_rs[0][3]))
			else:
				result.addMSG("* 数据库%s的元数据暂未收集，不能进行sql解析，如有需要请联系管理员解锁新功能，谢谢：）" % schema)
			
			return None 

	def __repr__(self):
		return '%s alias %s' % (self.tableName, self.alias)

	def init_Columns(self, result):
		columns_sql = 'select col_name, col_type, first_seq, cardinality, discrim from table_columns where table_id=%d' % self.tableId
		#print(columns_sql)
		columns_rs = dbase.fetchall(columns_sql)
		if dbase.isfull(columns_rs):
			for c in columns_rs:
				#self.columns[c[0].upper()] = DBColumn(c[0].upper(), self, c[1], c[2], c[3])
				self.columns[c[0].upper()] = DBColumn(result, c[0].upper(), self)
	
	def checkForceIndex(self, result):
		for ind in self.forceIndex:
			if ind not in self.indexes:
				result.addMSG('[sql优化] 在表%s上使用了"force index(`%s`)"，却未在where中包含该索引中定义的列，导致sql无法走正确的索引。请去掉force index(`%s`)或者调整查询条件' % (self.tableName, ind, ind))

#数据列
class DBColumn:
	def __init__(self, result, columnName, parentTable, columnType=None, first_seq=None, cardinality=0):
		self.columnId = 0
		self.columnType = ''
		self.columnName = columnName
		self.parentTable = parentTable
		self.first_seq = None
		self.cardinality = 0
		#key为索引id，value为[索引名, seq_in_index, cordinality, index_type]
		self.columnIndexes = {}

		if columnType == None:
			co_rs = dbase.fetchone('select id, col_type from table_columns where table_id = %d and col_name = "%s"' % (parentTable.tableId, columnName))
			if dbase.isfull(co_rs):
				self.columnId = co_rs[0]
				self.columnType = co_rs[1][:co_rs[1].rfind('(')] if '(' in co_rs[1] else co_rs[1]

				min_sql = 'select col_id, min(seq_in_index), cardinality from cols_indexes where col_id=%d group by col_id' % self.columnId
				min_rs = dbase.fetchone(min_sql)
				if min_rs is not None and dbase.isfull(min_rs):
					self.first_seq = min_rs[1]
					self.cardinality = min_rs[2]

					update_sql = 'update table_columns set first_seq=%d, cardinality=%d, discrim=%d where id=%d' % (min_rs[1], min_rs[2], 0 if min_rs[2] == 0 else parentTable.rows_cnt/min_rs[2], self.columnId)
					#print(update_sql)
					dbase.save(update_sql)

					ind_sql = "select ti.id, ti.index_name, ci.seq_in_index, ci.cardinality, ti.index_type from table_indexes ti, cols_indexes ci where ti.table_id = %d and ti.id = ci.index_id and ci.col_id = %d;" % (parentTable.tableId, self.columnId)
					#print(ind_sql)
					ind_rs = dbase.fetchall(ind_sql)
					if dbase.isfull(ind_rs):
						for l in ind_rs:
							self.columnIndexes[l[0]] = [l[1], l[2], l[3], l[4]]
					self.judgeIndex(result)
		else:
			#print(columnName)
			self.columnType = columnType
			self.first_seq = first_seq
			self.cardinality = cardinality

	def __repr__(self):
		return "Column %s.%s: first_seq=%s, cardinality=%d" % (self.parentTable.tableName if self.parentTable is not None else '', self.columnName, str(self.first_seq) if self.first_seq is not None else 'None', self.cardinality)

	#判断列上的索引有效性
	#返回值：[索引最靠前的列序号, 基数，错误提示信息]
	def judgeIndex(self, result):
		ind_ids = []
		#print(self.columnId)
		for k in self.columnIndexes.keys():
			if self.columnIndexes[k][1] == 1:
				ind_ids.append(k)
			#print(self.columnName, k, self.columnIndexes[k])
		
		#该列为独立索引/联合索引第一列，但基数太低
		if len(ind_ids) == 1 and self.columnIndexes[ind_ids[0]][2] < CARDINALITY_LEAST:
			result.addMSG("[索引建议] 表%s的索引`%s`基数(第一列%s在表中不重复的个数)过低(%d), 索引效率过低" % (self.parentTable.tableName, self.columnIndexes[ind_ids[0]][0], self.columnName, self.columnIndexes[ind_ids[0]][2]))

		#单列索引 和 多列索引的第一列重合时，建议删除单列索引
		singles = []
		multis = []
		if len(ind_ids)>1:
			ind_sql = "select index_id, count(col_id) from cols_indexes where index_id in (%s) group by index_id;" % (','.join(str(id) for id in ind_ids))
			#print(ind_sql)
			ind_rs = dbase.fetchall(ind_sql)
			if dbase.isfull(ind_rs):
				for l in ind_rs:
					if l[1]>1:
						multis.append(l[0])
					else:
						singles.append(l[0])
		if len(multis) > 0 and len(singles) > 0:
			result.addMSG("[索引建议] 表%s的索引%s和%s中都以 %s 开头，有重复，可考虑去掉索引%s" % (self.parentTable.tableName, '、'.join('`%s`' % self.columnIndexes[id][0] for id in singles), '、'.join('`%s`' % self.columnIndexes[id][0] for id in multis), self.columnName, '、'.join('`%s`' % self.columnIndexes[id][0] for id in singles)))
						
def findTableByName(tname, result):
	for t in result.TABLES:
		if tname.upper() == t.tableName.upper() or tname.upper() == t.alias.upper():
			return t
	return None

#以下比较运算符，用于分割where条件的左右两侧
EQUAL_STR = [' IS NOT ', ' IS ', '!=', '<=>', '<>', '=']
IN_STR = [' NOT IN ', ' IN ']
RANGE_LEFT_STR = ['>=', '>']
RANGE_RIGHT_STR = ['<=', '<']
RANGE_BETWEEN_STR = [(' BETWEEN ', ' AND '), ]
LIKE_STR = [' NOT LIKE ', ' LLIKE ', ' LIKE ']
RE_STR = [' RLIKE ', ' NOT REGEXP ', ' REGEXP ']
FULLTEXT_STR = [(' MATCH ', ' AGAINST '), ]

#有歧义的字符单独处理/不在同一列表中才算有歧义，如=和!=不算歧义
#'||','&&'其实用不到
AMBIGUOUS_map = {'=':['!=', '>=', '<='], '!':['!=', ], '>':['->>', '>>', '>=', '<>'], '<':['<<-', '<<', '<=', '<>'], '|':['||', ], '&':['&&', ], ' IN ':[' NOT IN ',], ' IS ':[' IS NOT ', ]}

#运算符，可单独使用
OPRATORS_STR = ['%%', '&', '~', '|', '^', '->>', '>>', '->', '<<', '!', '+', '-', '*', '/', ',']
OPRATORS1_STR = [' MOD ', ' DIV ']

#逻辑运算符，连接两个WhereConUni/WhereCon
LOGICAL_STR = [' AND ', ' OR ', '&&', '||', ' XOR ']

#函数，需要加()使用，以' 函数名('来作为判断是否存在的依据
FUNCTIONS = ["IF", "IFNULL", "NULLIF", \
"ABS", "ACOS", "ASIN", "ATAN2", "ATAN", "CEIL", "CEILING", "CONV", "COS", "COT", "CRC32", "DEGREES", "EXP", "FLOOR", "LN", "LOG", "LOG10", "LOG2", "MOD", "PI", "POW", "POWER", "RADIANS", "RAND", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE", \
"ASCII", "BIN", "BIT_LENGTH", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CONCAT", "CONCAT_WS", "ELT", "EXPORT_SET", "FIELD", "FIND_IN_SET", "FORMAT", "FROM_BASE64", "HEX", "INSERT", "INSTR", "LCASE", "LEFT", "LENGTH", "LOAD_FILE", "LOCATE", "LOWER", "LPAD", "LTRIM", "MAKE_SET", "MID", "OCT", "OCTET_LENGTH", "ORD", "POSITION", "QUOTE", "REGEXP", \
"REPEAT", "REPLACE", "REVERSE", "RIGHT", "RPAD", "RTRIM", "SOUNDEX", "SPACE", "STRCMP", "SUBSTR", "SUBSTRING", "SUBSTRING_INDEX", "TO_BASE64", "TRIM", "UCASE", "UNHEX", "UPPER", "WEIGHT_STRING", \
"ADDDATE", "ADDTIME", "CONVERT_TZ", "CURDATE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURTIME", "DATE", "DATE_ADD", "DATE_FORMAT", "DATE_SUB", "DATEDIFF", "DAY", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "EXTRACT", "FROM_DAYS", "FROM_UNIXTIME", "GET_FORMAT", "HOUR", "LOCALTIME", "LOCALTIMESTAMP", "MAKEDATE", "MAKETIME", "MICROSECOND", "MINUTE", "MONTH", "MONTHNAME", "NOW", "PERIOD_ADD", "PERIOD_DIFF", "QUARTER", "SEC_TO_TIME", "SECOND", "STR_TO_DATE", "SUBDATE", "SUBTIME", "SYSDATE", "TIME", "TIME_FORMAT", "TIME_TO_SEC", "TIMEDIFF", "TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", "TO_DAYS", "TO_SECONDS", "UNIX_TIMESTAMP", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "WEEK", "WEEKDAY", "WEEKOFYEAR", "YEAR", "YEARWEEK"]

#where语句中的限制条件
#and连接的、同一列上的所有条件都放在condition中
#or连接的n个条件，仅当全部都是同一个列的条件才能放在condiitons中，否则会被舍弃，无法用于索引判断，但会提示

#被whereCon_AND/whereCon_OR连接的单元，即单个where条件，其计算结果通常是一个bool值

class WhereConUni():

	def __init__(self, condition, condition_op = ''):
		self.condition = condition
		self.condition_op = condition_op
		self.left_functions = []
		
		self.left = None
		self.right = None
		#第三个操作数，如 left beteen right and right1
		self.right1 = None

		#单个操作符
		for oplist in ['EQUAL', 'IN', 'RANGE_LEFT', 'RANGE_RIGHT', 'LIKE', 'RE']:
			for op in globals()["%s_STR" % oplist]:
				if op in self.condition.upper():
					#运算符位置（非歧义）					
					op_index = self.condition.upper().index(op)
					#有歧义单独处理
					if op in AMBIGUOUS_map.keys():
						temp = self.condition.upper()
						#找到所有歧义字符替换成x，有几个字符替换几个。保证总字符数不变
						for wd in AMBIGUOUS_map[op]:
							wd_t = ''
							for c in wd:
								wd_t+='x'
							temp = temp.replace(wd, wd_t)
						if op not in temp:
							continue
						#替换歧义后op字符依然存在，则取得该运算符位置
						op_index = temp.index(op)

					self.left = self.condition[:op_index].strip()
					self.condition_op = oplist.strip()
					self.right = self.condition[op_index+len(op):].strip()

		if ' BETWEEN ' in self.condition.upper() and ' AND ' in self.condition[self.conditions.upper().index(' BETWEEN ')+9:].upper():
			self.condition_op = 'RANGE_BETWEEN'
			self.left = self.condition[:self.condition.upper().index(' BETWEEN ')].strip()
			self.right = self.condition[self.condition.upper().index(' BETWEEN ')+9:self.condition.index(' AND ')].strip()
			self.right1 = self.condition[self.condition.upper().index(' AND ')+5:].strip()

		if ' MATCH ' in self.condition.upper() and ' AGAINST ' in self.condition[self.condition.upper().index(' MATCH ')+7:].upper():
			self.condition_op = 'FULLTEXT'
			#left含括号
			self.left = self.condition[self.condition.upper().index(' MATCH ')+7:self.condition.upper().index(' AGAINST ')].strip()
			self.right = self.condition[self.condition.upper().index(' AGAINST ')+9:].strip()
		
		if self.left != None:
			s = self.left.upper()
			for f in FUNCTIONS:
				if f in s and f not in self.left_functions:
					f_0 = s.index(f)
					if f_0>0 and s[f_0-1] == ' ' or f_0 == 0:
						s = s.replace(' ', '')
						if s[s.index(f)+len(f)] == '{':
							self.left_functions.append(f)
	
	def __repr__(self):
		return "Uni(%s):[%s/%s, %s, %s]" % (self.condition, self.left, ','.join(f for f in self.left_functions),self.condition_op, self.right if self.right1 == None else "%s, %s" % (self.right, self.right1))
		#return self.condition
	
	def size(self):
		return 1

#通过"表名.列名"获得表达式中的列名及其所属的表
#返回[[列名,表名], [操作符, ]]
def getTablesAndColumns(expression, result):
		if ' ' in expression and expression.strip().split(' ')[0].upper() == 'SELECT':
			return None
		columns = []
		#无需判断括号，并去掉``
		#expression.expression.replace('(', ' ').replace(')', ' ').replace('`', '').strip()
		expression.replace('`', '').strip()
		if '.' not  in expression:
			columns = getTableFromColumn(expression, result.TABLES)
			return columns
		while '.' in expression:
			tname = expression[:expression.index('.')].strip()
			
			for op in [' ','{'] + OPRATORS_STR + OPRATORS1_STR:
				if op in tname:
					tname = tname[tname.rfind(op)+len(op):].strip()

			cname = expression[expression.index('.')+1:].strip()
			for op in [' ','{'] + OPRATORS_STR + OPRATORS1_STR:
				if op in cname:
					cname = cname[:cname.index(op)].strip()
			
			columns.append([cname, tname])

			expression = expression[expression.index(cname)+len(cname):].strip()
		return [columns, []]

#没有表名时，通过列名找到表
#返回[[[列名,表名], ], [操作符，]]
def getTableFromColumn(expression, tables):
		columns = []
		
		#存储所有函数，操作符
		ops = []
		#存储当前函数、操作符，和parse过的字符
		op = ''
		keyword = ''

		#逐字符过滤，排除运算符、函数
		for c in expression:
			if c >= 'a' and c <= 'z' or c >= 'A' and c <= 'Z' or c >= '0' and c <= '9' or c in '$_':
				if op == '%' and c in ('snd'):
					#处理%s,%d
					#吐出最后的%
					if keyword[-1] == '%':
						keyword = keyword[:-1]
					op = ''
					continue
					
				elif len(op)>0 and op in OPRATORS_STR:
					ops.append(op)
					op = ''
				
				keyword += c

			elif c == '`':
				if keyword != '':
					columns.append(keyword)
			elif c in ' (':
				if len(keyword) > 0 and (keyword.upper() in OPRATORS1_STR or keyword.upper() in FUNCTIONS):
					ops.append(keyword)
					keyword = ''
				elif len(keyword) > 0:
					columns.append(keyword)
					keyword = ''
				elif len(op) > 0 and op in OPRATORS_STR:
					ops.append(op)
					op = ''
			elif c in OPRATORS_STR and len(keyword) > 0:
				columns.append(keyword)
				keyword = ''
				if op != '':
					op += c
				else:
					ops.append(c)
			elif c == '%':
				op = c
				keyword += c
			else:
				pass

		if len(keyword) > 0 and not (keyword.upper() in OPRATORS1_STR or keyword.upper() in FUNCTIONS):
			columns.append(keyword)

		result0 = []
		if len(columns)>0 and len(tables)>0:
			sql = "select c.col_name, i.table_name from table_info i, table_columns c where c.table_id = i.id and c.col_name in (%s) and i.table_name in (%s)" % (','.join('"%s"' % c for c in columns), ','.join('"%s"' % t.tableName for t in tables))
			ind_rs = dbase.fetchall(sql)
			if dbase.isfull(ind_rs):
				for l in ind_rs:
					result0.append([l[0], l[1]])

		return [result0, ops]
					
		
#where条件列表
class WhereCon():

	def __init__(self, conditionsList, conditionsType = None):
		#成员为whereConUni或嵌套WhereCon。由于只是做索引判断，并不看重顺序 
		self.conditionsList = conditionsList 
		#连接并列的多个条件的AND/OR
		self.conditionsType = conditionsType

	def addConditionStr(self, whereConStr):
		self.conditionsList.append(WhereConUni(whereConStr.strip()))
	
	def addCondition(self, whereCon_uni):
		self.conditionsList.append(whereCon_uni)
	
	def getColumns(self):
		columns = []
		return columns

	def setType(self, conditionsType):
		self.conditionsType = conditionsType

	def getType(self):
		return self.conditionsType

	def size(self):
		return len(self.conditionsList)

	def __repr__(self):
		return "%s(%s)" % (self.conditionsType, ', '.join(str(con) for con in self.conditionsList))

	#检查一个嵌套的list是否为空
	def is_Empty(self, li):
		if isinstance(li, list) and len(li) == 0:
			return True
		elif isinstance(li, list) and isinstance(li[0], list):
			return self.is_Empty(li[0])
		elif isinstance(li, str):
			return False

	#检查一个嵌套的list中的元素是否完全相同
	def is_Same(self, li):
		if isinstance(li, list) and len(li) > 1:
			samp = li[0]
			for c in li:
				if samp != c:
					return False
			return True
		elif isinstance(li, list) and len(li) == 1:
			return self.is_Same(li[0])

	def containsNull(self, li):
		if isinstance(li, WhereCon) and len(li.conditionsList) > 0:
			result = False
			for con in li.conditionsList:
				result =  result or self.containsNull(con)
			return result
		elif isinstance(li, WhereConUni):
			return True if 'NULL' in li.right.upper() else False
		else:
			return False

	def check(self, result, conType = None):
		leftTables = []
		rightTables = []
		ops = []

		#当前单元可以走的索引，元素为: [table, index_name, 列名, seq, cardinality, ASC|DESC]
		indexes = []
		for con in self.conditionsList:
			if isinstance(con, WhereCon):
				leftTables, rightTables, ops, inds = con.check(result)
				#print(con.conditionsType, len(leftTables), self.is_Empty(rightTables), ops)
				#判断是否or连接多个 列=数值，要求OR连接，左边为列，右边没有列，算式为EQUAL, 右边不为null
				
				if con.conditionsType == 'OR' and len(leftTables) > 1 and self.is_Empty(rightTables) and ops == ['EQUAL', ]:
					containsNull = False
					if self.is_Same(leftTables):
						if not self.containsNull(con):
							result.addMSG('[sql优化] "(%s)" 可修改为 "%s in (%s)" 以提高查询效率' % (' OR '.join(c.condition for c in con.conditionsList), con.conditionsList[0].left, ', '.join(str(c.right) for c in con.conditionsList)))
							#print(leftTables) #[[['del_flag', 'a']], []], [[['del_flag', 'a']], []]

				if (con.conditionsType == 'OR' and self.is_Same(leftTables) and len(inds)>0) or con.conditionsType == 'AND':
					#or连接的条件，要求每个单元走相同的索引，最终才能走这个索引

					'''
					iindex = None
					for ind in inds:
						if iindex == None:
							iindex = ind
						elif iindex != ind:
							iindex = None
							break
					if iindex != None:
						indexes.append(iindex)
				elif con.conditionsType == 'AND':
					'''
					#and连接的条件，每个索引都算上
					for ind in inds:
						if ind not in indexes:
							indexes.append(ind)
					
			elif isinstance(con, WhereConUni):
				leftt = getTablesAndColumns(con.left, result)[0] if con.left is not None else None
				rightt = getTablesAndColumns(con.right, result)[0] if con.right is not None else None
				
				if leftt is not None and not self.is_Empty(leftt):
					leftTables.append(getTablesAndColumns(con.left, result))
					#查询条件为 colName in (子查询)的情况
					
					if con.condition_op == 'IN' and con.right.strip()[0] == '{' and (con.right.strip()[-1] == '}' or (con.right.strip()[-1] == ';' and con.right[:-1].strip()[-1] == '}')) and con.right.strip().upper()[1:7] == 'SELECT':
						result.addMSG("[sql优化] in的范围是子查询，效率较低，应改为联表查询形式，或分步执行查询，先进行子查询，再将结果代入本查询")
						continue

					for lf in leftt:
						left_table = findTableByName(lf[1], result)
						
						checkColumnType(left_table, lf[0], con.right, con.condition, result)

						checkDatetime(result, 'left', left_table, lf[0], con.condition_op, con.right, con.right1)
						#print(con, lf, left_table, leftt[0][0].upper())
						
						if left_table is not None and leftt[0][0].upper() in left_table.columns.keys():
							leftc = left_table.columns[leftt[0][0].upper()]
							#左侧有索引但使用了函数
							if len(con.left_functions) > 0 and con.left_functions != ['IFNULL']:
								if leftc.first_seq in (0, 1): 
									result.addMSG("[sql优化] 列%s.%s上有索引，但只能单独放在算式左侧才能使用索引，请改变写法将%s去除" % (left_table.tableName, lf[0], '、'.join("%s()" % f for f in con.left_functions)))
							
							#左侧有索引
							#右侧没有列
							#右侧不是Null
							#满足以上条件的情况下，才计入可走索引，否则进入联表判断
							if leftc.first_seq is not None and rightt is not None and self.is_Empty(rightt) and con.right.upper() != 'NULL' and con.right.upper() != 'NOT NULL':
								sql = 'select ti.index_name, ci.seq_in_index, ci.cardinality, ci.orderby from table_indexes ti, cols_indexes ci where ci.col_id=%d and ti.table_id=%d and ci.index_id=ti.id order by ci.seq_in_index;' % (leftc.columnId, left_table.tableId)
								ind_rs = dbase.fetchall(sql)
								for l in ind_rs:
									#去除重复（利用is_Same检验左侧的列是否相同来判断or是否走同一索引）
									if [left_table, l[0], leftc.columnName, l[1], l[2], l[3]] not in indexes:
										indexes.append([left_table, l[0], leftc.columnName, l[1], l[2], l[3]])
							
							if (leftc.first_seq is None or leftc.first_seq > 1) and ('DATE' in con.left.upper() or 'TIME' in con.left.upper()) and self.is_Empty(rightt) and con.right.upper() != 'NULL' and con.right.upper() != 'NOT NULL':
								#时间字段加索引
								msg = "[sql优化] 列%s.%s上无索引，通常时间字段上可以提供较好的区分度，请考虑添加索引" % (left_table.tableName, lf[0])
								if len(con.left_functions) > 0 and con.left_functions != ['IFNULL']:
									msg += ", 但只能单独放在算式左侧才能使用索引，请改变写法将%s去除" % '、'.join("%s()" % f for f in con.left_functions)
								result.addMSG(msg)

				#rightt = getTablesAndColumns(con.right, result)[0] if con.right is not None else None
				if rightt is not None and not self.is_Empty(rightt):
					rightTables.append(getTablesAndColumns(con.right, result))
					#判断是否是两表连接的条件: 左右均只有一个表（可能有特殊情况）、左右表不同
					if not self.is_Empty(leftt) and len(leftt) == 1 and len(rightt) == 1 and len(leftt[0]) == 2 and len(rightt[0]) ==2 and rightt[0][1] is not None and rightt[0][1] != '' and leftt[0][1] is not None and leftt[0][1] != '' and leftt[0][1] != rightt[0][1]:
						left_table = findTableByName(leftt[0][1], result)
						right_table = findTableByName(rightt[0][1], result)
						if left_table is not None and right_table is not None and '%s.%s' % (left_table.tableName,leftt[0][0].upper()) in left_table.columns.keys() and '%s.%s' % (right_table.tableName,rightt[0][0].upper()) in right_table.columns.keys():
							leftc = left_table.columns['%s' % leftt[0][0].upper()]
							rightc = right_table.columns['%s' % rightt[0][0].upper()]
							if left_table.rows_cnt > TABLE_ROWCNT_NEED_IDX and leftc.first_seq not in (0, 1):
								result.addMSG("[sql优化] 表%s较大（约%d行），连接字段%s需要索引" % (left_table.tableName, left_table.rows_cnt, leftt[0][0]))
							if right_table.rows_cnt > TABLE_ROWCNT_NEED_IDX and rightc.first_seq not in (0, 1):
								result.addMSG("[sql优化] 表%s较大（约%d行），连接字段%s需要索引" % (right_table.tableName, right_table.rows_cnt, rightt[0][0]))
					else:
						#检查索引列是否位于右侧
						for col in rightt:
							t = findTableByName(col[1], result)
							checkColumnType(t, col[0], con.left, con.condition, result)
							checkDatetime(result, 'right', t, col[0], con.condition_op, con.left)
							if t is not None and col[0].upper() in t.columns.keys():
								c = t.columns[col[0].upper()]
								if c.first_seq is not None and c.cardinality >= CARDINALITY_LEAST:
									result.addMSG("[sql优化] 索引列%s位于条件%s右侧，将无法使用索引，请调整写法将其单独置于算式左侧" % (col[0], con.condition))
					
				if con.condition_op is not None and con.condition_op != '' and con.condition_op not in ops:
					ops.append(con.condition_op)
			else:
				print('Not reconginized condition %s', con)
		return(leftTables, rightTables, ops, indexes)

def checkColumnType(table, columnName, value, conditionStr, result):
	if table is not None and columnName.upper() in table.columns.keys():
		colType = table.columns[columnName.upper()].columnType
		if '(' in colType:
			colType = colType[:colType.index('(')]
		if colType in ('bigint', 'decimal', 'double', 'int', 'mediumint', 'smallint', 'tinyint') and (value == '%s' or '"' in value or "'" in value):
			result.addMSG("[sql优化] 条件%s中，列%s类型为%s，算式中的值为字符串，将引起强制转换，严重影响查询效率，请将两者改写一致" % (conditionStr, columnName, colType))
		elif colType in ('varchar', 'datetime', 'char', 'date', 'longblob', 'text', 'timestamp') and (value == '%n' or value.isdigit()):
			result.addMSG("[sql优化] 条件%s中，列%s类型为%s，算式中的值为数字，将引起强制转换，严重影响查询效率，请将两者改写一致" % (conditionStr, columnName, colType))

def checkDatetime(result, flag, table, col, op, value, value1 = None):
	if table is not None and ('TIME' in col.upper() or 'DATE' in col.upper()) and op in ('RANGE_LEFT', 'RANGE_RIGHT', 'RANGE_BETWEEN'):
		#print(flag, table, col, op, value)
		fullname = '%s.%s' % (table.tableName.upper(), col.upper())
		if fullname not in result.TIME_RANGES.keys():
			result.TIME_RANGES[fullname] = [None, None]
		if op == 'RANGE_LEFT' and flag == 'left' or op == 'RANGE_RIGHT' and flag == 'right':
			result.TIME_RANGES[fullname][0] = value
		elif op == 'RANGE_RIGHT' and flag == 'left' or op == 'RANGE_LEFT' and flag == 'right':
			result.TIME_RANGES[fullname][1] = value
		elif op == 'RANGE_BETWEEN':
			result.TIME_RANGES[fullname][0] = value
			result.TIME_RANGES[fullname][1] = value1

def parse_Where(whereStr):
	
	#刻舟求剑的舟
	s = whereStr.upper()
	#已经替换了的函数、in，换成***（包含{}），以免其中含有AND OR干扰语法分析
	while '{' in s and '}' in s and s.index('}') > s.index('{'):
		s = s[:s.index('{')] + '*'*(s.index('}')-s.index('{')+1) + s[s.index('}')+1:]
	
	#提取and or xor && || ( )
	mks =re.findall(r"\sAND\s|\sOR\s|\sXOR\s|\s&&\s|\s\|\|\s|\(|\)", s)

	#print(s)
	#print(mks, whereStr)

	#没有分隔符，直接返回
	if len(mks) == 0 and whereStr.strip() > '':
		return WhereConUni(whereStr)
	
	whereCons = WhereCon([])		

	#标记括号 [[在whereStr中的位置, 在标记列表mks中的位置], ]
	parenthese_inx = Stack()
	
	#标记上一个逻辑连接
	last_logic = Stack()

	last_con_and = Stack()

	last_con_or = Stack()

	s_ind_cnt = 0
	#刻舟求剑的剑
	i = 0

	last_pos = 0

	while i < len(mks):
		c = mks[i]
		#在s的位置
		c_ind = s.index(c)
		#上一个c的结束位置
		last_pos += len(mks[i-1]) if i > 0 else 0

		#在原whereCon字符串的位置
		s_ind_cnt = c_ind + last_pos

		if c == '(':
			parenthese_inx.push([s_ind_cnt, i])

		elif c == ')':
			if parenthese_inx.size() > 0:

				#"("出栈
				last_pos_p = parenthese_inx.pop()
				
				#判断是否为函数，如果是函数，whereStr中的()变为{}, last_pos指针退回到上一个c的起始，并从mks中抹去这对()
				lastStr = whereStr[:last_pos_p[0]]
				lastStrs = lastStr.split(' ')
				
				#()中的部分
				strwithin = whereStr[last_pos_p[0] + 1:s_ind_cnt].strip()

				while len(lastStrs) > 0 and lastStrs[-1] == '':
					lastStrs = lastStrs[:len(lastStrs) - 1]
				t = False
				while len(lastStrs)>0:
					lastword = lastStrs[-1]
					if lastword.upper() in FUNCTIONS or (lastword.upper() == 'IN'):
						whereStr = '%s{%s}%s' % (whereStr[:last_pos_p[0]], strwithin, whereStr[s_ind_cnt+len(c):])
						return parse_Where(whereStr)
						#elif lastword.upper() == 'IN':
						#pass
					elif lastword != '':
						break
						
				#括号中是空且(前不为空
				if strwithin=='' and last_pos_p[0]>0:
					ts = re.findall(r"\w+", whereStr[:last_pos_p[0]])[-1]
					if ts.upper() in FUNCTIONS:
						print("Find function %s" % ts)
					else:
						print("Unrecongnized '%s()'"% ts)
					i += 1
					continue
				
				else:
					#递归括号中间的部分 
					con = parse_Where(strwithin)
					if con != None:
						t = last_pos_p[1] - 1
						typed = False
						while t >= 0:
								if mks[t] == '(':
									break
								if mks[t] in (' OR ', ' || ', ' XOR '):
									last_con_or.push(con)
									typed = True
									break
								elif mks[t] in (' AND ', ' && '):
									last_con_and.push(con)
									typed = True
									break
								t -= 1

						if typed:
								i += 1
								last_pos = s_ind_cnt
								s = s[s.index(c) + len(c):]
								continue

						#(xxx)后面的and/or
						t = i + 1
						while t < len(mks):
								if mks[t] == ')':
									break;
								if mks[t] in (' OR ', ' || ', ' XOR '):
									last_con_or.push(con)
									last_logic.push('OR)')
									typed = True
									break
								elif mks[t] in (' AND ', ' && '):
									last_con_and.push(con)
									last_logic.push('AND')
									typed = True
									break
								t += 1
							
						if typed:
								i += 1
								last_pos = s_ind_cnt
								s = s[s.index(c) + len(c):]
								continue

						#(xxx)前后没有and和or，直接返回
						#()前后是运算符怎么办
						whereCons.addCondition(con)
						return whereCons
									
					else:
						print("Unrecongnized 1(%s)" % strwithin)
		
		elif c in (' AND ', ' && ') and parenthese_inx.is_empty():
			#为AND时直接进栈
			last_logic.push("AND")
			#print("in and: %s, %d, %d" % (whereStr[last_pos:s_ind_cnt].strip(), last_pos, s_ind_cnt))
			con = parse_Where(whereStr[last_pos:s_ind_cnt].strip()) if last_pos < s_ind_cnt else None
			if con != None:
				last_con_and.push(con)
			else:
				print("Unrecongnized 3(%s %d %d)" % (whereStr[last_pos:s_ind_cnt].strip(), last_pos, s_ind_cnt))

		elif c in (' OR ', ' || ', ' XOR ') and parenthese_inx.is_empty():
			#为OR时，last_con_and栈内的AND需要出栈组成list，进入last_con_or栈
			ands = WhereCon([], 'AND')
			t = False
			while (not last_logic.is_empty()) and last_logic.peek() == 'AND' and last_con_and.size() > 0:
				ands.addCondition(last_con_and.pop())
				t = True
				last_logic.pop()
			
			#最前一个and前面的条件
			if t and last_con_and.size() > 0:
				ands.addCondition(last_con_and.pop())

			last_logic.push('OR')
			if ands.size() > 0:
				con = parse_Where(whereStr[last_pos:s_ind_cnt].strip()) if last_pos < s_ind_cnt else None
				#print("in or for and: %s, %d, %d" % (whereStr[last_pos:s_ind_cnt].strip(), last_pos, s_ind_cnt))
				if con!= None:
					ands.addCondition(con)
				last_con_or.push(ands)
			else:
				#print("in or for all: %s, %d, %d" % (whereStr[last_pos:s_ind_cnt].strip(), last_pos, s_ind_cnt))
				con = parse_Where(whereStr[last_pos:s_ind_cnt].strip()) if last_pos < s_ind_cnt else None
				if con != None:
					last_con_or.push(con)
				else:
					print("Unrecongnized 2(%s)" % whereStr[last_pos:s_ind_cnt].strip())    
		
		#已经走到最后，将and/or后连接的where条件进栈
		if c in (' AND ', ' && ', ' OR ', ' || ', ' XOR ') and i == len(mks) - 1 and parenthese_inx.is_empty():
			end_ind = len(whereStr) if i == len(mks) - 1 else s.index(mks[i+1])
			if end_ind > s_ind_cnt + len(c) and  whereStr[s_ind_cnt + len(c):end_ind].strip() > '':
				#print("in end: %s, %d, %d" % (whereStr[s_ind_cnt + len(c):end_ind].strip(), s_ind_cnt + len(c), end_ind))
				con = parse_Where(whereStr[s_ind_cnt + len(c):end_ind].strip())
				if con != None:
					if c in (' AND ', ' && '):
						last_con_and.push(con)
					elif c in (' OR ', '||', ' XOR '):
						last_con_or.push(con)

		i += 1
		last_pos = s_ind_cnt
		s = s[s.index(c) + len(c):]

	if last_con_and.size() > 0 and last_con_or.size() == 0:
		#如果没有or，合并and
		while last_con_and.size()>0:
			con = last_con_and.pop()
			whereCons.addCondition(con)
			whereCons.setType('AND')
	elif last_con_and.size() > 0 and last_con_or.size() > 0:
		#有and也有or，合并and作为or中的一项
		and_con = WhereCon([], 'AND')
		while last_con_and.size()>0:
			con = last_con_and.pop()
			and_con.addCondition(con)
		last_con_or.push(and_con)
		while last_con_or.size()>0:
			con = last_con_or.pop()
			whereCons.addCondition(con)
		whereCons.setType('OR')
	elif last_con_and.size() == 0 and last_con_or.size() > 0:
		while last_con_or.size()>0:
			con = last_con_or.pop()
			whereCons.addCondition(con)
		whereCons.setType('OR')

	#print(whereCons)
	return whereCons

def get_Cached(sql, result):
	hit = False
	
	'''
	db_info = {}

	db_sql = '(select concat(ip, "_", port, "_", db), id, update_time, ip, port, db from db_info) union (select concat(gi.ip, "_", gi.port, "_", dbi.db), dbi.id, dbi.update_time, gi.ip, gi.port, dbi.db from db_info dbi, db_group_info gi where gi.info_id=dbi.id)'
	rs = dbase.fetchall(db_sql)
	for l in rs:
		db_info[l[0]] = [l[1], l[2], l[3], l[4], l[5]]
	'''

	md5 = dbh.genearteMD5(sql)
	db_sql = 'select pr.result_content, pr.scan_rows, pr.result_level from sql_cached sc, parse_result pr where sql_md5="%s" and sc.id=pr.sql_id and pr.is_latest=1' % md5
	print(db_sql)
	rs = dbase.fetchall(db_sql)
	if dbase.isfull(rs):
		msglist = rs[0][0].split('\n')
		for m in msglist:
			result.addMSG(m)
		result.rows = rs[0][1]
		hit = True

	return hit
	

def get_tables(schema, ips, sql, layer = 0):
	result = ParseResult()

	sql = sql.replace('\t', ' ').replace('\n', ' ').strip()
	ts = sqlparse.parse(sql)[0]

	#同一层的表，作为一个list放入堆栈
	current_tables = Stack()
	#层级计数。from 新建一层，join算同一层，子查询+1层
	#layer = -1

	#元素[layer, type(FROM/(LEFT )JOIN/IN), sql]
	inner_queries = []

	#判断是否为select count(1)/count(*) from (子查询) tmp_tbl
	#分段，未发现 0, 发现 1，from (子查询) 2，有where-1
	isAllCount = 0

	isTable = False
	tableType = None

	isJoin = False 

	isWhere = False

	lastToken = None

	#join xxTable on xxx
	ons = []

	whereStr = ''

	parsable = True

	for s in ts.tokens:
		sstr = ' '.join(filter(lambda x: x, str(s).split(' ')))
		#print("token: '%s'" % (sstr.upper()))

		if s.ttype != Whitespace and s.ttype != Keyword and isTable:
			#print('isTable:', sstr, isJoin)
			
			result.sqls_parsed[-1] += ' ' + sstr.strip()

			if '(' in sstr and ')' in sstr and 'SELECT' in sstr[sstr.index('(') + 1:sstr.rfind(')')].upper():
				isJoin = True
				if lastToken == 'FROM' and isAllCount>0:
					isAllCount += 1
				inner_queries.append([layer+1, lastToken.upper(), sstr[sstr.index('(') + 1:sstr.rfind(')')]])
			else:
				table = DBTable(schema, ips, s, tableType, result)
				if table.parentDb == None:
					parsable = False
				else:
					result.TABLES.append(table)

					if table.tableType == "FROM":
						#layer += 1
						table_list = [table, ]
						current_tables.push(table_list)
						#elif 'JOIN' in table.tableType and layer>=0:
					elif 'JOIN' in table.tableType and current_tables.size()>=1:
						tables_list = current_tables.peek()
						tables_list.append(table)

			isTable = False
			lastToken = ''

		elif sstr.upper() == 'SELECT':
			lastToken = 'SELECT'
			isWhere = False

			result.sqls_parsed.append('\t'*layer + sstr.strip())

		elif lastToken == 'SELECT' and s.ttype != Whitespace and s.ttype != Keyword:
			for s in sstr.strip().split(','):
				result.SELECTS.append(s)

			result.sqls_parsed[-1] += ' ' + sstr.strip()

		elif sstr.upper() == 'FORCE':
			lastToken = 'FORCE'
			isWhere = False

			result.sqls_parsed[-1] += ' ' + sstr.strip()

		elif sstr.upper()[:5] == 'INDEX' and lastToken == 'FORCE':
			isWhere = False
			isTable = False
			
			result.sqls_parsed[-1] += ' ' + sstr.strip()

			lastToken = 'FORCE_INDEX'
			if len(sstr) > 5:
				st = sstr[5:].strip()
				if st[0]=='(' and st[-1] == ')':
					st = st[1:len(st)-1]
					st = st.replace('`', '').replace(' ', '')
					result.TABLES[-1].forceIndex = st.split(',')
					lastToken = ''

		elif s.ttype != Whitespace and lastToken == 'FORCE_INDEX':
			st = sstr.strip()

			result.sqls_parsed[-1] += ' ' + st.strip()

			if st[0]=='(' and st[-1] == ')':
				st = st[1:len(st)-1].strip()
				st = st.replace('`', '').replace(' ', '')
				result.TABLES[-1].forceIndex = st.split(',')
			lastToken = ''
			isWhere = False
			isTable = False

		elif s.ttype != Whitespace and lastToken in ('ORDER_BY', 'GROUP_BY'):
			
			result.sqls_parsed[-1] += ' ' + sstr.strip()

			cos = sstr.strip().split(',')
			for co in cos:
				#co = co.strip()
				co = re.sub(r'\s+', ' ', co.strip())
				asc = 'ASC'
				if lastToken == 'ORDER_BY' and ' ' in co:
					asc = co[co.index(' ')+1:].upper().strip()
					co = co[:co.index(' ')].strip()

				#找到排序字段所在的表
				#目前不支持select字段别名
				a = getTablesAndColumns(co, result)
				if dbase.isfull(a):
					[cName, tName] = a[0][0]
					if tName is not None and tName != '':
						tb = findTableByName(tName, result)
					
					if tb is not None:
						if lastToken == 'ORDER_BY':
							tb.orderBy.append([cName, asc])
						elif lastToken == 'GROUP_BY':
							tb.groupBy.append(cName)
						
			lastToken = ''
			isWhere = False
			isTable = False
		
		elif sstr.upper() in ('FROM', 'JOIN', 'LEFT JOIN', 'LEFT JOIN', 'INNER JOIN'):
			isTable = True
			isWhere = False
			tableType = sstr.strip().upper()
			lastToken = tableType
			
			select_str = re.sub(r'\s+', ' ', result.sqls_parsed[-1].upper().strip())
			if tableType == 'FROM' and select_str in ('SELECT COUNT(%N)', 'SELECT COUNT(*)', 'SELECT COUNT(1)'):
				isAllCount = 1

			result.sqls_parsed.append('\t'*layer + sstr)

		elif sstr.upper()[0:5] in ('ORDER', 'GROUP'):
			lastToken = re.sub(r'\s+', '_', str(s).strip().upper())
			isWhere = False
			isTable = False

			result.sqls_parsed.append('\t'*layer + sstr)

		elif sstr.upper() == 'BY':
			if lastToken in ('ORDER', 'GROUP'):
				lastToken = lastToken + '_BY'
			#lastToken = '%s_%s' % (lastToken, str(s).strip().upper())
			result.sqls_parsed[-1] += ' ' + sstr.strip()

		elif sstr.upper() == 'HAVING':
			lastToken = 'HAVING'
			result.sqls_parsed.append('\t'*layer + sstr)

		elif lastToken == 'HAVING' and s.ttype != Whitespace and s.ttype != Keyword:
			result.sqls_parsed[-1] += ' ' + sstr.strip()


		elif sstr.upper() in ("ON", ):
			isWhere = False
			isTable = False
			lastToken = 'ON'

			result.sqls_parsed[-1] += ' ' + sstr.strip()

		elif str(s).strip().upper()[:6] == "WHERE ":
			isTable = False
			isWhere = True
			lastToken = "WHERE"
			whereStr = str(s).strip()[6:]

			result.sqls_parsed.append('\t'*layer + sstr)
			if isAllCount>0:
				isAllCount -= 1

		elif s.ttype != Whitespace and lastToken == 'ON' and str(s).strip().upper() != 'AND':
			ons.append(str(s).strip())
			result.sqls_parsed[-1] += ' ' + sstr
		
		elif str(s).strip().upper()[:5] == "LIMIT":
			isTable = False
			isWhere = True
			if lastToken == 'ORDER':
				result.sqls_parsed[-1] += ' ' + sstr.strip()
			else:
				result.sqls_parsed.append('\t'*layer + sstr)
			lastToken = "LIMIT"

		elif s.ttype != Whitespace and lastToken == "LIMIT":
			if result.sqls_parsed[-1][-1] == '%':
				result.sqls_parsed[-1] += sstr.strip()
			else:
				result.sqls_parsed[-1] += ' ' + sstr.strip()

		elif lastToken == "WHERE":
			result.sqls_parsed[-1] += ' ' + sstr.strip()

		elif s.ttype != Whitespace:
			result.sqls_parsed.append('\t'*layer + sstr)

	#for sql in result.sqls_parsed:
	#	print("- ",sql)

	'''
	if isJoin and isAllCount == 0:
		result.addMSG("* FROM/联表子查询的优化功能尚未支持，敬请期待！")
		#parsable = False
	'''

	if len(result.TABLES)>0:
		hit = get_Cached(sql, result)
		if hit:
			return result

	WHERES = None
	
	if len(result.SELECTS) == 1 and result.SELECTS[0].strip() == '*':
			result.addMSG('[sql优化] select * 造成不可避免的回表操作，消耗大量资源，如果不是必要请尽量避免')

	if isAllCount == 2:
		result.addMSG('[sql优化] select count(1) from (子查询) 会浪费大量数据库资源，建议去掉外层，直接在子查询中count')

	if parsable and whereStr > '':
		WHERES = parse_Where(whereStr)
		if isinstance(WHERES, WhereConUni):
			WHERES = WhereCon([WHERES, ], 'AND')

	if parsable and len(ons) > 0:
		onstr = ' AND '.join(on for on in ons)
		#print(onstr)
		result.JOINONS = parse_Where(onstr)
		#print(JOINONS)
		if WHERES is not None and WHERES.getType() == 'OR':
			WHERES = WhereCon([WHERES, ], 'AND')
		if isinstance(result.JOINONS, WhereCon):
			for con in result.JOINONS.conditionsList:
				WHERES.conditionsList.append(con)
		elif isinstance(result.JOINONS, WhereConUni):
			WHERES.conditionsList.append(result.JOINONS)

	print("查询条件: ", WHERES)
	inds = []
	if parsable and WHERES is not None and WHERES.size() > 0:
		leftTables, rightTables, ops, inds = WHERES.check(result)
		#print(leftTables, rightTables, ops, inds)
	
	if parsable and len(result.TABLES) > 0:
		for t in result.TABLES:
			#print(t.orderBy, t.groupBy)
			t.checkForceIndex(result)

	for t in result.TIME_RANGES.keys():
		if result.TIME_RANGES[t][1] != None:
			tbl = findTableByName(t[:t.index('.')], result)
			if tbl is not None and tbl.rows_cnt > TABLE_ROWCNT_NEED_IDX:
				result.addMSG('[sql优化] 表%s较大(%d行)，查询为过去时间段，可以考虑放在从库执行' % (tbl.tableName, tbl.rows_cnt))
	
	if parsable and len(inds) > 0:
		#inds元素, 包含所有where、order/group by中涉及的列 [dbTable, index_name, 列名, seq, cardinality, ASC|DESC]
		keyname = ''
		
		alter_colname = ''
		alter_index = ''

		mostcard = 1
		alter_card = 1
		
		alter_seq = None

		mostcard_rows = 0
		alter_rows = 0

		allkeys = {}
		possiblekeys = []
		flag = False
		
		#判断是否用到一个索引，先按"表名|索引名"归类，如果一个索引中的列按01234排列则可以用到索引，如果不以0/1开始则用不到
		#包含了二级索引
		for ind in inds:
			indexS = "`%s`.`%s`" % (ind[0].tableName, ind[1])
			if indexS not in allkeys:
				allkeys[indexS] = []
				allkeys[indexS].append(ind)
			else:
				#按seq顺序放置
				i = len(allkeys[indexS])
				allkeys[indexS].append([])
				while i >= 1:
					if ind[3] <= allkeys[indexS][i-1][3]:
						allkeys[indexS][i] = allkeys[indexS][i-1]
					else:
						allkeys[indexS][i] = ind
						flag = True
						break
					i -= 1
			if not flag:
				allkeys[indexS][0] = ind

			#if ind[3] in (0, 1):
			#	possiblekeys.append(ind)
				#改为最后遍历allkeys判断（以便综合判断联合索引）
				'''
				if ind[4]>mostcard:
					mostcard = ind[4]
					mostcard_rows = ind[0].rows_cnt/ind[4]
					keyname = "`%s`.`%s`" % (ind[0].tableName, ind[1])
				'''
		for kname in allkeys.keys():
			print(allkeys[kname])
			card = 1
			if allkeys[kname][0][3] not in (0,1):
				ind = allkeys[kname][0]
				if ind[4]>alter_card:
					alter_card = ind[4]
					alter_seq = ind[3]
					alter_rows = ind[0].rows_cnt/ind[4]
					alter_colname = "`%s`.`%s`" % (ind[0].tableName, ind[2])
					alter_index = "`%s`" % ind[1]
				continue
			

			i = 1
			mostcard1 = allkeys[kname][0][4]
			while i < len(allkeys[kname]):
				if allkeys[kname][i][3]>allkeys[kname][i][3]+1:
					break
				mostcard1 *= allkeys[kname][i][4]
				i += 1

			if [kname, mostcard1, allkeys[kname][0][0].rows_cnt/mostcard1] not in possiblekeys:
				possiblekeys.append([kname, mostcard1, allkeys[kname][0][0].rows_cnt/mostcard1])

			if mostcard1 > mostcard:
				mostcard = mostcard1
				keyname = kname
		
		
		mostcard_rows = allkeys[keyname][0][0].rows_cnt/mostcard

		if len(possiblekeys) > 0 and len(possiblekeys)>1:
			result.addMSG('[执行计划] 该查询可能走的索引: %s' % ', '.join('%s(基数%d, 约扫描%d行)' % (ind[0], ind[1], ind[2]) for ind in possiblekeys))
		if keyname > '':
			msg = '[执行计划] 该查询最可能走的索引: %s(基数%d, 约扫描%d行)' % (keyname, mostcard, mostcard_rows)
			if mostcard < CARDINALITY_LEAST:
				msg += '，该索引基数过低，请根据业务需求和数据量大小进行调整'

				# 第二列以后的基数都是跟着第一列的，这里无法这样判断
				#if alter_seq is not None and alter_card > mostcard and alter_card > CARDINALITY_LEAST:
				#	msg += '; 列%s具有较高的基数%d，但位于联合索引%s的第%d列, 查询无法使用上面的索引，请考察联合索引顺序是否合理' % (alter_colname, alter_card, alter_index, alter_seq)
			result.addMSG(msg)
			result.rows = mostcard_rows

	elif isAllCount == 0 and parsable and (WHERES is None or WHERES.size == 0):
		result.addMSG('[sql优化] 该查询上无任何查询条件，跑库操作务必在业务低峰期进行')

	elif isAllCount == 0 and parsable and len(result.TABLES)>0:
		result.addMSG('[执行计划] 该查询上无任何可用索引，请根据业务需求和数据量大小建合适索引或增加合理限制条件；统计需求建议放在从库查询。')
		
	if isJoin and isAllCount == 0:
		result.addMSG("* FROM/联表子查询的优化功能尚未支持，敬请期待！")
		
	if len(result.TABLES)>0:
		result.addMSG('* 以上结果为估算，可能与mysql优化器实际结果不一致，仅供参考')
		result.setValues(result.TABLES[0].parentDb, result.MSGS)
	else:
		result.setValues(None, result.MSGS)
	
	return result
	
def getQueryInfo(schema, ips, sql):
	tables = get_tables(schema, ips, sql, 0)
	if len(tables) == 1:
		return tables


if __name__ == '__main__':
	
	#处理参数
	argTag = ''
	argValue = None
	for i in range(1, len(sys.argv)):
		if sys.argv[i][0] == '-':
			arg = sys.argv[i][1:].strip()
			if arg[0] == '-':
				arg = arg[1:]
			argTag = arg[0]
			if argTag not in sql_info.keys():
				raise RuntimeError("Unrecongenized argument '%s'" % argTag)
				exit()
			if len(arg) == 1 or arg[1:] == "":
				continue
			else:
				argValue = arg[1:]
		elif argTag != '':
			argValue = sys.argv[i]

		sql_info[argTag] = argValue
		argTag = ''

	#检查服务器信息
	#if sql_info['h']>"" and sql_info['P']>"" and sql_info['P'].isdigit() and sql_info['D']>"" and sql_info['e']>"":
	if sql_info['h']>"" and sql_info['D']>"" and sql_info['e']>"":
		#格式 {'ip:端口','ip:端口'}
		ips = []
		if ',' in sql_info['h']:
			ip_str = sql_info['h']
			if '{' in ip_str and '}' in ip_str:
				ip_str = ip_str[ip_str.index('{')+1:ip_str.index('}')].replace("'", "")
				ips = ip_str.split(',')
			else:
				ips.append(ip_str.replace("'", ""))
		schema = sql_info['D'].replace("'", "")
		#db = DBInfo(ip=sql_info['h'], port=int(sql_info['P']), db=sql_info['D'])
		sql = sql_info['e']
		#print(schema, ips)
		get_tables(schema, ips, sql, 0)
	else:
		raise RuntimeError("Sql info is incompleted")
