#!/usr/bin/python3
# -- coding: utf-8 --
#coding = utf8

import datetime, time
import os, sys
import dbase
import DBHelper as dbh

LOG_PATH = '/apps/logdata/system/mysql_backup'

indexTypes = {"PRIMARY":0, "UNIQUE":1, "KEY":2, "FULLTEXT":3}


#LOG_FILE log文件目录
#mtimeStr log文件修改时间(用来与数据库中的时间比较)
def updateDDL(LOG_FILE, mtimeStr):

	if not os.path.exists(LOG_FILE):
		print("No log received.")
	
	with open(LOG_FILE, 'rb') as logfile:
		text = logfile.read()
	
	for lin in text.split(b'\n'):
		try:
			#line = unicode(line, 'utf8')
			line = lin.decode('utf8')
		except UnicodeDecodeError as e:
			try:
				line = lin.decode('gbk')
			except UnicodeDecodeError as e:
				line = str(lin).replace("\\", "")
				print("code err : ", line)
				pass

		vals_table = {"db_ip":"", "db_port":3306, "db_name":"", "tablename":"", "table_rows_cnt":0, "ddl_update_time":""}
		vals_imp = {"create_table":"", "cols":"", "indexes":""}

		for val in vals_table.keys():
			val_tag = '%s = ' % val
			if val_tag in line:
				line1 = line[line.index(val_tag) + len(val_tag):]
				valueStr = line1[:line1.replace('|', ' ').replace(',', ' ').index(' ')].strip()
				if valueStr.isdigit():
					vals_table[val] = int(valueStr)
				elif (not valueStr.isdigit()) and val == 'table_rows_cnt':
					print("[ERROR] Invalide value %s for row_cnt" % valueStr)
					
				elif valueStr is not None and valueStr != 'NULL':
					vals_table[val] = valueStr

		for val in vals_imp.keys():
			val_tag = '%s = """' % val
			if val_tag in line:
				line1 = line[line.index(val_tag) + len(val_tag):]
				valueStr = line1[:line1.index('"""')].strip().replace("\\n", "").replace("\\", "").replace("\"", "")
				vals_imp[val] = valueStr
	
		#conn = mdb.Connect(host=MDB_IP, port=MDB_PORT, user=MDB_USER, passwd=MDB_PWD, db=MDB_DB, charset='utf8')
		#c = conn.cursor()
	
		if vals_table["db_ip"] > "":
			sql = 'insert ignore db_info (ip, port, db) values ("%s", %d, "%s")' % (vals_table["db_ip"], vals_table["db_port"], vals_table["db_name"])
			dbase.save(sql)
			
			sql = "select date(ti.update_time) from table_info ti, db_info dbi where ti.db_id=dbi.id and dbi.ip = '%s' and dbi.port = %d and dbi.db = '%s' and ti.table_name='%s';" % (vals_table["db_ip"], vals_table["db_port"], vals_table["db_name"], vals_table["tablename"])
			rs = dbase.fetchall(sql)
			if dbase.isfull(rs):
				print(rs[0][0],mtimeStr)
				if str(rs[0][0])>mtimeStr:
					print("Table %s (in %s:%d/%s) is of newer version(%s>%s) in db" % (vals_table["tablename"],vals_table["db_ip"], vals_table["db_port"], vals_table["db_name"], rs[0][0], mtimeStr))
					continue
			
			#表有更新，先将旧数据删除(但原db_info中的信息保留)
			sql = 'delete ci, c, i, t \
				from table_info t \
				left join db_info dbi on dbi.id=t.db_id \
				left join table_columns c on c.table_id=t.id \
				left join table_indexes i on i.table_id=t.id \
				left join cols_indexes ci on ci.col_id=c.id \
				where t.table_name="%s" and dbi.ip="%s" and dbi.port=%d and dbi.db = "%s";' % (vals_table["tablename"], vals_table["db_ip"], vals_table["db_port"], vals_table["db_name"])
			dbase.save(sql)

			sql = 'select id from db_info where ip="%s" and port=%d and db="%s"' % (vals_table["db_ip"], vals_table["db_port"], vals_table["db_name"])
			rs = dbase.fetchall(sql)
			if rs != None and len(rs)>0:
				db_id = rs[0][0]
		
				sql = 'replace into table_info (db_id, table_name, rows_cnt, create_str) values (%d, "%s", %d, "%s")' % (db_id, vals_table["tablename"], vals_table["table_rows_cnt"], vals_imp["create_table"])
				dbase.save(sql)
				sql = 'select id from table_info where db_id = %d and table_name = "%s"' % (db_id, vals_table["tablename"])
				dbase.save(sql)
				
				if rs != None and len(rs)>0:
					table_id = rs[0][0]

					if vals_imp["cols"] > "":
						cols = vals_imp["cols"][1:-1].strip()
						while cols > "":
							col_name = cols[:cols.index(":")].replace("'", "").strip()
							cols = cols[cols.index(":") + 1:].strip()
						
							col_type = cols[1:cols.index(",")].replace("'", "").strip()
							cols = cols[cols.index(",") + 1:].strip()
						
							min_seq = None
							min_card = 0
							asc = 'ASC'
							discrim = 0
						
							indexesStr = cols[cols.index("{") + 1:cols.index("}")]
							cols = cols[cols.index("}]") + 3:].strip()
						
							if indexesStr > "":
								for ind in indexesStr.split('],'):
									if '[' in ind:
										ind = ind[ind.index('[')+1:]
									ss = ind.split(',')
									if len(ss) >= 1 and ss[0].strip().isdigit():
										seq = int(ss[0].strip())
									else:
										continue
									if len(ss) >= 2 and ss[1].strip().isdigit():
										card = int(ss[1].strip())
									else:
										continue
									if len(ss) >=3 and ss[2].strip().upper() in ('ASC', 'DESC'):
										asc = ss[2].strip().upper()
									else:
										continue

									if min_seq != None and min_seq < seq:
										continue 
									min_seq = seq 
									min_card = card

							sql = 'replace into table_columns (table_id, col_name, col_type, first_seq, cardinality, discrim) values (%d, "%s", "%s", %s, %d, %d)' % (table_id, col_name, col_type, "Null" if min_seq == None else str(min_seq), min_card, 0 if min_card == 0 else vals_table["table_rows_cnt"]/mincard)
							dbase.save(sql)

							#colsMap[col_name] = 0
					
						if len(vals_imp["indexes"]) > 2:
							indexes = vals_imp["indexes"][1:-1].strip()
							for indexS in indexes.split('],'):
								index_name = indexS[:indexS.index(":")].replace("'", "").strip()
								index_type = 2
								index_typeStr = indexS[indexS.rfind(",") + 1:].replace("'", "").strip()
								if index_typeStr in indexTypes.keys():
									index_type = indexTypes[index_typeStr]

								index_cols = indexS[indexS.index("{") + 1:indexS.index("}")]
							
								sql = 'replace into table_indexes (table_id, index_name, index_type) values (%d, "%s", %d)' % (table_id, index_name, index_type)
								dbase.save(sql)

								sql = 'select id from table_indexes where table_id=%d and index_name = "%s"' % (table_id, index_name)
								ind_rs = dbase.fetchall(sql)
								
								index_id = ind_rs[0][0]
								cols = {}
								#print(vals_table["tablename"]+ " : " +index_cols)
								for ind_c in index_cols.split('),'):
									ind_c = ind_c.replace(')', '').strip()
									#print(ind_c)
									cname = ind_c[:ind_c.index(':')].replace("'", "").strip()
									#seq, card
									cols[cname] = [0, 0, 'ASC']
									if '(' in ind_c:
										ind_c = ind_c[ind_c.index('(')+1:]
									ss = ind_c.split(',')
									if len(ss) >= 1:
										seq = ss[0].strip()
										if seq != None and seq.isdigit():
											 cols[cname][0] = int(seq)
									if len(ss) >= 2:
										card = ss[1].strip()
										if card != None and card.isdigit():
											cols[cname][1] = int(card)
									if len(ss) >= 3:
										asc = ss[2].strip()
										if asc != None and asc.upper() in ('ASC', 'DESC'):
											cols[cname][2] = asc.upper()
									
									#print(cols[cname])
								
								sql = "select col_name, id from table_columns where table_id=%d and col_name in (%s)" % (table_id, ','.join('"%s"' % cname for cname in cols.keys()))
								cn_rs = dbase.fetchall(sql)
								
								if cn_rs and len(cn_rs)>0:
									for l in cn_rs:
										if l[0] in cols.keys():
											sql = "replace into cols_indexes (col_id, index_id, seq_in_index, cardinality, orderby) values (%d, %d, %d, %d, '%s')" % (l[1], index_id, cols[l[0]][0], cols[l[0]][1], cols[l[0]][2])
											dbase.save(sql)
									
									col_ids = ','.join(str(l[1]) for l in cn_rs)
									sql = 'select col_id, min(seq_in_index), cardinality from cols_indexes where col_id in (%s) group by col_id' % col_ids
									col_ind_rs = dbase.fetchall(sql)
									
									if col_ind_rs and len(col_ind_rs) > 0:
										for ll in col_ind_rs:
											sql = 'update table_columns set first_seq=%d, cardinality=%d, discrim=%d where id=%d' % (ll[1], ll[2], 0 if ll[2] == 0 else vals_table["table_rows_cnt"]/ll[2], ll[0])
											dbase.save(sql)


 
	logfile.close()

for dir in os.listdir(LOG_PATH):
	print(dir)
	if os.path.isdir("%s/%s" % (LOG_PATH, dir)) and dbh.isIpadd(dir):
		LOG_FILE = '%s/%s/dbinfo.log' % (LOG_PATH, dir)
		if os.path.exists(LOG_FILE):
			print(LOG_FILE)
			mtimeStr = time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(LOG_FILE)))
			updateDDL(LOG_FILE, mtimeStr)
			
			res = os.system("mv %s/%s/dbinfo.log %s/%s/dbinfo_%s.log" % (LOG_PATH, dir, LOG_PATH, dir, mtimeStr.replace('-', '')))
			if res != 0:
				print("mv error %d" % res)
