#!/usr/bin/python
# -- coding: utf-8 --

import web
import sql_parser
import datetime

web.config.debug = False

urls = (
	'/parse/?', 'sqlreview'
)

class sqlreview:
	def GET(self):
		input_data = web.input()
		#return "schema:%s\nip:%s\nsql:%s\n" % (input_data.schema, input_data.ip, input_data.sql)
		ips = []
		if input_data.schema>"" and input_data.ip>"" and input_data.sql>"":
			#格式 {'ip:端口','ip:端口'}
			ip_str = input_data.ip.replace("'", "")
			if ',' in ip_str and '{' in ip_str and '}' in ip_str:
				ip_str = ip_str[ip_str.index('{')+1:ip_str.index('}')].replace("'", "")
			ips = ip_str.split(',')
		else:
			ips.append(ip_str.replace("'", ""))
		#db = DBInfo(ip=sql_info['h'], port=int(sql_info['P']), db=sql_info['D'])
		print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		print(input_data.schema, ips)
		result = sql_parser.get_tables(input_data.schema.replace("'", ''), ips, input_data.sql, source = 0)
		
		#msg = '<br>'.join(msg for msg in msgs)
		render = web.template.render('templates/')

		return render.index(result)

#if __name__ == "__main__":
app = web.application(urls, globals())
application = app.wsgifunc()
#app.run()
