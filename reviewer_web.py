#!/usr/bin/python
# -- coding: utf-8 --

import web
import sql_parser

web.config.debug = False

urls = (
	'/parse/?', 'sqlreview'
)

class sqlreview:
	def GET(self):
		input_data = web.input()
		#return "schema:%s\nip:%s\nsql:%s\n" % (input_data.schema, input_data.ip, input_data.sql)
		if input_data.schema>"" and input_data.ip>"" and input_data.sql>"":
			#格式 {'ip:端口','ip:端口'}
			ips = []
			ip_str = input_data.ip.replace("'", "")
			if ',' in ip_str and '{' in ip_str and '}' in ip_str:
				ip_str = ip_str[ip_str.index('{')+1:ip_str.index('}')].replace("'", "")
			ips = ip_str.split(',')
		else:
			ips.append(ip_str.replace("'", ""))
		#db = DBInfo(ip=sql_info['h'], port=int(sql_info['P']), db=sql_info['D'])
		print(input_data.schema, ips)
		result = sql_parser.get_tables(input_data.schema.replace("'", ''), ips, input_data.sql)
		
		#msg = '<br>'.join(msg for msg in msgs)
		render = web.template.render('templates/')

		return render.index(result)

if __name__ == "__main__":
	app = web.application(urls, globals())
	app.run()
