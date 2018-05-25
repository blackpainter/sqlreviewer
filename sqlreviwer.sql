create database sqlreviewer;

use sqlreviewer;

CREATE TABLE `db_info` (
  `id` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'id, 主键',
  `ip` varchar(20) NOT NULL DEFAULT '0.0.0.0' COMMENT '数据库ip',
  `port` int(10) NOT NULL DEFAULT '3306' COMMENT '数据库端口号',
  `db` varchar(50) NOT NULL DEFAULT '' COMMENT '数据库名',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_ip_port_db` (`ip`,`port`,`db`),
  KEY `idx_db` (`db`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='数据库信息';

CREATE TABLE `table_info` (
  `id` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'id, 主键',
  `db_id` bigint(32) NOT NULL DEFAULT '0' COMMENT '所属数据库',
  `table_name` varchar(100) NOT NULL DEFAULT '' COMMENT '表名',
  `rows_cnt` bigint(32) NOT NULL DEFAULT '0' COMMENT '表行数',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
  `create_str` text COMMENT '建表语句',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_db_table` (`db_id`,`table_name`),
  KEY `idx_table` (`table_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='数据表信息';

CREATE TABLE `table_columns` (  `id` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'id, 主键',
  `table_id` bigint(32) NOT NULL DEFAULT '0' COMMENT '所属数据表',
  `col_name` varchar(100) NOT NULL DEFAULT '' COMMENT '列名',
  `col_type` varchar(30) NOT NULL DEFAULT '' COMMENT '列的数据类型定义',
  `first_seq` tinyint(5) DEFAULT NULL COMMENT '该列所在索引中最靠前的seq，无索引null，独立索引为0，联合索引1/2/3',
  `cardinality` bigint(32) NOT NULL DEFAULT '0' COMMENT '列的基数',
  `discrim` bigint(32) NOT NULL DEFAULT '0' COMMENT '区分度discriminabiltity = cardinality/表的总行数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_table_col` (`table_id`,`col_name`),
  KEY `idx_colname_seq` (`col_name`,`first_seq`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='数据库表中的列';

CREATE TABLE `table_indexes` (  `id` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'id, 主键',
  `table_id` bigint(32) NOT NULL DEFAULT '0' COMMENT '所属数据表',
  `index_name` varchar(100) NOT NULL DEFAULT '' COMMENT '索引名',
  `index_type` tinyint(3) NOT NULL DEFAULT '2' COMMENT '索引类型, 0:PRIMARY, 1:UNIQUE, 2:KEY',
  `read_performance` bigint(32) NOT NULL DEFAULT '0' COMMENT '平均读时间',
  `write_performance` bigint(32) NOT NULL DEFAULT '0' COMMENT '平均写时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_table_idx` (`table_id`,`index_name`),
  KEY `idx_idxname` (`index_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='数据库表中的索引';

CREATE TABLE `cols_indexes` (
  `id` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'id, 主键',
  `col_id` bigint(32) NOT NULL DEFAULT '0' COMMENT '列',
  `index_id` bigint(32) NOT NULL DEFAULT '0' COMMENT '所属索引',
  `seq_in_index` tinyint(5) DEFAULT NULL COMMENT '该列所在索引中最靠前的seq，独立索引为0，联合索引1/2/3',
  `cardinality` bigint(32) NOT NULL DEFAULT '0' COMMENT '列的基数',
  `orderby` enum('asc','desc') DEFAULT 'asc',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_index_col` (`index_id`,`col_id`,`seq_in_index`),
  KEY `idx_col` (`col_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='索引中的列';