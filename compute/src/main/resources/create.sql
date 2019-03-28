-- offset 索引表
create table offset
(
	id int auto_increment primary key,
	offsetName varchar(200) null,
	time datetime not null,
	constraint offset_offsetName_uindex unique (offsetName)
);

-- pv/uv
create table common_report
(
	id bigint auto_increment primary key,
	aid bigint not null,
	pv int default 0 null,
	uv int default 0 null,
	time date not null,
	constraint common_report_aid_time_uindex unique (aid, time)
);

-- 留存
create table ttable.come_report
(
	id int auto_increment primary key,
	time datetime not null,
	aid int not null,
	come3 int default 0 null,
	come5 int default 0 null,
	come7 int default 0 null,
	constraint come_report_aid_time_uindex unique (aid, time)
);





