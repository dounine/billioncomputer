create table common_report
(
	id bigint auto_increment primary key,
	aid bigint not null,
	pv int default 0 null,
	uv int default 0 null,
	time date not null,
	constraint common_report_aid_time_uindex unique (aid, time)
);
create table offset
(
	id int auto_increment primary key,
	offsetName varchar(200) null,
	time datetime not null,
	constraint offset_offsetName_uindex unique (offsetName)
);



