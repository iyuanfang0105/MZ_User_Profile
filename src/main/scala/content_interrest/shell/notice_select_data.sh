#!/bin/bash
dt=$1
srcTable="uxip.dwd_app_action_detail"
dstTable="algo.dxp_notice_ori_data"

cat <<EOF >/tmp/.notice.hql
create table if not exists ${dstTable}(
	imei string,
	fid string,
	event_name string,
	category string,
	pk_name string,
	ftitle string,
	fcontent string
	)
partitioned by (stat_date bigint)
stored as rcfile;

insert overwrite table ${dstTable}
partition(stat_date=${dt})
select imei,
misc_map [ 'key' ] fid,
event_name,
misc_map [ 'category' ] category,
misc_map [ 'PackageName' ] PackageName,
misc_map [ 'title' ] title,
misc_map [ 'content' ] content
from ${srcTable}
where stat_date = ${dt}
and pkg_name = 'com.android.systemui'
and event_name in ('notifiction_receive' --接收
,
'notifiction_icon_intent_click') --点击
and misc_map [ 'category' ] is not null 
and misc_map [ 'category' ] in ("news","recommend","subscribe");
select count(1) from ${dstTable} where stat_date=${dt};
EOF
hive  -f /tmp/.notice.hql
