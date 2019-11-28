--存放注册主题中从业务数据中获取的注册数据
create external table if not exists dw_release.dw_release_register_data(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
  device_num string comment '设备唯一编码',
  device_type string comment '1 android| 2 ios | 9 其他',
  sources string comment '渠道',
  channels string comment '通道',
  exts string comment '扩展信息',
  ct bigint comment '创建时间'
) partitioned by (bdp_day string)
stored as parquet
location '/data/release/dw/release_register_data/
;

-- 将数据抽取到注册数据表
insert into table dw_release.dw_release_register_data partition(bdp_day='20190614')
select
release_session,
release_status,
device_num,
device_type,
sources,
channels,
exts,
ct
from ods_release.ods_01_release_session
where
bdp_day = '20190614'
and
release_status = '06'

-- 创建注册主题用户表
create table if not exists dw_release.dw_release_register_users (
user_id string COMMENT '用户id：手机号',
release_session string comment '投放会话id',
release_status string comment '参考下面投放流程状态说明',
device_num string comment '设备唯一编码',
device_type string comment '1 android| 2 ios | 9 其他',
sources string COMMENT '渠道',
channels string COMMENT '通道',
ctime bigint COMMENT '创建时间'
)
partitioned by (bdp_day string)
stored as parquet
location '/data/release/dw/release_register_user/'

-- 将注册数据表中的数据插入到注册主题表
insert into table dw_release.dw_release_register_users partition(bdp_day='20190614')
select 
get_json_object(exts, '$.user_register') as user_id,
release_session,
release_status,
device_num,
device_type,
sources,
channels,
ct as ctime
from 
dw_release.dw_release_register_data
where
bdp_day = '20190614'
and
release_status = '06'
