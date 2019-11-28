set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000
set hive.exec.max.dynamic.partitions.pernode=100000;

with release_exposure as (
select
    se.release_session,
    se.release_status,
    se.device_num,
    se.device_type,
    se.sources,
    se.channels,
    cu.idcard,
    cu.gender,
    cu.area_code,
    cu.longitude,
    cu.latitude,
    cu.matter_id,
    cu.model_code,
    cu.model_version,
    cu.aid,
    se.ct,
    se.bdp_day
from ods_release.ods_01_release_session se
left join dw_release.dw_release_customer cu
on cu.release_session = se.release_session
and
    cu.bdp_day = '${bdp_day}'
where
    se.bdp_day = '${bdp_day}'
and
    se.release_status ='${release_status}'
 )

 insert into table dw_release.dw_release_exposure partition(bdp_day)
 select
    release_session,
    release_status,
    device_num,
    device_type,
    sources,
    channels,
    idcard,
    age,
    gender,
    area_code,
    longitude,
    latitude,
    matter_id,
    model_code,
    model_version,
    aid,
    ct,
    bdp_day
 from release_exposure