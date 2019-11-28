set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000
set hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table dm_release.dm_exposure_sources partition(bdp_day='${bdp_day}')
select 
ex.sources,
ex.channels,
ex.device_type,
count(distinct(ex.device_num)) exposure_count,
(count(distinct(ex.device_num)) / cu.user_count)  exposure_rates
from
dw_release.dw_release_exposure ex
join dm_release.dm_customer_sources cu
on cu.bdp_day='${bdp_day}'
and cu.sources = ex.sources
and cu.channels = ex.channels
and cu.device_type = ex.device_type
where ex.bdp_day='${bdp_day}'
group by
ex.sources,
ex.channels,
ex.device_type,
cu.user_count
;