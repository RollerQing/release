set spark.sql.shuffle.partitions=${partitions};
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000
set hive.exec.max.dynamic.partitions.pernode=100000;


from (
select
sources,
channels,
device_type,
gender,
area_code,
(case when age<18 then '1'
when age between 18 and 25 then '2'
when age between 26 and 35 then '3'
when age between 36 and 45 then '4'
else '5'
end) age_range,
device_num
from
dw_release.dw_release_exposure
where bdp_day='${bdp_day}'
)  ex
insert overwrite table dm_release.dm_exposure_cube partition(bdp_day='${bdp_day}')
select
ex.sources,
ex.channels,
ex.device_type,
ex.gender,
ex.area_code,
ex.age_range,
count(distinct(device_num)) exposure_count,
count(distinct(device_num)) / cu.user_count exposure_rates
join dm_release.dm_customer_cube cu
on cu.bdp_day='${bdp_day}'
and cu.sources = ex.sources
and cu.channels = ex.channels
and cu.device_type = ex.device_type
and cu.gender = ex.gender
and cu.area_code = ex.area_code
and cu.age_range = ex.age_range
group by
ex.sources,
ex.channels,
ex.device_type,
ex.gender,
ex.area_code,
ex.age_range,
cu.user_count
;
