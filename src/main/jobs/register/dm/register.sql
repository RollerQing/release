insert overwrite table dm_release.dm_register_users partition(bdp_day='20190614')
select
case device_type 
when 1 then 'iOS'
when 2 then 'android'
else 'other' end as platform,
sources,
channels,
count(*)
from
dw_release.dw_release_register_users
group by
device_type,
sources,
channels
;