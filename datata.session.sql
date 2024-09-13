
select min(year),max(year),array_sort(json_keys(attributes)) as attrs
from staging.parcels_raw
group by 3
order by 1

