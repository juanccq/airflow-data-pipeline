{{ config(
	materialized='incremental',
	unique_key=['city', 'load_date']
) }}

with silver_data as (
	select * from {{ source('spark_silver', 'sales_by_city') }}

	{% if is_incremental() %}
		-- Grabs only the data newer than what is already in the mart.
		where load_date > (select max(load_date) from {{ this }})
	{% endif %}
)

select
	city,
	total_sales,
	load_date,
	case
		when total_sales > 5665000000 then 'Tier 1 - High Volume'
		else 'Tier 2 - Standard'
	end as performance_tier
from silver_data
