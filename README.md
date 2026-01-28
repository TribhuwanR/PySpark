# PySpark

Want to know the exact cost of a Databricks job run?

Databricks stores all billing-related information in two system tables:

✅system.billing.usage
1️⃣ This table captures DBU usage details for every job run.
2️⃣If a job runs for 4 hours, you will see 4 records (one per hour).

✅system.billing.list_prices
1️⃣ This table contains the DBU prices for different cluster types and regions.
2️⃣For a single cluster type, there can be multiple records based on price effective start and end dates.

By joining these two tables, you can calculate the exact cost of a specific job run.

SQL query to calculate job cost:
-------------------------------------------------------------------------------------
select
sum(x.cost) as cost_in_dollar
from (
select
u.usage_quantity,
p.price,
u.usage_quantity * p.price as cost
from (
select
usage_start_time,
usage_end_time,
usage_quantity,
usage_unit,
sku_name
from system.billing.usage
where usage_metadata.job_run_id = '481719574637711'
) u
join (
select
pricing.effective_list.default as price,
sku_name
from system.billing.list_prices
where price_end_time is null
) p
on u.sku_name = p.sku_name
) x
-------------------------------------------------------------------------------------

Important points to keep in mind:
1️⃣Replace the job_run_id with your own job run ID
2️⃣The filter “price_end_time is null” fetches the latest prices
3️⃣For older job runs, replace this filter with a condition that matches the price start and end dates

This approach gives you a transparent and accurate view of Databricks job costs at the job-run level.

If you want to understand this in more details, checkout my video:
https://lnkd.in/gsdMpp_r

#databricks #dataengineering #sql

**Link:**  https://www.youtube.com/watch?v=tirGDRKm1Gc
