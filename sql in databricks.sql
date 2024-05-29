-- Databricks notebook source
-- MAGIC %fs rm -r  /user/hive/warehouse/demo_db.db

-- COMMAND ----------

create database if not exists demo_db

-- COMMAND ----------

create table if not exists demo_db.fire_service_calls_tbl(
  CallNumber string,
  UnitID string,
  IncidentNumber string,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode string,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority string,
  ALSUnit string,
  CallTypeGroup string,
  NumAlarms string,
  UnitType string,
  UnitSequenceInCallDispatch string,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay string
) using parquet

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl 
values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null)

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------

truncate table demo_db.fire_service_calls_tbl

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl
select * from global_temp.fire_service_calls_view

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------

use demo_db

-- COMMAND ----------

select count(*) from fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q1 : HOW MANY DISTINCT TYPES OF CALLS WERE MADE TO THE FIRE DEPARTMENT?

-- COMMAND ----------

select distinct CallType, count(CallType) as TotalCalls from fire_service_calls_tbl where CallType is not null group by CallType order by TotalCalls desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q2 : What were the distinct call types of calls made to the fire department?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC the solution is in the above query only. No need to insert the total calls column and group by

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q3 : find out all responses with delayed times greater than 5 mins?

-- COMMAND ----------

select * from fire_service_calls_tbl where delay > 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q4 : what were the most common call types?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC this is also done in the first question by adding the total calls column in the output and using group by

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q5: what zip codes accounted for most common calls?

-- COMMAND ----------

select distinct calltype, zipcode, count(*) as total_incidents_in_each_zip_code from fire_service_calls_tbl where CallType is not null group by CallType, zipcode order by total_incidents_in_each_zip_code desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q6: what san francisco neighborhoods are in the zip codes 94102 and 94103?

-- COMMAND ----------

select distinct Neighborhood from fire_service_calls_tbl  where Zipcode in ("94102","94103")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC q7 : what was the sum of all alarms, average, min and max of the call response time?

-- COMMAND ----------

select sum(NumAlarms), min(Delay), avg(delay), max(delay) from fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q8 : how many distinct years of data is in the dataset?

-- COMMAND ----------

select distinct year(to_date(CallDate , "MM/dd/yyyy")) as year from fire_service_calls_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q9: what week of the year 2018 had the most fire calls?

-- COMMAND ----------

select distinct weekofyear(to_date(CallDate , "MM/dd/yyyy")) as week, count(*) as calls_made  from fire_service_calls_tbl where year(to_date(CallDate , "MM/dd/yyyy"))  = 2018 group by week order by week desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC q10 : what neighborhoods in san francisco had the worst response time in 2018? (used 2011 later)

-- COMMAND ----------

select Neighborhood, Delay from fire_service_calls_tbl where City = "SF" and  year(to_date(CallDate, "MM/dd/yyyy")) = 2011b order by delay desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC we can change the year to be anythng but in the question there was 2018 earlier and there were no records for SF city in 2018 so I used 2011.
