# Databricks notebook source
# DBTITLE 1,Task 3 Gold
# MAGIC %md
# MAGIC # Task 3. Gold - 대여소별 거치율 MERGE
# MAGIC
# MAGIC Silver에서 이번에 갱신된 대여소(`updated_at` 기준)만 골라 거치율과 상태를 계산하고, `station_id` 기준으로 `dddb.default.gold_bike_station_usage`에 MERGE 합니다.

# COMMAND ----------

# DBTITLE 1,Gold: 갱신 대여소 집계 후 MERGE
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER = "dddb.default.silver_bike_station"
GOLD   = "dddb.default.gold_bike_station_usage"

# 1) 대상 테이블이 없으면 먼저 생성
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD} (
  station_id STRING, station_name STRING, rack_cnt INT, parking_bike_cnt INT,
  parking_rate DOUBLE, status STRING, latitude DOUBLE, longitude DOUBLE,
  silver_updated_at TIMESTAMP, updated_at TIMESTAMP)
""")

# 2) 증분 읽기: Gold에 반영된 마지막 Silver 갱신 시각 이후 행만
last_ts = spark.table(GOLD).agg(F.max("silver_updated_at")).first()[0]
silver_new = spark.table(SILVER)
if last_ts is not None:
    silver_new = silver_new.filter(F.col("updated_at") > F.lit(last_ts))

# 3) 거치율 = 주차 자전거 / 거치대 수, 상태 등급
src = (silver_new
    .withColumn("parking_rate",
                F.round(F.col("parking_bike_cnt") / F.nullif(F.col("rack_cnt"), F.lit(0)) * 100, 1))
    .withColumn("status",
        F.when(F.col("parking_bike_cnt") == 0, "대여불가(0대)")
         .when(F.col("parking_rate") < 30, "부족")
         .when(F.col("parking_rate") <= 100, "적정")
         .otherwise("과잉"))
    .select("station_id", "station_name", "rack_cnt", "parking_bike_cnt",
            "parking_rate", "status", "latitude", "longitude",
            F.col("updated_at").alias("silver_updated_at"),
            F.current_timestamp().alias("updated_at")))

# 4) MERGE: station_id 기준 Upsert
(DeltaTable.forName(spark, GOLD).alias("t")
    .merge(src.alias("s"), "t.station_id = s.station_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print("기준 시각:", last_ts, "| 반영 대여소:", src.count())
display(spark.table(GOLD).groupBy("status").count().orderBy(F.desc("count")))