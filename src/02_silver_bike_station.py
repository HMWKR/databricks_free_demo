# Databricks notebook source
# DBTITLE 1,Task 2 Silver
# MAGIC %md
# MAGIC # Task 2. Silver - 파싱 / 정제 후 MERGE
# MAGIC
# MAGIC Bronze에서 아직 처리하지 않은 신규 배치만 읽어(`ingested_at` 기준) JSON을 파싱하고, `station_id` 기준으로 `dddb.default.silver_bike_station`에 MERGE(있으면 UPDATE, 없으면 INSERT) 합니다.

# COMMAND ----------

# DBTITLE 1,Silver: 신규 배치 파싱 후 MERGE
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE = "dddb.default.bronze_bike_station"
SILVER = "dddb.default.silver_bike_station"

# 1) 대상 테이블이 없으면 먼저 생성
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
  station_id STRING, station_name STRING, rack_cnt INT, parking_bike_cnt INT,
  shared_rate INT, latitude DOUBLE, longitude DOUBLE, ingested_at TIMESTAMP, updated_at TIMESTAMP)
""")

# 2) 증분 읽기: Silver에 반영된 마지막 시각 이후의 Bronze 배치만
last_ts = spark.table(SILVER).agg(F.max("ingested_at")).first()[0]
bronze_new = spark.table(BRONZE)
if last_ts is not None:
    bronze_new = bronze_new.filter(F.col("ingested_at") > F.lit(last_ts))

# 3) JSON 파싱 / 타입 변환
json_schema = """
rentBikeStatus STRUCT<row: ARRAY<STRUCT<
  stationId STRING, stationName STRING, rackTotCnt STRING, parkingBikeTotCnt STRING,
  shared STRING, stationLatitude STRING, stationLongitude STRING>>>
"""
src = (bronze_new
    .select(F.from_json("raw_json", json_schema).alias("j"), "ingested_at")
    .select(F.explode("j.rentBikeStatus.row").alias("r"), "ingested_at")
    .select(
        F.col("r.stationId").alias("station_id"),
        F.trim("r.stationName").alias("station_name"),
        F.col("r.rackTotCnt").cast("int").alias("rack_cnt"),
        F.col("r.parkingBikeTotCnt").cast("int").alias("parking_bike_cnt"),
        F.col("r.shared").cast("int").alias("shared_rate"),
        F.col("r.stationLatitude").cast("double").alias("latitude"),
        F.col("r.stationLongitude").cast("double").alias("longitude"),
        "ingested_at")
    .filter(F.col("station_id").isNotNull())
    # 신규 배치가 여러 개면 대여소별 최신 1건만 (MERGE 키 중복 방지)
    .withColumn("rn", F.row_number().over(Window.partitionBy("station_id").orderBy(F.desc("ingested_at"))))
    .filter("rn = 1").drop("rn")
    .withColumn("updated_at", F.current_timestamp()))

# 4) MERGE: station_id 기준 Upsert
(DeltaTable.forName(spark, SILVER).alias("t")
    .merge(src.alias("s"), "t.station_id = s.station_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print("기준 시각:", last_ts, "| 신규 대여소 행:", src.count())
display(spark.sql(f"DESCRIBE HISTORY {SILVER}").select("version", "operation", "operationMetrics").limit(3))