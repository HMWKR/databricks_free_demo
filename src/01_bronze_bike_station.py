# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Task 1 Bronze
# MAGIC %md
# MAGIC # Task 1. Bronze - 따릉이 API 원본 적재
# MAGIC
# MAGIC 공공포털 따릉이 대여소 API 응답(JSON)을 가공 없이 `dddb.default.bronze_bike_station`에 APPEND 합니다. 실행할 때마다 새 배치(`batch_id`)가 누적되어 수집 이력이 보존됩니다.

# COMMAND ----------

# DBTITLE 1,Bronze: API 호출 후 APPEND
import requests
from pyspark.sql import functions as F

BRONZE = "dddb.default.bronze_bike_station"
API_KEY = dbutils.secrets.get(scope="seoul_api", key="api_key")   # Setup 노트북에서 저장한 키

# Job 파라미터: dry_run(적재 생략하고 수집만 점검) / max_pages(1회 최대 수집 페이지 수)
dbutils.widgets.text("dry_run", "false", "Dry run 여부")
dbutils.widgets.text("max_pages", "5", "최대 수집 페이지")
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"
MAX_PAGES = int(dbutils.widgets.get("max_pages"))

# API는 1회 1000건까지 -> 1000건씩 페이징하며 원본 JSON 문자열 수집
pages, start = [], 1
while len(pages) < MAX_PAGES:
    url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/bikeList/{start}/{start + 999}/"
    res = requests.get(url, timeout=30)
    res.raise_for_status()
    if "rentBikeStatus" not in res.json():   # 더 이상 데이터 없음
        break
    pages.append((start, res.text))
    start += 1000

# 같은 실행에서 수집한 페이지는 같은 batch_id / ingested_at 을 가짐
bronze_df = (spark.createDataFrame(pages, "start_index INT, raw_json STRING")
    .withColumn("batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
    .withColumn("ingested_at", F.current_timestamp()))

# 증분: 매 실행마다 APPEND (dry_run=true면 적재 생략)
if DRY_RUN:
    print(f"[dry-run] 수집 {len(pages)} pages 확인, 적재 생략")
else:
    bronze_df.write.mode("append").option("mergeSchema", "true").saveAsTable(BRONZE)
    print(f"Bronze APPEND: {len(pages)} pages")
display(spark.table(BRONZE).groupBy("batch_id").count().orderBy(F.desc("batch_id")).limit(5))