from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, when, lit, monotonically_increasing_id, to_date, date_format
import os

# 初始化 Spark 会话
spark = SparkSession.builder.appName("MIMIC De-ID with Mapping").getOrCreate()

# 设置路径
input_path = "../data/"
output_path = "../output/"

# 确保输出目录存在
os.makedirs(output_path, exist_ok=True)

# 1️⃣ 处理 PATIENTS.csv
patients = spark.read.option("header", True).csv(input_path + "PATIENTS.csv")
patients = patients.withColumn("dob", to_date("dob"))
patients = patients.withColumn("age", lit(2024) - year("dob"))
patients = patients.withColumn("age_group", when(col("age") <= 20, "0-20")
                                .when(col("age") <= 40, "21-40")
                                .when(col("age") <= 60, "41-60")
                                .otherwise("60+"))
patients = patients.withColumn("deid_subject_id", monotonically_increasing_id() + 100000)

# 保存映射表
subject_id_map = patients.select("subject_id", "deid_subject_id").dropDuplicates()
subject_id_map.write.mode("overwrite").option("header", True).csv(output_path + "subject_id_map.csv")

# 脱敏后的主表
patients_clean = patients.select("deid_subject_id", "gender", "age_group")
patients_clean.write.mode("overwrite").option("header", True).csv(output_path + "PATIENTS_deid_spark.csv")

# 2️⃣ 处理 ADMISSIONS.csv
admissions = spark.read.option("header", True).csv(input_path + "ADMISSIONS.csv")
admissions = admissions.join(subject_id_map, on="subject_id")
admissions = admissions.withColumn("admittime", to_date("admittime"))
admissions = admissions.withColumn("admit_month", date_format("admittime", "yyyy-MM"))
admissions_clean = admissions.select("deid_subject_id", "admission_type", "admit_month")
admissions_clean.write.mode("overwrite").option("header", True).csv(output_path + "ADMISSIONS_deid_spark.csv")

# 3️⃣ 处理 CAREGIVERS.csv
caregivers = spark.read.option("header", True).csv(input_path + "CAREGIVERS.csv")
caregivers = caregivers.withColumn("deid_cgid", monotonically_increasing_id() + 20000)
caregivers = caregivers.withColumn("description", lit("Medical Staff"))

# 保存映射表
cgid_map = caregivers.select("cgid", "deid_cgid").dropDuplicates()
cgid_map.write.mode("overwrite").option("header", True).csv(output_path + "cgid_map.csv")

caregivers_clean = caregivers.select("deid_cgid", "description")
caregivers_clean.write.mode("overwrite").option("header", True).csv(output_path + "CAREGIVERS_deid_spark.csv")

print("✅ PySpark 脱敏 + 映射输出完成，所有文件已保存在 output/ 目录。")
spark.stop()
