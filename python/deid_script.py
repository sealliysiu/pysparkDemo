import pandas as pd
from faker import Faker
import os

# 初始化 Faker
fake = Faker()
Faker.seed(42)

# 设置相对路径（从 python/ 目录执行）
input_dir = "../data"
output_dir = "../output"

# 确保输出目录存在
os.makedirs(output_dir, exist_ok=True)

# 加载数据
patients_df = pd.read_csv(os.path.join(input_dir, "PATIENTS.csv"))
admissions_df = pd.read_csv(os.path.join(input_dir, "ADMISSIONS.csv"))
caregivers_df = pd.read_csv(os.path.join(input_dir, "CAREGIVERS.csv"))

# 脱敏 PATIENTS.csv
unique_subjects = patients_df['subject_id'].unique()
subject_id_map = {orig: fake.unique.random_int(min=100000, max=999999) for orig in unique_subjects}
patients_df['subject_id'] = patients_df['subject_id'].map(subject_id_map)

patients_df['dob'] = pd.to_datetime(patients_df['dob'], errors='coerce')
patients_df['age'] = 2024 - patients_df['dob'].dt.year
patients_df['age_group'] = patients_df['age'].apply(
    lambda x: '0-20' if x <= 20 else '21-40' if x <= 40 else '41-60' if x <= 60 else '60+'
)
patients_df.drop(columns=['dob', 'dod', 'dod_hosp', 'dod_ssn', 'age'], inplace=True)

# 脱敏 ADMISSIONS.csv
admissions_df['subject_id'] = admissions_df['subject_id'].map(subject_id_map)
admissions_df['admittime'] = pd.to_datetime(admissions_df['admittime'], errors='coerce')
admissions_df['admit_month'] = admissions_df['admittime'].dt.to_period('M').astype(str)
admissions_df.drop(columns=['admittime', 'dischtime', 'deathtime', 'edregtime', 'edouttime'], inplace=True)

# 脱敏 CAREGIVERS.csv
unique_caregivers = caregivers_df['cgid'].unique()
cgid_map = {orig: fake.unique.random_int(min=10000, max=99999) for orig in unique_caregivers}
caregivers_df['cgid'] = caregivers_df['cgid'].map(cgid_map)
caregivers_df['description'] = "Medical Staff"

# 保存脱敏后的文件
patients_df.to_csv(os.path.join(output_dir, "PATIENTS_deid.csv"), index=False)
admissions_df.to_csv(os.path.join(output_dir, "ADMISSIONS_deid.csv"), index=False)
caregivers_df.to_csv(os.path.join(output_dir, "CAREGIVERS_deid.csv"), index=False)

# 保存 subject_id 映射关系
subject_id_map_df = pd.DataFrame(list(subject_id_map.items()), columns=["original_subject_id", "deid_subject_id"])
subject_id_map_df.to_csv(os.path.join(output_dir, "subject_id_map.csv"), index=False)

# 保存 cgid 映射关系
cgid_map_df = pd.DataFrame(list(cgid_map.items()), columns=["original_cgid", "deid_cgid"])
cgid_map_df.to_csv(os.path.join(output_dir, "cgid_map.csv"), index=False)

print("✅ 脱敏完成，文件保存在 ../output 目录中")
