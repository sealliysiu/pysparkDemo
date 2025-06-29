# 🏥 MIMIC-III 医疗结构化数据脱敏处理项目

本项目旨在对 MIMIC-III Clinical Database 的结构化 CSV 数据进行去标识化（De-Identification）处理，以保障隐私数据安全，同时保留研究与分析所需的信息。适用于医疗 AI 建模、数据共享前的预处理、合规训练数据构建等场景。

---

## 📁 项目目录结构

```
├── data/             # 原始输入文件（PATIENTS.csv, ADMISSIONS.csv, CAREGIVERS.csv）
├── output/           # 输出的脱敏文件
├── python/
│   ├── deid_pandas.py    # 使用 pandas 实现的脱敏脚本
│   └── deid_spark.py     # 使用 PySpark 实现的脱敏脚本（推荐）
├── subject_id_map.csv    # subject_id 映射日志（仅内部审计使用）
├── cgid_map.csv          # caregivers 映射日志
└── De-ID_Report.md       # 脱敏说明文档
```

---

## 🛠️ 脚本功能说明

### 输入文件（位于 `data/`）：
- `PATIENTS.csv`: 包含病人基本信息
- `ADMISSIONS.csv`: 包含入院记录
- `CAREGIVERS.csv`: 包含医护人员信息

### 脱敏逻辑：

| 字段名       | 处理方式                  |
|--------------|---------------------------|
| `subject_id` | 替换为随机编号（不可逆）   |
| `dob`        | 泛化为年龄段（如 21-40）  |
| `admittime`  | 精度降为年月（yyyy-MM）   |
| `cgid`       | 替换为随机编号（不可逆）   |
| `description`| 统一替换为 “Medical Staff” |
| 时间戳字段   | 精确时间全部删除           |

---

## 🧩 实现版本对比

| 特性/工具   | `deid_pandas.py`              | `deid_spark.py`               |
|------------|-------------------------------|-------------------------------|
| 适用场景   | 小型测试，原型开发              | 大规模数据处理，推荐生产使用   |
| 性能       | 依赖内存，单线程                | 并行执行，支持百万级数据       |
| 依赖库     | `pandas`, `faker`              | `pyspark`, `faker`            |
| 输出格式   | CSV 文件 + 脱敏映射日志         | 同上                           |
| 执行环境   | 任意 Python 环境                | 推荐使用 Ubuntu + Java 8 虚拟机 |

---

## 🚀 使用方法

### ✅ 方法 1：运行 pandas 版本（适用于本机测试）

```bash
cd python
python deid_pandas.py
```

### ✅ 方法 2：运行 PySpark 版本（推荐）

```bash
cd python
spark-submit deid_spark.py
```

> ⚠️ 请确保你已正确安装 Java 8 与 Spark，并在 Ubuntu 虚拟机中运行更稳定。

---

## 📦 依赖环境

- Python 3.8+
- Java 8（⚠️ 非 Java 17）
- Spark 3.x
- 安装依赖：

```bash
pip install pandas faker pyspark
```

---

## 🧾 输出结果

位于 `output/` 目录：

- `PATIENTS_deid.csv` / `PATIENTS_deid_spark.csv`
- `ADMISSIONS_deid.csv` / `ADMISSIONS_deid_spark.csv`
- `CAREGIVERS_deid.csv` / `CAREGIVERS_deid_spark.csv`
- `subject_id_map.csv`, `cgid_map.csv`
- [脱敏说明文档](./De-ID_Report.md)

---

## 📚 参考与合规性说明

- 参考标准：HIPAA Safe Harbor，PIPL，MIT MIMIC 数据使用条款
- 所有脱敏字段不可逆，输出数据不包含可识别个体身份的信息
- 项目仅供教学、科研、合规实验用途

---

## 👨‍💻 作者

liy | 2025  
GitHub: [sealliysiu](https://github.com/sealliysiu)
