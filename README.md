# 🏦 Bank Project

A data processing and analysis project for banking datasets using **Python**, **Spark**, and **Hadoop ecosystem tools**.

---

## 🚀 Project Overview

This project focuses on **data processing, scoring analysis, and reporting** for banking datasets. It includes:

- ETL (Extract, Transform, Load) scripts for data preprocessing
- Spark jobs for scalable analysis
- Hive/Pig scripts for querying and scoring
- Sample input datasets for testing

---

## 🛠️ Technologies Used

![Python](https://img.shields.io/badge/Python-3.13-blue?style=flat-square&logo=python)
![Apache Spark](https://img.shields.io/badge/Spark-3.5-orange?style=flat-square&logo=apache-spark)
![Hadoop](https://img.shields.io/badge/Hadoop-ECE21A?style=flat-square&logo=apache-hadoop)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github)

---

## ⚙️ How to Run

1. **Set up Spark environment** (PySpark or standalone Spark cluster)

2. **Run Spark job**:
```bash
spark-submit SparkScoringAnalysis.py
```

3. **Run Pig scripts** (optional):
```bash
pig job.pig
```

4. **Query results in Hive**:
```sql
-- Example:
SELECT * FROM scoring_results LIMIT 10;
```

---

## 📫 Connect With Me

- 📧 Email: ghalia.benais@gmail.com

---

## ⚡ Notes

- Ensure Hadoop/Spark services are running before executing scripts
- Sample data is included for testing purposes
- Scripts are modular for easier customization
