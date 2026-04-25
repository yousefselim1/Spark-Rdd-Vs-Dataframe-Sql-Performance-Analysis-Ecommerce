# 📊 Performance Analysis of Apache Spark APIs for Large-Scale E-Commerce Data Analytics  
### A Comparative Study of RDD, DataFrame, and Spark SQL with Optimization Techniques

---

## 📌 Table of Contents

1. [📖 Overview](#-overview)  
2. [🎯 Project Objectives](#-project-objectives)  
3. [❓ Problem Statement](#-problem-statement)  
4. [📊 Dataset Description](#-dataset-description)  
5. [🧹 Data Preprocessing](#-data-preprocessing)  
6. [🔍 Queries Implemented](#-queries-implemented)  
7. [⚙️ Implementation Overview](#️-implementation-overview)  
8. [📈 Execution Plan Analysis](#-execution-plan-analysis)  
9. [⚡ Performance Comparison](#-performance-comparison)  
10. [🚀 Optimization Analysis](#-optimization-analysis)  
11. [📊 Cluster Configuration & Scalability Tests](#-cluster-configuration--scalability-tests)  
12. [📌 Final Insights](#-final-insights)  
13. [📂 Project Structure](#-project-structure)  
14. [🛠️ Setup & Installation](#️-setup--installation)  
15. [▶️ How to Run](#️-how-to-run)  
16. [📊 Results](#-results)  
17. [📚 Technologies Used](#-technologies-used)  
18. [👥 Team](#-team)  

---

# 📖 Overview

This project analyzes a **large-scale global e-commerce dataset (1M+ records)** using **Apache Spark**. It focuses on comparing three Spark APIs:

- **RDD (Resilient Distributed Dataset)**
- **DataFrame API**
- **Spark SQL**

The project evaluates:

- Query execution performance  
- Execution plans  
- Optimization techniques  
- Scalability behavior  

---

# 🎯 Project Objectives

- Analyze large-scale data using Apache Spark  
- Compare performance of RDD vs DataFrame vs SQL  
- Understand Spark optimization mechanisms  
- Apply optimization techniques (caching, partitioning, joins)  
- Extract meaningful business insights  

---

# ❓ Problem Statement

Modern e-commerce platforms generate massive amounts of transactional and behavioral data. Traditional systems struggle to process such data efficiently.

### Core Question:

> **How can Apache Spark efficiently analyze large-scale e-commerce data, and how do RDD, DataFrame, and Spark SQL differ in performance and optimization?**

---

# 📊 Dataset Description

## 🔗 Dataset Source  
[Kaggle Dataset](https://www.kaggle.com/datasets/akrambelha/global-e-commerce-dataset-1m-records-20242026)

---

## 📦 Dataset Overview

| Attribute | Value |
|----------|------|
| Records | 1,000,000+ |
| Columns | 40+ |
| Format | CSV, Parquet |
| Domain | E-Commerce |

---

## 🧩 Key Features

- Customer Data  
- Product Data  
- Transaction Data  
- Payment Info  
- Shipping Info  
- Behavioral Data  

---

## 📊 Data Types

| Type | Examples |
|------|--------|
| Numerical | price, profit |
| Categorical | category, country |
| Temporal | order_date |

---

## 🚀 Big Data Characteristics

- **Volume:** >1M records  
- **Variety:** Multiple data types  
- **Velocity:** Simulated continuous transactions  
- **Complexity:** Multi-entity relationships  

---

# 🧹 Data Preprocessing

## Steps Performed

- Removed null values  
- Dropped duplicates  
- Converted timestamps  
- Feature engineering (month extraction)  

---

## Example Code

```python
df = df.dropDuplicates()
df = df.filter(df["total_price_usd"].isNotNull())
df = df.withColumn("order_date", F.to_timestamp("order_date"))
```

---

# 🔍 Queries Implemented

| Query | Description |
|------|------------|
| Q1 | Filter high-value orders |
| Q2 | Revenue by category |
| Q3 | Avg order value by country |
| Q4 | Orders by payment method |
| Q5 | Max order per customer |
| Q6 | Monthly orders |
| Q7 | Revenue by category & country |
| Q8 | Join analysis |
| Q9 | Window function (cumulative revenue) |
| Q10 | High-value customers |

---

# ⚙️ Implementation Overview

Each query implemented using:

| API | Description |
|----|-----------|
| RDD | Low-level, no optimization |
| DataFrame | Structured, optimized |
| SQL | Declarative, optimized |

---

## Analysis Includes

- Execution Plans (`.explain(True)`)
- Execution Time
- Optimization Techniques

---

# 📈 Execution Plan Analysis

## Spark Plans

| Plan Type | Description |
|----------|------------|
| Logical | Query structure |
| Optimized | Catalyst optimizations |
| Physical | Execution strategy |

---

## Common Operators

- HashAggregate  
- SortMergeJoin  
- Exchange (Shuffle)  

---

# ⚡ Performance Comparison

## Results Table

| Query | RDD (s) | DataFrame (s) | SQL (s) |
|------|--------|--------------|--------|
| Q1 | 104 | 10.7 | 8.8 |
| Q2 | 112 | 12.1 | 9.5 |
| Q3 | 108 | 10.0 | 8.3 |
| Q4 | 105 | 9.9 | 8.1 |
| Q5 | 127 | 13.2 | 11.0 |
| Q6 | 109 | 9.8 | 9.2 |
| Q7 | 144 | 9.8 | 8.0 |
| Q8 | 136 | 13.9 | 11.1 |
| Q9 | 233 | 17.0 | 13.2 |
| Q10 | 160 | 33.8 | 11.5 |

---

## Key Insight

\[
\text{Performance: SQL} > \text{DataFrame} >> \text{RDD}
\]

---

# 🚀 Optimization Analysis

## Techniques Used

| Technique | Impact |
|----------|--------|
| Caching | Faster repeated queries |
| Broadcast Join | Avoid shuffle |
| Partitioning | Improve parallelism |
| Parquet | Faster I/O |

---

## CSV vs Parquet

| Format | Time |
|--------|-----|
| CSV | 24s |
| Parquet | 1.3s |

---

# 📊 Cluster Configuration & Scalability Tests

To evaluate system scalability, different Spark configurations were tested by varying the number of partitions and memory allocation.

The results show that increasing partitions improves parallelism but may introduce overhead if too large. Similarly, increasing memory improves performance up to a certain limit, after which the improvement becomes minimal.

These experiments demonstrate how Spark performance depends on cluster configuration and resource allocation.

---

# 📌 Final Insights

## Best API

- **Spark SQL performed best**
- DataFrame close second
- RDD slowest

---

## Business Insights

- High-value customers identified  
- Revenue varies by country  
- Category demand differs  

---

## Lessons Learned

- Use structured APIs  
- Optimize joins  
- Use Parquet  
- Apply caching  

---

# 📂 Project Structure

```
spark_project/
├── setup.py
├── convert_parquet.py
├── preprocessing.py
├── eda.py
├── data/
│   ├── ecommerce.csv
│   └── ecommerce.parquet/
├── rdd/
│   └── queries_rdd.py
├── dataframe/
│   └── queries_df.py
├── sql/
│   └── queries_sql.py
├── optimization/
│   └── join_optimization.py
├── results/
│   ├── rdd_output.log
│   ├── df_output.log
│   ├── sql_output.log
│   ├── optimization_output.log
│   ├── rdd_timings.json
│   ├── df_timings.json
│   ├── sql_timings.json
│   └── optimization_timings.json
```

---

# 🛠️ Setup & Installation

## Requirements

- Python 3.10+
- Apache Spark 3.5+
- Java 11

---

## Install Spark

```bash
wget https://downloads.apache.org/spark/spark-3.5.1.tgz
tar -xvf spark-3.5.1.tgz
```

---

## Set Environment

```bash
export SPARK_HOME=~/spark
export PATH=$SPARK_HOME/bin:$PATH
```

---

# ▶️ How to Run

## Run Queries

```bash
spark-submit code/all_queries.py
```

## Save Output

```bash
spark-submit code/all_queries.py | tee output/results.txt
```

---

# 📊 Results

- Logs stored in `/results/`
- JSON timing files included
- Screenshots required for report

---

# 📚 Technologies Used

- Apache Spark  
- Python  
- PySpark  
- Hadoop (Local Mode)  
- VirtualBox  

---

# 👥 Team

| Name                     |
|--------------------------|
| Yousef Selim             |
| Mohamed Ehab Yousri      |

---

# ⭐ Final Note

This project demonstrates how **Apache Spark + optimization techniques** enable scalable and efficient big data analytics.

---
