# 2023-07 纽约出租车行程时长分析项目

一个数据科学项目，使用 PySpark 分析 2019 年 1 月至 6 月期间纽约市出租车的行程时长，包括数据预处理、地理可视化和回归建模过程。

---

## 项目概述

本项目基于纽约市出租车和豪华轿车委员会（TLC）提供的开源数据，结合天气和区域信息，探索影响出租车行程时长的因素，并构建回归模型对其进行预测。
在处理上千万条出租车行程数据时，本项目使用了 PySpark 进行大规模数据预处理与格式转换，以提高处理效率并支持后续建模流程。

- **研究目标**：理解并建模 `trip_duration`（乘客上车到下车的时间）
- **时间范围**：2019 年 1 月 – 2019 年 6 月
- **使用工具**：PySpark、Python、Pandas、NumPy、Scikit-learn、Seaborn、GeoPandas、Jupyter Notebook
- 项目的最终报告（PDF 格式）位于 [此处](./report/ADS_Project_1_Report.pdf)。

---

## 项目结构

```plaintext
├── data/
│   ├── landing/               # 原始数据下载目录
│   ├── raw/                   # 初步预处理后的数据
│   ├── curated/               # 清洗和标准化后的数据
│   ├── merged_data/           # 用于建模的最终合并数据
│   └── taxi_zones/            # 地理分区参考数据
├── notebooks/
│   ├── step2-1st_preprocessing.ipynb
│   ├── step3-2nd_preprocessing.ipynb
│   ├── step4-download_external_data_and_merge.ipynb
│   ├── step5-download_taxi_zone_data_and_geo_plot.ipynb
│   ├── step6-plotting_and_analysis.ipynb
│   ├── step8-model_and_evaluation.ipynb
├── scripts/
│   ├── step1-download_tlc_data.py
│   ├── step7-build_test_data.py
├── README.md
```

> ⚠️ 注意：仓库中的 `data/` 各子文件夹初始为空。请运行提供的脚本填充数据，或参考 README 中的数据来源链接。

---

## 数据处理流程

### 数据获取

- 下载 TLC 出租车行程数据（`step1`），以及包括天气和地理分区在内的外部数据（`step4`, `step5`, `step7`）

### 数据预处理

- 对出租车数据和外部数据进行初步和二次清洗（`step2`, `step3`, `step4`）
- 合并清洗后的数据，为建模做准备（`step4`, `step5`, `step7`）

### 探索性数据分析

- 对行程时长的分布、变量相关性和空间分布进行可视化分析（`step6`）

### 建模

- 使用合并后的数据构建回归模型
- 用 2019 年 7 月数据作为测试集评估模型效果（`step8`）

---

## 模型与评估

- 模型类型：标准回归模型（如线性回归、随机森林等）
- 评估指标：RMSE（均方根误差）、R² 分数
- 训练数据：2019 年 1–6 月；测试数据：2019 年 7 月

---

## 数据来源

- [纽约市出租车与豪华轿车委员会 (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- 天气与分区数据来自公开存储库

---

## 作者信息

**尹梓涵 (Zihan Yin)**  
墨尔本大学理学士（主修数据科学，辅修统计与随机建模）  
学生 ID：1149307

---

## 授权协议

本项目基于 [MIT License](LICENSE) 授权。

---

_此项目是作为墨尔本大学的学科MAST30034应用数据科学的一部分，于2023年第2学期完成的。_
