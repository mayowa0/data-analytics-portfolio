# Mayowa Alamutu — Data Analytics Portfolio

> Data Analyst & Researcher | Python · SQL · Google Looker Studio · Power BI · Machine Learning · Statistics  
> 📍 Lagos, Nigeria &nbsp;|&nbsp; 🌐 [mayowahabeeb.framer.website](https://mayowahabeeb.framer.website/) &nbsp;|&nbsp; 💼 [LinkedIn](https://www.linkedin.com/in/mayowa-alamutu-84185a25a) &nbsp;|&nbsp; 📧 mayowa.habeeb18@gmail.com

---

## About This Portfolio

This repository contains the source code, notebooks, and project pages for my data analytics portfolio. I'm an engineering-trained data analyst and researcher with hands-on experience in SQL pipelines, Python automation, business intelligence dashboards, statistical analysis, and machine learning — currently working as a Data Analyst Intern at a Nigerian fintech company.

**Live Portfolio →** [mayowa0.github.io/data-analytics-portfolio](https://mayowa0.github.io/data-analytics-portfolio/)

---

## Projects

### 🤖 Project 01 — Automated Loan Reporting System
**Tools:** Python · PostgreSQL · psycopg2 · pandas · openpyxl · ZeptoMail API

A Python-based automation pipeline that queries a live PostgreSQL database on schedule, calculates business KPIs across two user segments (merchants and individual users), and delivers formatted HTML email reports with Excel attachments — automatically, every week and every month. Four report variants built: Merchant Weekly, Merchant Monthly, Individual User Weekly, Individual User Monthly. Zero manual steps after deployment.

- `merchant_weekly_report.py` — Weekly merchant loan disbursement report
- `seedfi_user_monthly_report.py` — Monthly individual user report (signups, disbursements, conversion rate, funnel metrics)

---

### 📊 Project 02 — Q1 2026 Business Performance Analysis
**Tools:** SQL · Python · pandas · Microsoft PowerPoint

A 29-slide executive presentation covering the full Q1 2026 performance of a Nigerian B2C digital lending platform. Sections: Executive Summary → Disbursement Analysis → Repayment & Collections → Conversion & Process Efficiency → Comparative Analysis → Recommendations.

Key analytical frameworks: borrower persona segmentation (DPD-based), application drop-off funnel analysis, vintage cohort repayment tracking, Q2 forecast modelling, and external industry benchmarking.

---

### 📈 Project 03 — Interactive Business Dashboards (Google Looker Studio)
**Tools:** Google Looker Studio · SQL · PostgreSQL · Data Modelling

Built and maintained a suite of interactive analytical dashboards connecting live PostgreSQL data sources to real-time KPI reports. Designed for non-technical stakeholders with drill-down capabilities, trend analysis, and period comparisons.

*[View on E-Portfolio →](https://mayowahabeeb.framer.website/)*

---

### 🏦 Project 05 — Loan Default Prediction Model
**Tools:** Python · scikit-learn · XGBoost · SMOTE · pandas · Seaborn

Binary classification model to predict loan default risk from borrower demographic, financial, and behavioural features. Compared Logistic Regression, Random Forest, and XGBoost. Applied SMOTE to handle class imbalance. Achieved **89% accuracy** and **0.91 ROC-AUC**. Feature importance output makes predictions interpretable to non-technical credit officers.

📓 [`loan_default_prediction.ipynb`](loan_default_prediction.ipynb)

---

### ⚡ Project 06 — Nigeria Energy Access Analysis
**Tools:** Python · pandas · Seaborn · Matplotlib · Power BI · World Bank API

Exploratory data analysis of electricity access disparities across Nigeria's 6 geopolitical zones using public data from the World Bank, NERC, and NBS. Mapped a 51 percentage-point North–South access gap and visualised the chronic generation–demand deficit (4,000 MW actual vs 18,000 MW demand) leaving 85+ million Nigerians without reliable power.

📓 [`nigeria_energy_analysis.ipynb`](nigeria_energy_analysis.ipynb)

---

### ♻️ Project 07 — CO₂ Emissions & Recycling Impact Dashboard
**Tools:** Python · pandas · Excel · Tableau · IEA Data

Quantifies the environmental benefit of metallurgical recycling across five metal streams in Nigeria (aluminium, steel, copper, lead, e-waste). Built a dynamic Excel impact calculator and an interactive Tableau dashboard. Key finding: full formalisation of Lagos's recycling sector could avoid 248,000 tonnes of CO₂ annually. Directly extends my co-authored publication on recycling in metallurgical processes (Harvard IJERT, 2025).

📓 [`co2_recycling_analysis.ipynb`](co2_recycling_analysis.ipynb)

---

### 📐 Project 08 — Fintech Growth Analytics: A Statistical Deep Dive
**Tools:** Python · scipy · statsmodels · pandas · Seaborn · Matplotlib

Four statistical frameworks applied to a Nigerian digital lending dataset in one cohesive notebook — both academic rigour and plain-English business interpretation throughout.

| Section | Method | Key Result |
|---|---|---|
| 1 | Two-proportion z-test (A/B) | New UI: +3.6pp lift, p<0.001, Power=84% |
| 2 | OLS Multiple Regression | Adj. R²=0.72, income & credit score top predictors |
| 3 | Holt-Winters Time Series | Q2 forecast: ₦2.8B ±10%, MAPE<5% |
| 4 | Chi-Square + One-Way ANOVA | Default rates differ significantly by tier, η²=0.41 |

Full assumptions testing throughout: Shapiro-Wilk, Levene, VIF/multicollinearity, ADF stationarity, Bonferroni-corrected post-hoc comparisons, bootstrap prediction intervals.

📓 [`fintech_statistical_analysis.ipynb`](fintech_statistical_analysis.ipynb)

---

## Publications

| Title | Journal | Year | DOI |
|---|---|---|---|
| Advanced Nanocomposite Polymer-Biomaterial Catalysts for Integrated Biodiesel and Battery Systems | Asian Journal of Advanced Research and Reports | 2025 | [10.9734/ajarr/2025/v19i121217](https://doi.org/10.9734/ajarr/2025/v19i121217) |
| Recycling and Reuse of Materials in Metallurgical Processes | Harvard International Journal of Engineering Research and Technology | 2025 | [10.70382/hijert.v8i5.004](https://doi.org/10.70382/hijert.v8i5.004) |
| Mineralogical Characterization of Sooro and Zankan Lithium Ore | Under Review | 2025 | Available upon request |

---

## Certifications (Selected)

- Data Analytics Programme — SideHustle / Terra Holding Limited
- Deloitte Data Analytics Job Simulation — Forage
- Machine Learning for All — University of London / Coursera
- Excel Fundamentals for Data Analysis — Macquarie University / Coursera
- Foundations: Data, Data, Everywhere — Google / Coursera
- Computer Vision with Embedded Machine Learning — Edge Impulse / Coursera

*Full certification list on the [live portfolio](https://mayowa0.github.io/data-analytics-portfolio/).*

---

## Tech Stack

```
Languages    : Python 3.11 · SQL (PostgreSQL)
Statistics   : scipy · statsmodels · hypothesis testing · regression · time series
ML Libraries : scikit-learn · XGBoost · imbalanced-learn (SMOTE)
Data         : pandas · NumPy · openpyxl · psycopg2
Visualisation: Seaborn · Matplotlib · Google Looker Studio · Power BI · Tableau
Other        : Git · Jupyter Notebook · python-dotenv · ZeptoMail API
```

---

## Repository Structure

```
data-analytics-portfolio/
│
├── index.html                              # Main portfolio (live site homepage)
├── project_automation.html                 # Project 01 — Automated Reporting System
├── project_q1_analysis.html                # Project 02 — Q1 Performance Analysis
├── project_loan_default.html               # Project 05 — Loan Default Prediction
├── project_nigeria_energy.html             # Project 06 — Nigeria Energy Analysis
├── project_co2_recycling.html              # Project 07 — CO2 & Recycling Dashboard
├── project_statistical_analysis.html       # Project 08 — Statistical Deep Dive
│
├── merchant_weekly_report.py               # Automated merchant weekly report script
├── seedfi_user_monthly_report.py           # Automated individual user monthly report script
│
├── loan_default_prediction.ipynb           # ML notebook — loan default model
├── nigeria_energy_analysis.ipynb           # EDA notebook — Nigeria energy access
├── co2_recycling_analysis.ipynb            # Analysis notebook — CO2 & recycling
├── fintech_statistical_analysis.ipynb      # Stats notebook — A/B, regression, TS, ANOVA
│
└── README.md                               # This file
```

---

## Background

B.Eng. Materials & Metallurgical Engineering, University of Ilorin (2024). I transitioned into data analytics because the most impactful lever in any organisation is the quality of its decisions — and decisions live or die on data. My engineering background gives me a systematic, evidence-based approach to analysis that I now apply to fintech, energy, and sustainability domains.

---

## Contact

Open to data analyst, data science, analytics engineering, and research roles.

**Email:** mayowa.habeeb18@gmail.com  
**LinkedIn:** [linkedin.com/in/mayowa-alamutu-84185a25a](https://www.linkedin.com/in/mayowa-alamutu-84185a25a)  
**E-Portfolio:** [mayowahabeeb.framer.website](https://mayowahabeeb.framer.website/)
