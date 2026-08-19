# Fintech SaaS AB Analytics

An **end-to-end analytics project** simulating an **A/B test** on a **fintech** app's new onboarding flow across **10K users**. 
Although the per-user revenue does not increase significantly, the new flow offers overall **improvements** in 
the **activation, conversion, and retention rates** and should be **shipped**. 

### Tech Stack
- BigQuery Data Warehouse
- dbt
- SQL (Joins, CTEs, Aggregations)
- Python 
- Jupyter Notebook
- GitHub Projects and Actions

### Analyses
- SRM check, A/B tests for primary and secondary metrics, funnel analysis, segment analysis, 
churn, and revenue analysis

### Insights
- Treatment (the new onboarding flow) improved conversion by **9.13pp** (25.98% vs 16.86%, p < 0.0001). Conversion rate is defined
as the number of users who complete their first transaction. 
- The conversion lift improved across every tested segment, i.e., country, device type, and acquisition channel
- Treatment improved notification CTR by **6.61pp**
- Treatment subscribers churned **6.72pp less** than control (34.4% vs 41.1%, p = 0.0016)
- The premium plan's churn reduced for treatment, but it was not statistically significant
- 2 per-user metrics were tested (MRR and LTV), and they showed no significant difference for treatment and control. 
Hence, the new onboarding flow's benefit lies in converting and retaining more users, not in higher
revenue per user

### Repo Structure
- **dbt_saas_ab_analytics/** - dbt project containing all models, tests, and documentation
- **scripts/** - Python scripts for generating data and loading data to BigQuery
- **data/** - Input data and the transformed data
- **analysis/** - Python scripts and Jupyter Notebooks containing the statistical analysis and A/B tests
- **README.md** - project overview

### Data Notes
- This project uses synthetic data, as highly detailed public datasets with real product
A/B test experiment data are not readily available. The data was designed to be as realistic as possible:

  - Experiment assignment uses deterministic hashing
  - There are around 2% null values for device and acquisition channel, and around 1% duplicate event rows

- The data contains a mild sample ratio mismatch (52 control/48 treatment), which is statistically significant. However, as the magnitude is 
4 percentage points, it isn't as extreme and I continued with 
the A/B tests. It is worth looking into and fixing this distribution of users to each variant in the
future.
