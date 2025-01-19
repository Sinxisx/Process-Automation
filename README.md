# Process Automation
The main goal of this project is to automate all existing dashboards and BAU analytics processes.
There are several main processes that are to be automated:
1. Daily Funding ✅
2. Daily Lending [on progress]
3. LoB Customer Portfolio [not started]
4. SLIK (Sistem Layanan Informasi Keuangan) [not started]
5. Sales Baseline & Upline [not started]

## 1. Daily Funding
This process extracts bankwide funding data from database (Oracle SQL) and combine it with multiple manual data to create daily funding dashboard.
Currently it processes Non-Retail LoB data. Retail data is to be added.
The dashboard is made using Looker Studio with data source from BigQuery.
There are 2 separate dashboards with the same layout and content but with different user:
- Sales Dashboard: For sales team to monitor their customers funding balance, changes, potential leakage, or other opportunities.
- Management Dashboard: For head office team to monitor area, regional, LoB, or bankwide funding performance such as balance, CASA ratio, Cost of Fund, etc.\
The dashboard layout is follows:
![Non_Retail_Product_-_Management_Dashboard conv 1](https://github.com/user-attachments/assets/812d549d-28bb-402c-a6d2-1f8cafaeecaf)

## 2. Daily Lending
TBA
