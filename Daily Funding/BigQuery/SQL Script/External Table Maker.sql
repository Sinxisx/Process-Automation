-- This script is used to create external table for daily data upload
-- Remove the baseline joining for management version
CREATE OR REPLACE EXTERNAL TABLE `maybank-analytics-production.FUNDING_BANKWIDE.DAILY_FUNDING_EXTERNAL`
(
    BASE_DT FLOAT64,
    BASE_DT_PARSED DATE,
    BASE_YM STRING,
    AGREE_ID STRING,
    FLAG STRING,
    ACCT_NO STRING, 
    REGION STRING, 
    AREA STRING, 
    BRANCH STRING, 
    GCIF_NO STRING, 
    CIF_NO STRING, 
    CUST_TYPE STRING, 
    PROD_NM STRING, 
    SUB_PROD_NM STRING, 
    SEGMENT STRING, 
    GCIF_NAME STRING, 
    PROD_TYPE STRING, 
    CURR_CODE STRING, 
    COLT FLOAT64, 
    RATE_DPK FLOAT64, 
    BASE_AMT_FIX FLOAT64,
    MTD_AVG_AMT_FIX FLOAT64,
    DTD FLOAT64, 
    MTD FLOAT64, 
    YTD FLOAT64, 
    DIVISION STRING, 
    `SOURCE` STRING, 
    SEGMENT_FIX STRING, 
    BASE_AMT_ACCUM_MTD FLOAT64,
    INT_EXP_ACCUM_MTD FLOAT64,
    COF_MTD FLOAT64, 
    HIGH_COF_FLAG STRING, 
    LOB_SORT FLOAT64, 
    CASA_TD STRING,
    DTD_10B STRING, 
    MTD_10B STRING, 
    BLOCK INT64
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://maybank-analytics-data/Daily_NR/Funding/*.csv'],
    skip_leading_rows = 1
)