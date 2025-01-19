-- Main data viewtable for Looker Studio
-- Remove the baseline joining for management version
WITH
  DASHBOARD AS (
  SELECT
    BASE_DT,
    BASE_DT_PARSED,
    BASE_YM,
    AGREE_ID,
    FLAG,
    ACCT_NO,
    REGION,
    AREA,
    BRANCH,
    GCIF_NO,
    CIF_NO,
    CUST_TYPE,
    PROD_NM,
    SUB_PROD_NM,
    SEGMENT,
    GCIF_NAME,
    PROD_TYPE,
    CURR_CODE,
    COLT,
    RATE_DPK,
    BASE_AMT_FIX,
    (RATE_DPK*BASE_AMT_FIX) AS INT_EXPENSE_YEARLY,
    MTD_AVG_AMT_FIX,
    DTD,
    MTD,
    YTD,
    DIVISION,
    SOURCE,
    SEGMENT_FIX,
    BASE_AMT_ACCUM_MTD,
    INT_EXP_ACCUM_MTD,
    COF_MTD,
    HIGH_COF_FLAG,
    LOB_SORT,
    CASE
      WHEN MtD < -10000000000 THEN "YES"
      ELSE "NO"
  END
    AS MtD10B,
    CASE
      WHEN DtD < -10000000000 THEN "YES"
      ELSE "NO"
  END
    AS DtD10B,
    CASE
      WHEN SEGMENT_FIX = 'GB-CORP'THEN 'GB-CORPORATE BANKING'
      ELSE 'CFS NON-RETAIL'
  END
    AS DIRECTORATE,
    CASE
      WHEN COLT = '1' THEN 'YES'
      ELSE 'NO'
  END
    AS BtB
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.MASTER_FUNDING_NR`
  WHERE
    BASE_DT IN (
    SELECT
      MAX(BASE_DT) AS BASE_DT
    FROM
      `maybank-analytics-production.FUNDING_BANKWIDE.MASTER_FUNDING_NR`
    GROUP BY
      BASE_YM )),
  BASELINE_FUNDING AS (
  SELECT
    DISTINCT GCIF_NO AS BASELINE_GCIF,
    EMAIL_KARYAWAN
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.BASELINE_FUNDING` ),
  UPLINE_FUNDING AS (
  SELECT
    GCIF_NO AS UPLINE_GCIF,
    SALES_NAME,
    LINE_MANAGER_1,
    LINE_MANAGER_2,
    LINE_MANAGER_3,
    LINE_MANAGER_4
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.UPLINE_FUNDING`),
  UPLINE_GB_TB AS (
  SELECT
    GCIF AS UPLINE_GB_GCIF,
    TB_SALES_NAME,
    TB_TEAM_LEADER,
    TB_HEAD
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.UPLINE_GB_TB`),
  LF AS (
  SELECT
    GCIF,
    MTD_DCNT_M12,
    MAX_BAL,
    FINAL_FLAG,
    CASE
      WHEN WINBACK = TRUE THEN 'YES'
      ELSE 'NO'
  END
    AS WB_FLAG
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.LeakageFlag` ),
  FDLD AS (
  SELECT
    DISTINCT GCIF_NO,
    'LENDING' AS FL_TEMP
  FROM
    `maybank-analytics-production.CPR.Master_Lending_Partitioned`
  WHERE
    BASE_DT = (
    SELECT
      MAX(BASE_DT)
    FROM
      `maybank-analytics-production.CPR.Master_Lending_Partitioned`)),
  CASA_AGG AS (
  SELECT
    GCIF_NO,
    SUM(BASE_AMT_FIX) AS CASA_AMT
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.MASTER_FUNDING_NR`
  WHERE
    PROD_TYPE IN ('CA',
      'SA')
    AND SOURCE = 'TBL_BAL'
    AND BASE_DT = (
    SELECT
      MAX(BASE_DT)
    FROM
      `maybank-analytics-production.FUNDING_BANKWIDE.MASTER_FUNDING_NR`
    WHERE
      SOURCE = 'TBL_BAL')
  GROUP BY
    GCIF_NO
  ORDER BY
    GCIF_NO),
  CASA_BKT AS(
  SELECT
    *,
    CASE
      WHEN CASA_AMT < 1000000 THEN '<1 mio'
      WHEN CASA_AMT >= 1000000
    AND CASA_AMT <= 25000000 THEN '≥1 mio - ≤25 mio'
      WHEN CASA_AMT > 25000000 AND CASA_AMT <= 100000000 THEN '>25 mio - ≤100 mio'
      WHEN CASA_AMT > 100000000
    AND CASA_AMT <= 500000000 THEN '>100 mio - ≤500 mio'
      WHEN CASA_AMT > 500000000 AND CASA_AMT <= 5000000000 THEN '>500 mio - ≤5 bio'
      WHEN CASA_AMT > 5000000000
    AND CASA_AMT <= 25000000000 THEN '>5 bio - ≤25 bio'
      WHEN CASA_AMT > 25000000000 AND CASA_AMT <= 100000000000 THEN '>25 bio - ≤100 bio'
      WHEN CASA_AMT > 100000000000 THEN '>100 bio'
  END
    AS CASA_BUCKET,
    CASE
      WHEN CASA_AMT < 1000000 THEN 1
      WHEN CASA_AMT >= 1000000
    AND CASA_AMT <= 25000000 THEN 2
      WHEN CASA_AMT > 25000000 AND CASA_AMT <= 100000000 THEN 3
      WHEN CASA_AMT > 100000000
    AND CASA_AMT <= 500000000 THEN 4
      WHEN CASA_AMT > 500000000 AND CASA_AMT <= 5000000000 THEN 5
      WHEN CASA_AMT > 5000000000
    AND CASA_AMT <= 25000000000 THEN 6
      WHEN CASA_AMT > 25000000000 AND CASA_AMT <= 100000000000 THEN 7
      WHEN CASA_AMT > 100000000000 THEN 8
  END
    AS CASA_NUM
  FROM
    CASA_AGG),
  PH AS (
  SELECT
    GCIF_NO,
  IF
    (Trade=1,'Yes','No') AS Trade_YN,
  IF
    (M2E=1,'Yes','No') AS M2E_YN,
  IF
    (FX=1,'Yes','No') AS FX_YN,
  IF
    (Payroll=1,'Yes','No') AS Payroll_YN,
  IF
    (Cash_Pickup___Delivery=1,'Yes','No') AS CPU_YN,
  IF
    (Virtual_Account=1,'Yes','No') AS VA_YN,
  IF
    (QRIS=1,'Yes','No') AS QRIS_YN,
  IF
    (Coolpay=1,'Yes','No') AS Coolpay_YN,
    LOB,
    TOTAL,
    Active_Inactive
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.ph_master` ),
  REGION_MONTHLY AS (
  SELECT
    GCIF AS GCIF_NO,
    REGION AS REGION_MONTHLY
  FROM
    `maybank-analytics-production.PDT_RSME.RSME_Master_Dashboard_Partitioned`
  WHERE
    Month = (
    SELECT
      MAX(Month)
    FROM
      `maybank-analytics-production.PDT_RSME.RSME_Master_Dashboard_Partitioned`)
  UNION ALL
  SELECT
    GCIF AS GCIF_NO,
    REGION AS REGION_MONTHLY
  FROM
    `maybank-analytics-production.PDT_SMEPlus.SMEPlus_Master_Dashboard_Partitioned`
  WHERE
    Month = (
    SELECT
      MAX(Month)
    FROM
      `maybank-analytics-production.PDT_SMEPlus.SMEPlus_Master_Dashboard_Partitioned`)
  UNION ALL
  SELECT
    GCIF AS GCIF_NO,
    REGION AS REGION_MONTHLY
  FROM
    `maybank-analytics-production.PDT_BB.BB_Master_Dashboard_Partitioned`
  WHERE
    Month = (
    SELECT
      MAX(Month)
    FROM
      `maybank-analytics-production.PDT_BB.BB_Master_Dashboard_Partitioned`) ),
  REGION_MONTHLY_2 AS (
  SELECT
    DISTINCT *
  FROM
    REGION_MONTHLY),
  FINAL AS (
  SELECT
    *
  FROM
    BASELINE_FUNDING
  LEFT JOIN
    DASHBOARD
  ON
    DASHBOARD.GCIF_NO = BASELINE_FUNDING.BASELINE_GCIF
  LEFT JOIN
    UPLINE_FUNDING
  ON
    BASELINE_FUNDING.BASELINE_GCIF = UPLINE_FUNDING.UPLINE_GCIF
  LEFT JOIN
    UPLINE_GB_TB
  ON
    BASELINE_FUNDING.BASELINE_GCIF = UPLINE_GB_TB.UPLINE_GB_GCIF
  LEFT JOIN
    LF
  ON
    DASHBOARD.GCIF_NO = LF.GCIF
  LEFT JOIN
    FDLD
  USING
    (GCIF_NO)
  LEFT JOIN
    CASA_BKT
  USING
    (GCIF_NO)
  LEFT JOIN
    PH
  USING
    (GCIF_NO))
SELECT
  * EXCEPT (FL_TEMP,
    REGION_MONTHLY),
  CASE
    WHEN FL_TEMP = 'LENDING' THEN 'LENDING'
    ELSE 'FUNDING'
END
  AS FL_FIN,
  CASE
    WHEN REGION_MONTHLY_2.REGION_MONTHLY IS NULL THEN FINAL.REGION
    ELSE REGION_MONTHLY_2.REGION_MONTHLY
END
  AS REGION_MONTHLY
FROM
  FINAL
LEFT JOIN
  REGION_MONTHLY_2
USING
  (GCIF_NO)