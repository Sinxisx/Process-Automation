-- Product holding data viewtable for Looker Studio
WITH
  PH AS(
  SELECT
    GCIF_NO,
    Trade AS TRADE,
    M2E,
    FX,
    Payroll AS PAYROLL,
    Cash_Pickup___Delivery AS CPU,
    Virtual_Account AS VA,
    QRIS,
    Coolpay AS COOLPAY,
    LOB,
    Metric AS METRIC,
    TOTAL AS PH_TOTAL,
    Active_Inactive AS Activeness
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.ph_master`),
  TBL_BAL AS(
  SELECT
    DISTINCT GCIF_NO,
    REGION,
    AREA,
    BRANCH,
    DIVISION,
    SEGMENT_FIX,
    LOB_SORT,
    BASE_AMT_FIX,
    CASE
      WHEN SEGMENT_FIX = 'GB-CORP'THEN 'GB-CORPORATE BANKING'
      ELSE 'CFS NON-RETAIL'
  END
    AS DIRECTORATE,
    CASE
      WHEN COLT = 1 THEN 'YES'
      ELSE 'NO'
  END
    AS BtB,
    CASE
      WHEN MtD < -10000000000 THEN "YES"
      ELSE "NO"
  END
    AS MtD10B,
    CASE
      WHEN DtD < -10000000000 THEN "YES"
      ELSE "NO"
  END
    AS DtD10B
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.TBL_BAL_MASTER_Partitioned`
  WHERE
    SOURCE = "TBL_BAL"
    AND REGION IS NOT NULL
    AND BASE_DT = (
    SELECT
      MAX(BASE_DT)
    FROM
      `maybank-analytics-production.FUNDING_BANKWIDE.TBL_BAL_MASTER_Partitioned`
    WHERE
      SOURCE = 'TBL_BAL')
  ORDER BY
    BASE_AMT_FIX DESC),
  LF AS (
  SELECT
    GCIF AS GCIF_NO,
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
  BASELINE_DUMMY AS (
  SELECT
    GCIF_NO,
    EMAIL_KARYAWAN
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.BASELINE_FUNDING` ),
  UPLINE_DUMMY AS (
  SELECT
    GCIF_NO,
    SALES_NAME,
    LINE_MANAGER_1,
    LINE_MANAGER_2,
    LINE_MANAGER_3,
    LINE_MANAGER_4
  FROM
    `maybank-analytics-production.FUNDING_BANKWIDE.UPLINE_FUNDING`),
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
  FINAL AS (
  SELECT
    *
  FROM
    TBL_BAL
  LEFT JOIN
    PH
  USING
    (GCIF_NO)
  LEFT JOIN
    LF
  USING
    (GCIF_NO)
  LEFT JOIN
    FDLD
  USING
    (GCIF_NO)
  LEFT JOIN
    UPLINE_DUMMY
  USING
    (GCIF_NO)),
  FIN_2 AS (
  SELECT
    * EXCEPT (FL_TEMP),
    CASE
      WHEN FL_TEMP = 'LENDING' THEN 'LENDING'
      ELSE 'FUNDING'
  END
    AS FL_FIN
  FROM
    FINAL),
  FIN3 AS (
  SELECT
    * EXCEPT(row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY GCIF_NO ORDER BY BASE_AMT_FIX DESC) row_num
    FROM
      FIN_2) t
  WHERE
    row_num=1)
SELECT
  * EXCEPT (REGION_MONTHLY),
  CASE
    WHEN REGION_MONTHLY.REGION_MONTHLY IS NULL THEN FIN3.REGION
    ELSE REGION_MONTHLY.REGION_MONTHLY
END
  AS REGION_MONTHLY
FROM
  FIN3
LEFT JOIN
  BASELINE_DUMMY
USING
  (GCIF_NO)
LEFT JOIN
  REGION_MONTHLY
USING
  (GCIF_NO)