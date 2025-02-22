WITH
CURRD AS (
    SELECT DISTINCT 
    BASE_DT,
    TO_DATE(TO_CHAR(BASE_DT),'yyyymmdd') BASE_DT_PARSED,
    SUBSTR(BASE_DT,1,6) AS BASE_YM, 
    AGREE_ID,
    CTRL2,
    FLAG, 
    ACCT_NO, 
    ACCT_BR,
    GCIF_CREATE_BR, 
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
    SUB_LOB_CD, 
    RATE_DPK/100 AS RATE_DPK,
    CASE WHEN BASE_AMOUNT < 0 THEN 0 ELSE BASE_AMOUNT END AS BASE_AMT_FIX,
    CASE WHEN MTD_AVG_AMOUNT < 0 THEN 0 ELSE MTD_AVG_AMOUNT END AS MTD_AVG_AMT_FIX,
    MTD_INT_EXP, 
    NPK_SALES, 
    COLT,
    STATUS,
    SYSTEM_TYPE,
    CTRL3
    FROM PDA.MASTER_FUNDING
    WHERE SEGMENT NOT LIKE 'CFS-RB%'
    AND BASE_DT = (SELECT MAX(BASE_DT) FROM PDA.MASTER_FUNDING)
),
PREVD AS (
    SELECT DISTINCT 
    AGREE_ID, 
    CURR_CODE, 
    CASE WHEN BASE_AMOUNT < 0 THEN 0 ELSE BASE_AMOUNT END AS BASE_AMT_FIX_PREVD
    FROM PDA.MASTER_FUNDING
    WHERE SEGMENT NOT LIKE 'CFS-RB%'
    AND BASE_DT = (SELECT MAX(BASE_DT) 
    FROM PDA.MASTER_FUNDING 
    WHERE BASE_DT < (SELECT MAX(BASE_DT) FROM PDA.MASTER_FUNDING))
),
LM AS (
    SELECT DISTINCT MAX(BASE_DT)
    FROM PDA.MASTER_FUNDING
    WHERE BASE_DT 
    LIKE (SELECT CONCAT(SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE((MAX(BASE_DT)),'YYYYMMDD'),-1),'YYYYMMDD'),1,6),'%')FROM CURRD)
),
LY AS (
    SELECT MAX(BASE_DT)
    FROM PDA.MASTER_FUNDING
    WHERE BASE_DT
    LIKE (SELECT CONCAT(TO_CHAR(TRUNC(SYSDATE, 'YEAR') - 1, 'YYYY'),'12%') FROM DUAL)
),
PREVM AS (
    SELECT DISTINCT 
    AGREE_ID, 
    CURR_CODE, 
    CASE WHEN BASE_AMOUNT < 0 THEN 0 ELSE BASE_AMOUNT END AS BASE_AMT_FIX_PREVM
    FROM PDA.MASTER_FUNDING
    WHERE SEGMENT NOT LIKE 'CFS-RB%'
    AND BASE_DT = (SELECT * FROM LM)
),
PREVY AS (
    SELECT DISTINCT 
    AGREE_ID, 
    CURR_CODE, 
    CASE WHEN BASE_AMOUNT < 0 THEN 0 ELSE BASE_AMOUNT END AS BASE_AMT_FIX_PREVY
    FROM PDA.MASTER_FUNDING
    WHERE SEGMENT NOT LIKE 'CFS-RB%'
    AND BASE_DT = (SELECT * FROM LY)
),
MTD_ACCUM AS(
    SELECT 
    AGREE_ID,
    SUM(CASE WHEN BASE_AMOUNT < 0 THEN 0 ELSE BASE_AMOUNT END) BASE_AMT_ACCUM_MTD,
    SUM(CASE WHEN BASE_AMOUNT < 0 THEN 0 ELSE BASE_AMOUNT*RATE_DPK/100 END) INT_EXP_ACCUM_MTD
    FROM PDA.MASTER_FUNDING
    WHERE BASE_DT LIKE (SELECT SUBSTR(MAX(BASE_DT),1,6)||'%' FROM CURRD)
    AND GCIF_NO IN (SELECT DISTINCT GCIF_NO FROM CURRD)
    GROUP BY AGREE_ID
),
MTD AS (
    SELECT 
    AGREE_ID, 
    CASE WHEN BASE_AMT_ACCUM_MTD<=0 THEN 0 ELSE INT_EXP_ACCUM_MTD/BASE_AMT_ACCUM_MTD END AS COF_MTD,
    BASE_AMT_ACCUM_MTD,
    INT_EXP_ACCUM_MTD
    FROM MTD_ACCUM
),
BLOKIR AS(
    SELECT DISTINCT 
    ACCT_NO, 
    CTRL2, 
    '1' AS BLOCK
    FROM PDA.MASTER_HOLD_ACCOUNT
    WHERE BASE_DT = (SELECT MAX(BASE_DT) FROM PDA.MASTER_HOLD_ACCOUNT)
),
MSTR_TBL AS (
    SELECT 
    c.BASE_DT,
    c.BASE_DT_PARSED,
    c.BASE_YM ,
    c.AGREE_ID, 
    c.FLAG, 
    c.ACCT_NO, 
    c.ACCT_BR,
    c.GCIF_CREATE_BR, 
    c.REGION, 
    c.AREA, 
    c.BRANCH, 
    c.GCIF_NO,
    c.CIF_NO, 
    c.CUST_TYPE, 
    c.PROD_NM, 
    c.SUB_PROD_NM, 
    c.SEGMENT,
    c.GCIF_NAME, 
    c.PROD_TYPE, 
    c.CURR_CODE, 
    c.SUB_LOB_CD, 
    c.RATE_DPK, 
    c.BASE_AMT_FIX,
    c.MTD_AVG_AMT_FIX,
    c.MTD_INT_EXP, 
    c.NPK_SALES, 
    c.COLT,
    t.BASE_AMT_ACCUM_MTD,
    t.INT_EXP_ACCUM_MTD,
    t.COF_MTD,
    (NVL(c.BASE_AMT_FIX,0) - NVL(d.BASE_AMT_FIX_PREVD,0)) AS DTD,
    (NVL(c.BASE_AMT_FIX,0) - NVL(m.BASE_AMT_FIX_PREVM,0)) AS MTD,
    (NVL(c.BASE_AMT_FIX,0) - NVL(y.BASE_AMT_FIX_PREVY,0)) AS YTD,
    CASE 
        WHEN c.PROD_TYPE = 'CA' AND c.CURR_CODE = 'IDR' AND t.COF_MTD <= 0.015 AND c.COLT IS NULL THEN 'NORMAL COF (IDR ≤1.5%'|| chr(38) ||'USD ≤0.25%)'
        WHEN c.PROD_TYPE = 'CA' AND c.CURR_CODE = 'IDR' AND t.COF_MTD > 0.015 AND t.COF_MTD <= 0.03 AND c.COLT IS NULL THEN 'MEDIUM COF (IDR 1.5-3.0%'|| chr(38) ||'USD 0.25-1.00%)'
        WHEN c.PROD_TYPE = 'CA' AND c.CURR_CODE = 'IDR' AND t.COF_MTD > 0.03 AND c.COLT IS NULL THEN 'HIGH COF (IDR >3.0%'|| chr(38) ||'USD >1.0%)'
        WHEN c.PROD_TYPE = 'CA' AND c.CURR_CODE = 'USD' AND t.COF_MTD <= 0.0025 AND c.COLT IS NULL THEN 'NORMAL COF (IDR ≤1.5%'|| chr(38) ||'USD ≤0.25%)'
        WHEN c.PROD_TYPE = 'CA' AND c.CURR_CODE = 'USD' AND t.COF_MTD > 0.0025 AND t.COF_MTD <= 0.01 AND c.COLT IS NULL THEN 'MEDIUM COF (IDR 1.5-3.0%'|| chr(38) ||'USD 0.25-1.00%)'
        WHEN c.PROD_TYPE = 'CA' AND c.CURR_CODE = 'USD' AND t.COF_MTD > 0.01 AND c.COLT IS NULL THEN 'HIGH COF (IDR >3.0%'|| chr(38) ||'USD >1.0%)'
        ELSE '' END AS HIGH_COF_FLAG,
    CASE 
        WHEN c.SEGMENT IN ('CFS-NONRB-MICRO', 'CFS-NONRB-RSME') THEN 'CFS-SMER '|| chr(38) ||' MICRO'
        WHEN c.SEGMENT = 'CFS-NONRB-SME+' THEN 'CFS-SME+'
        WHEN c.SEGMENT = 'CFS-NONRB-BB' THEN 'CFS-BB'
        WHEN c.SEGMENT = 'XXX-GB-CORP' THEN 'GB-CORP'
    END AS SEGMENT_FIX,
    CASE 
        WHEN c.SEGMENT IN ('CFS-NONRB-MICRO', 'CFS-NONRB-RSME') THEN 1
        WHEN c.SEGMENT = 'CFS-NONRB-SME+' THEN 2
        WHEN c.SEGMENT = 'CFS-NONRB-BB' THEN 3
        WHEN c.SEGMENT = 'XXX-GB-CORP' THEN 4
    END AS LOB_SORT,
    CASE 
        WHEN c.PROD_TYPE IN ('CA', 'SA') THEN 'CASA'
        WHEN c.PROD_TYPE = 'TD' THEN 'TD'
    END AS CASA_TD,
    CASE 
        WHEN (c.BASE_AMT_FIX - d.BASE_AMT_FIX_PREVD) < -10000000000 THEN 'YES'
        ELSE 'NO'
    END AS DTD_10B,
    CASE 
        WHEN (c.BASE_AMT_FIX - m.BASE_AMT_FIX_PREVM) < -10000000000 THEN 'YES'
        ELSE 'NO'
    END AS MTD_10B,
    CASE 
        WHEN c.SUB_LOB_CD = '01001' THEN 'LLC'
        WHEN c.SUB_LOB_CD = '01002' THEN 'FIG'
        WHEN c.SUB_LOB_CD = '01999' THEN 'SOE'
        ELSE 'UNDEFINED'
    END AS DIVISION,
    c.STATUS,
    c.SYSTEM_TYPE,
    c.CTRL3,
    b.BLOCK
    FROM CURRD c
    LEFT JOIN PREVD d ON c.AGREE_ID = d.AGREE_ID
    LEFT JOIN PREVM m ON c.AGREE_ID = m.AGREE_ID
    LEFT JOIN PREVY y ON c.AGREE_ID = y.AGREE_ID
    LEFT JOIN MTD t ON c.AGREE_ID = t.AGREE_ID
    LEFT JOIN BLOKIR b ON  c.ACCT_NO = b.ACCT_NO AND c.CTRL2=b.CTRL2
)
SELECT * FROM MSTR_TBL