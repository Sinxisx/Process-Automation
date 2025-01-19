# %%
import pandas as pd
import numpy as np
import oracledb
from sqlalchemy import create_engine
import os
import glob
import paramiko
import io
import datetime
import logging
pd.set_option('display.max_columns',30)

# %%
log_file = f'Logs/MF_{datetime.datetime.now().strftime('%Y-%m-%d')}.log'

# %%
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, mode='a'),
        logging.StreamHandler()
    ]
)

# %% [markdown]
# # A. Load Data

# %%
logging.info('Processing Start')

# %%
# setup date for querying
# last month
dataDt = datetime.date.today()-datetime.timedelta(days=1)
firstDate = dataDt.replace(day=1)
lastMonth = firstDate - datetime.timedelta(days=1)
lastMonth = lastMonth.strftime("%Y%m")

#last year
firstMonth = dataDt.replace(month=1, day=1)
lastYear = firstMonth - datetime.timedelta(days=1)
lastYear = lastYear.strftime("%Y%m")

# curr monthj
currMonth = firstDate.strftime("%Y%m")

# %% [markdown]
# ## 1. DBA: Master Funding & MTD Aggregate

# %% [markdown]
# ### a. Master Funding

# %%
# credentials
usernameDba = os.environ['UID']
passwordDba = os.environ['DBAPW']
dsnDba = '10.220.50.121:1661/ANALYTIC'

# %%
try:
    # create connection
    connection = oracledb.connect(user=usernameDba, password=passwordDba, dsn=dsnDba)

    # create engine
    engine = create_engine('oracle+oracledb://', creator=lambda: connection)

    # open and read the file as a single buffer
    mfQuery = open(r'MF.sql', 'r')
    sqlFile = mfQuery.read()
    # sqlFile.format(LM=lastMonth,LY=lastYear,CM=currMonth)
    mfQuery.close()

    # run query and store data in df (avgtime 2 min)
    logging.info('Running SQL Query')
    MF = pd.read_sql(sqlFile, engine)
    logging.info('SQL Query Finished')
except Exception as e:
    logging.error("Exception occurred", exc_info=True)

# %%
# change column names into UPPER CASE
MF.columns = [x.upper() for x in MF.columns]

# %%
# create source column for MASTER FUNDING data (TBL_BAL)
MF['SOURCE'] = 'TBL_BAL'

# %%
# convert BASE_DT to int for compatibility reasons
MF['BASE_DT'] = MF['BASE_DT'].astype(int)

# convert STATUS, SYSTEM_TYPE, and CTRL3 to string for compatibility
MF['STATUS'] = MF['STATUS'].astype(str)
MF['SYSTEM_TYPE'] = MF['SYSTEM_TYPE'].astype(str)
MF['CTRL3'] = MF['CTRL3'].astype(str)

# %%
MF.info()

# %%
MF[['BASE_AMT_FIX','MTD','YTD']].describe()

# %%
MF.columns

# %% [markdown]
# ### b. Preprocess Bank Entities

# %%
# get working directory
path = os.getcwd()
parDir = path

# %%
# read bank entities GCIF as listt
BE = pd.read_excel(parDir+'\\Bank List\\Bank_Lists.xlsx', sheet_name='Sheet1')
BE = BE['GCIF_NO'].drop_duplicates().tolist()

# %%
# get bank entities funding data
MFBank = MF[MF['GCIF_NO'].isin(BE)]
MFBank = MFBank[MFBank['SEGMENT'].str.contains('GB')]

# %%
# filter all MF to include only active and proper accounts
MF = MF[
    ((MF['AGREE_ID'].str.startswith('ST')) &
    (~MF['STATUS'].isin(['03','04','05'])) &
    (MF['SYSTEM_TYPE'].str.startswith('8')) &
    (MF['CTRL3']!= '797')) |
    ((MF['AGREE_ID'].str.startswith('IM')) &
    (~MF['STATUS'].isin(['04','05','08'])) &
    (MF['SYSTEM_TYPE'].str.startswith('8')) &
    (MF['CTRL3']!= '797'))
]

# %%
# concat bank data and cleansed master funding
MFFin = pd.concat([MF,MFBank], ignore_index=True)

# %%
MFFin.info()

# %% [markdown]
# ## 2. Local Files

# %%
def get_last_sunday(year_month):
    # Parse the input year_month in YYYYMM format
    year = int(year_month[:4])
    month = int(year_month[4:6])

        # Get the first day of the next month
    if month == 12:  # Handle December case
        first_day_next_month = datetime.datetime(year + 1, 1, 1)
    else:
        first_day_next_month = datetime.datetime(year, month + 1, 1)

    # Get the last day of the current month
    last_day_of_month = first_day_next_month - datetime.timedelta(days=1)

    # Calculate the number of days to the previous Sunday
    days_to_sunday = last_day_of_month.weekday()

    # Get the last Sunday date
    last_sunday = last_day_of_month - datetime.timedelta(days=days_to_sunday)

    # Return the date in YYYYMMDD format
    return last_sunday.strftime('%Y%m%d')

# %%
RMR_Target= pd.DataFrame(columns=['BASE_DT','BASE_YM','FLAG','SEGMENT',
                                  'PROD_TYPE','BASE_AMT_FIX','SOURCE'])

# %%
# RMR NR Dir
RMR_NR_Dir = parDir+'\\RMR\\Non-Retail\\'
RMR_NR_Files = list(filter(os.path.isfile, glob.glob(RMR_NR_Dir + '*')))
RMR_NR_Files.sort(key=lambda x:os.path.getmtime(x))
RMR_NR_Files = [x for x in RMR_NR_Files if '~' not in x]

# %%
RMR_NR_Files

# %%
RMR_GB_Dir = parDir+'\\RMR\\GB\\'
RMR_GB_Files = list(filter(os.path.isfile, glob.glob(RMR_GB_Dir + '*')))
RMR_GB_Files.sort(key=lambda x:os.path.getmtime(x))
RMR_GB_Files = [x for x in RMR_GB_Files if '~' not in x]

# %%
RMR_GB_Files

# %% [markdown]
# ## a. RMR Non_Retail

# %%
# format data into table as required
RMRSheetNames = ['RSME','BB','SME+','Micro']
Segments = ['CFS-NONRB-RSME','CFS-NONRB-BB',
            'CFS-NONRB-SME+','CFS-NONRB-MICRO']
prodTypes = ['CA','SA','TD']
Flags = ['Conven','Sharia']
Kinds = ['RMR','TARGET']
rowResult = 0
for i in range(4): # for every sheet/segment
    data = pd.read_excel(RMR_NR_Files[-1],
                     sheet_name=RMRSheetNames[i],
                     usecols='F,H',
                     skiprows=4,
                     nrows=28)
    BASE_DT = get_last_sunday(data.iloc[0,0].strftime('%Y%m%d'))
    for j in range(2): # for every kind (RMR/Target)
        col = j
        for k in range(2): # for every flag (Conven/Sharia)
            row  = 21 if k==0 else 25
            for l in range(3): # for every prod type (CA/SA/TD)
                RMR_Target.loc[rowResult,'BASE_DT'] = BASE_DT
                RMR_Target.loc[rowResult,'BASE_YM'] = BASE_DT[:6]
                RMR_Target.loc[rowResult,'FLAG'] = Flags[k]
                RMR_Target.loc[rowResult,'SEGMENT'] = Segments[i]
                RMR_Target.loc[rowResult,'PROD_TYPE'] = prodTypes[l]
                RMR_Target.loc[rowResult,'BASE_AMT_FIX'] = data.iloc[row,col]*1000000
                RMR_Target.loc[rowResult,'SOURCE'] = Kinds[j]
                row+=1
                rowResult+=1


# %%
RMR_Target.shape

# %% [markdown]
# ## b. RMR GB

# %%
# format data into table as required
rowResult = 48
data = pd.read_excel(RMR_GB_Files[-1],
                sheet_name='Corp',
                usecols='F,H',
                skiprows=4,
                nrows=28)
BASE_DT = get_last_sunday(data.iloc[0,0].strftime('%Y%m%d'))
for j in range(2): # for every kind (RMR/Target)
    col = j
    for k in range(2): # for every flag (Conven/Sharia)
        row  = 21 if k==0 else 25
        for l in range(3): # for every prod type (CA/SA/TD)
            RMR_Target.loc[rowResult,'BASE_DT'] = BASE_DT
            RMR_Target.loc[rowResult,'BASE_YM'] = BASE_DT[:6]
            RMR_Target.loc[rowResult,'FLAG'] = Flags[k]
            RMR_Target.loc[rowResult,'SEGMENT'] = 'XXX-GB-CORP'
            RMR_Target.loc[rowResult,'PROD_TYPE'] = prodTypes[l]
            RMR_Target.loc[rowResult,'BASE_AMT_FIX'] = data.iloc[row,col]*1000000
            RMR_Target.loc[rowResult,'SOURCE'] = Kinds[j]
            row+=1
            rowResult+=1

# %%
RMR_Target.shape

# %%
RMR_Target['BASE_AMT_FIX'] = RMR_Target['BASE_AMT_FIX'].astype(float)

# %%
RMR_Target['BASE_DT_PARSED'] = pd.to_datetime(RMR_Target['BASE_DT'])

# %%
RMR_Target[['BASE_DT','BASE_YM','FLAG','SEGMENT','PROD_TYPE','BASE_AMT_FIX','SOURCE']]

# %% [markdown]
# # B. Processing

# %% [markdown]
# ## 1. MF Aggregate

# %%
MF.columns

# %%
# aggregate columns
MF_Agg = MF.groupby(['BASE_DT', 'BASE_YM', 'FLAG', 'SEGMENT', 'SEGMENT_FIX', 'PROD_TYPE', 'DIVISION'], dropna=False)['BASE_AMT_FIX'].agg(BASE_AMT_FIX = 'sum')
MF_Agg.reset_index(inplace=True)

# %%
MF_Agg.info()

# %%
MF_Agg['SOURCE'] = 'TBL_BAL_SUMMARY'

# %%
MF_All = pd.concat([MFFin,MF_Agg], ignore_index=True)
MF_All['BASE_DT'] = MF_All['BASE_DT'].astype(int)
MF_All['BASE_YM'] = MF_All['BASE_YM'].astype(int)

# %%
MF_All.info()

# %% [markdown]
# ## 2. RMR & Target

# %% [markdown]
# DB data source unavailable. Requires manual file update.

# %%
# create RMR container with identical columns to MF
RMRContainer = MF.iloc[0:0]

# %%
# Create Lob Mapping
lobMap = pd.DataFrame({'SEGMENT':["CFS-NONRB-MICRO", "CFS-NONRB-RSME", "CFS-NONRB-SME+", "CFS-NONRB-BB", "XXX-GB-CORP"],
                       'SEGMENT_FIX':["CFS-SMER & MICRO","CFS-SMER & MICRO", "CFS-SME+", "CFS-BB", "GB-CORP"]})

# %%
# merge LoB Mapping
RMR_Target = RMR_Target.merge(lobMap, on='SEGMENT', how='left')

# %%
# concat data to empty df
RMR_Target = pd.concat([RMRContainer, RMR_Target])

# %% [markdown]
# # C. Masking

# %%
# container for masked acct and agr
acct_fix = []
agr_fix = []

# mask acct and agr
for i in list(MF_All.index):
    agree = str(MF_All.loc[i,'AGREE_ID'])
    a = agree[:-7] + "XXXX" + agree[-3:]
    acct = str(MF_All.loc[i, "ACCT_NO"])
    b = acct[:-7] + "XXXX" + acct[-3:]
    agr_fix.append(a)
    acct_fix.append(b)

# replace original field with masked values
MF_All['AGREE_ID'] = agr_fix
MF_All['ACCT_NO'] = acct_fix

# %%
# name masking function
def masking_name(param):
    if param not in ["nan","NaN"]:
        return  ' '.join([item.replace(item[1:-1], "*"*len(item[1:-1])) if len(item)>2 and item[-1]!="," and item[-1]!="." else (item.replace(item[1:-2], "*"*len(item[1:-2])) if len(item)>2 and (item[-1]==",") else (item if len(item)>2 and (item[-1]==".") else item.replace(item[1:-2], "*"*len(item[1:-2])))) for item in param.split()])
    else:
        return param

# %%
# mask names
MF_All["GCIF_NAME"] = MF_All["GCIF_NAME"].astype(str)
MF_All["GCIF_NAME"] = MF_All["GCIF_NAME"].apply(masking_name) 

# %%
MF_All.columns

# %%
# Assign MF as currfin with selected columns
currFin = MF_All[['BASE_DT', 'BASE_DT_PARSED', 'BASE_YM', 'AGREE_ID', 'FLAG', 'ACCT_NO',
       'REGION', 'AREA', 'BRANCH', 'GCIF_NO', 'CIF_NO', 'CUST_TYPE', 'PROD_NM',
       'SUB_PROD_NM', 'SEGMENT', 'GCIF_NAME', 'PROD_TYPE', 'CURR_CODE',
       'COLT', 'RATE_DPK', 'BASE_AMT_FIX', 'MTD_AVG_AMT_FIX',
       'DTD', 'MTD', 'YTD', 'DIVISION', 'SOURCE', 'SEGMENT_FIX',
       'BASE_AMT_ACCUM_MTD', 'INT_EXP_ACCUM_MTD',
       'COF_MTD', 'HIGH_COF_FLAG', 'LOB_SORT', 'CASA_TD', 'DTD_10B',
       'MTD_10B','BLOCK']]

# %%
# Assign to RMR and Target with selected columns
RMR_Target = RMR_Target[['BASE_DT', 'BASE_DT_PARSED', 'BASE_YM', 'AGREE_ID', 'FLAG', 'ACCT_NO',
       'REGION', 'AREA', 'BRANCH', 'GCIF_NO', 'CIF_NO', 'CUST_TYPE', 'PROD_NM',
       'SUB_PROD_NM', 'SEGMENT', 'GCIF_NAME', 'PROD_TYPE', 'CURR_CODE',
       'COLT', 'RATE_DPK', 'BASE_AMT_FIX', 'MTD_AVG_AMT_FIX',
       'DTD', 'MTD', 'YTD', 'DIVISION', 'SOURCE', 'SEGMENT_FIX',
       'BASE_AMT_ACCUM_MTD', 'INT_EXP_ACCUM_MTD',
       'COF_MTD', 'HIGH_COF_FLAG', 'LOB_SORT', 'CASA_TD', 'DTD_10B',
       'MTD_10B','BLOCK']]

# %%
# remove possible duplicates
print(currFin.duplicated().sum())
currFin.drop_duplicates(inplace=True)

# %%
currFin.head()

# %%
# Data date
MF_date = currFin.loc[0,'BASE_DT']
RMR_Target_Date = RMR_Target.loc[0,'BASE_YM']

# %%
currFin.info()

# %% [markdown]
# # D. RMR & Target Append

# %%
# Load YMKeeper
YMRecordPrev = pd.read_csv(parDir+'/Keeper/YMKeeper.csv')
RMRTargetRecordYM = YMRecordPrev.loc[0,'RMRTarget_YM']

# %%
# concat RMR Target if current YM > saved YM
if int(RMR_Target.loc[0,'BASE_YM']) > RMRTargetRecordYM:
    currFin = pd.concat([currFin,RMR_Target],ignore_index=True)
else:
    pass

# %%
currFin['SOURCE'].value_counts()

# %%
# save data
currFin.to_csv(parDir+f'\\Temp_Result\\MF_{MF_date}.csv', index=False)

# %% [markdown]
# # E. Save Result

# %% [markdown]
# ## 1. Connect to SFTP

# %%
# SFTP connection details
hostname = '10.220.42.38'
port = '22'
username = os.environ['UID']
password = os.environ['UPW']

# %% [markdown]
# ## 2. Save to SFTP

# %%
# create dir if not exist
def create_remote_directory(sftp, remote_path):
    """
    Recursively create directories on the SFTP server if they do not exist.
    :param sftp: SFTP session object.
    :param remote_path: Full path of the directory to create.
    """
    dirs = remote_path.split('/')
    current_path = ""
    for dir_name in dirs:
        if dir_name:  # Skip empty parts
            current_path += f"/{dir_name}"
            try:
                sftp.stat(current_path)  # Check if the directory exists
            except FileNotFoundError:
                sftp.mkdir(current_path)  # Create the directory
                print(f"Directory created: {current_path}")

# %%
# sftp hostname and credential
hostnameSFTP = '10.220.42.38'
portSFTP = '22'
usernameSFTP = os.environ['UID']
passwordSFTP = os.environ['UPW']

# %%
# sava dataframe as csv to Memory
csv_buffer = io.StringIO()
currFin.to_csv(csv_buffer, index=False)
csv_buffer.seek(0) # reset buffer position to beginning

# Save path
remote_path = f'/PDA/PNR Automation/Daily Funding/MF_{MF_date}.csv'
remote_directory = '/PDA'

# Estabilish SFTP connection
try:
    # Create the SSH client
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    ssh.connect(hostnameSFTP, port=portSFTP, username=usernameSFTP, password=passwordSFTP)

    # Open SFTP session
    sftp = ssh.open_sftp()

    # Ensure the directory exists
    create_remote_directory(sftp, remote_directory)

    # Write the buffer to the remote file
    with sftp.file(remote_path, 'w') as remote_file:
        remote_file.write(csv_buffer.getvalue())

    logging.info(f'File uploaded successfully to {remote_path}')
except Exception as e:
    logging.error("Exception occurred", exc_info=True)
finally:
    if 'sftp' in locals():
        sftp.close()
    if 'ssh' in locals():
        ssh.close()

# %% [markdown]
# ## 3. Save YM Keeper

# %%
# save RMR and Target YM
YMRecord = pd.DataFrame({'RMRTarget_YM':[RMR_Target.loc[0,'BASE_YM']]})
YMRecord.to_csv(parDir+'/Keeper/YMKeeper.csv', index=False)

# %%
YMRecord

# %%
logging.info('Processing Finished')


