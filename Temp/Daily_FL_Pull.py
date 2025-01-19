# %%
import paramiko
import pandas as pd
from io import BytesIO 
import oracledb
import pandas as pd
from sqlalchemy import create_engine
import os
import logging
from datetime import datetime

# %% [markdown]
# # Setup Log

# %%
log_file = f'Logs/DailyPuller_{datetime.now().strftime('%Y-%m-%d')}.log'

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
# # A. XPOSE Fasilitas

# %%
# server credentials
hostname = '10.220.42.38'
port = 22
username = os.environ['UID']
password = os.environ['UPW']

# %%
# create ssh client
ssh = paramiko.SSHClient()

# %%
# load system ssh keys
ssh.load_system_host_keys()

# %%
# add missing host keys automaticallly
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)

# %%
try:
    ssh.connect(hostname, port=port, username=username, password=password)
    sftp = ssh.open_sftp()
    remote_file_path = '/CLS/FASILITAS.DAT'
    with sftp.file(remote_file_path, 'r') as remote_file:
        file_data = remote_file.read()
    csv_file = BytesIO(file_data)
    Fasilitas = pd.read_csv(csv_file,sep='|', header=None,low_memory=False)
    FDate = Fasilitas.iloc[0,0].astype(int)
    sv_Fasilitas = f'E:/Pricing_Non_Retail/Christ/xpose/FASILITAS_{FDate}.DAT'
    Fasilitas.to_csv(sv_Fasilitas, sep="|", index=False)
    sftp.close()
    ssh.close()
    logging.info('Facility done.')
except:
    logging.error('SFTP login failed!')     
    pass


# %% [markdown]
# # Master Funding

# %%
# credentials
username = os.environ['DBA_USR']
password = os.environ['DBA_PW']
dsn = '10.220.50.121:1661/ANALYTIC'

# %%
# create connection
try:
    connection = oracledb.connect(user=username, password=password, dsn=dsn)
# %%
# create engine
    engine = create_engine('oracle+oracledb://', creator=lambda: connection)
except Exception as e:
    logging.error(e)
    pass
# %%
# open and read the file as a single buffer
fd = open('test_db.sql', 'r')
sqlFile = fd.read()
fd.close()

# %%
query = """SELECT DISTINCT BASE_DT, AGREE_ID, FLAG, ACCT_NO, ACCT_BR, REGION, AREA, BRANCH, GCIF_NO, CIF_NO,
SUB_PROD_NM, SEGMENT, GCIF_NAME, PROD_TYPE, CURR_CODE, RATE_DPK, BASE_AMOUNT, NPK_SALES, COLT
FROM PDA.MASTER_FUNDING
WHERE ((agree_id LIKE 'ST%' AND STATUS NOT IN ('03','04','05') AND system_type LIKE '8%' AND ctrl3 <>'797')
OR (agree_id like 'IM%' and status not in ('04','05','08') and system_type like '8%' and ctrl3 <>'797' ))
AND NOT SEGMENT = 'CFS-RB-5MB' 
AND BASE_DT=(SELECT MAX(BASE_DT) FROM PDA.MASTER_FUNDING)"""
# (SELECT MAX(BASE_DT) FROM PDA.MASTER_FUNDING)

# %%
try:
    MF = pd.read_sql(query, engine)
except Exception as e:
        logging.error(e)
        pass

# %%
MFDate = MF.iloc[0,0]

# %%
sv_MF = f'E:/Pricing_Non_Retail/Ghazi/Master Funding/MASTER_FUNDING_{MFDate}.csv'

# %%
MF.to_csv(sv_MF, sep=",", index=False)

# %%
logging.info('Master funding done.')


