''' want to transform the log file into a csv file  and aggregate the users data'''

import logging
import pandas as pd
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s :%(levelname)s: %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

logger=logging.getLogger(__name__)

logger.info('starting log file etl process')

def log_file_etl(logfile):
    logger.info(f'reading log file {logfile}')
    with open(logfile,'r') as f:
        lines=f.readlines()

    print(lines[1].split(':'))
    user_data=[i.split(':')[-1] for i in lines ]
    user_data_split=[i.split('-') for i in user_data ]
    
    df=pd.DataFrame(user_data_split,columns=['user','action','mail'])
    df['mail']=df['mail'].str.strip('\n')
    logger.info(f"sample user record :{df.head()}")

    df_agg=df.pivot_table(index=['user','mail'],columns='action',aggfunc='size').reset_index()
    logger.info(f"sample user record :{df_agg.head()}")

    df_agg.to_csv(f'{os.getcwd()}\\data\\user_action.csv',index=False)
    logger.info(f"etl process completed")