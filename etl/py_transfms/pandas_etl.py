import pandas as pd
import os


def users_agg():
    df=pd.read_csv(f'{os.getcwd()}\\data\\users.csv')
    df['order_date']=pd.to_datetime(df['order_date'])
    df['month']=df['order_date'].dt.month
    df['year']=df['order_date'].dt.year
    df_agg=df.groupby(['year','month','name']).agg({'amount':'sum'}).reset_index().sort_values(by=['year','month','amount'], \
                     ascending=[True,True,False])
    df_agg.to_csv(f'{os.getcwd()}\\data\\users_agg.csv',index=False)


if __name__=='__main__':
    users_agg()
