
import pandas as pd
import numpy

def helper(val):
    if val[-2:] == '23':
        return True
    else: return False


df = pd.read_csv("archive/sales_data_full.csv", parse_dates=['invoice_date'])

tmp = df[df['invoice_date'].apply(lambda x: helper(x))]
print(len(tmp))
cid = tmp['customer_id'].unique().tolist()
print(len(cid))
df2 = pd.read_csv("archive/customer_data_full.csv")
print(len(df2))
tmp2 = df2.merge(tmp['customer_id'],how='inner')
print(tmp2.columns)
print(len(tmp2))
tmp2.to_csv("customer_data_23.csv",index=False)
print(tmp2.head())

