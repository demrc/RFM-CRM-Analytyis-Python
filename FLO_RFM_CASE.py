import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

df_ = pd.read_csv(r"......csv")
df = df_.copy()
#Part 1
df.columns

df.head(10)
df.describe()
df.dtypes
df.isnull().sum()

df["omnichannel_order"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["omnichannel_order"]


df["omnichannel_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
df["omnichannel_value"]

df["first_order_date"]= pd.to_datetime(df["first_order_date"], format="%Y-%m-%d" )
df["last_order_date"]= pd.to_datetime(df["last_order_date"], format="%Y-%m-%d" )
df["last_order_date_offline"]= pd.to_datetime(df["last_order_date_offline"], format="%Y-%m-%d" )
df["last_order_date_online"]= pd.to_datetime(df["last_order_date_online"], format="%Y-%m-%d" )

df.dtypes

rfm_c = df.groupby("master_id").agg({'omnichannel_order': lambda omnichannel_order: (omnichannel_order.mean()),
                                     'omnichannel_value': lambda omnichannel_value: omnichannel_value.mean()})

most_order_ten = rfm_c.sort_values(by="omnichannel_order",ascending=False).head(10)
most_order_ten

most_value_ten = rfm_c.sort_values(by="omnichannel_value",ascending=False).head(10)
most_value_ten


def create_df(df, csv=False):
    df.columns
    df.head(10)
    df.describe()
    df.dtypes
    df.isnull().sum()
    df["omnichannel_order"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["omnichannel_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
    
    df["first_order_date"]= pd.to_datetime(df["first_order_date"], format="%Y-%m-%d" )
    df["last_order_date"]= pd.to_datetime(df["last_order_date"], format="%Y-%m-%d" )
    df["last_order_date_offline"]= pd.to_datetime(df["last_order_date_offline"], format="%Y-%m-%d" )
    df["last_order_date_online"]= pd.to_datetime(df["last_order_date_online"], format="%Y-%m-%d" )
    
    df_c = df.groupby("master_id").agg({'omnichannel_order': lambda omnichannel_order: (omnichannel_order.mean()),
                                     'omnichannel_value': lambda omnichannel_value: omnichannel_value.mean()})
    
    most_order_ten = rfm_c.sort_values(by="omnichannel_order",ascending=False).head(10)
    most_value_ten = rfm_c.sort_values(by="omnichannel_value",ascending=False).head(10)
    
    if csv:
        df.to_csv("rfm.csv")

    return df

df = df_.copy()

df_new = create_df(df, csv=True)

#Part 2

df.head()
df["last_order_date"].max()

today_date = dt.datetime(2021, 6, 1)
type(today_date)

rfm = df.groupby("master_id").agg({'last_order_date': lambda  last_order_date: (today_date - last_order_date.max()).days,
                                     'omnichannel_order': lambda omnichannel_order: omnichannel_order,
                                     'omnichannel_value': lambda omnichannel_value: omnichannel_value.sum()
                                      })

rfm.columns = ['recency', 'frequency', 'monetary']

#Part 3

rfm["recency_score"] = pd.qcut(rfm["recency"], 5 , labels = [5,4,3,2,1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method = "first"), 5 , labels = [1,2,3,4,5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5 , labels = [1,2,3,4,5])

rfm["RFM_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))

#Part 4

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)

#Part 5

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

new_table=pd.merge(df,rfm,on="master_id")

case_a = new_table[(new_table["segment"] == "champions") | (new_table["segment"] == "loyal_customers") & (((new_table["monetary"]/2)> 250))& ((new_table["interested_in_categories_12"].str.contains("KADIN")))]

case_a_customer_id=case_a["master_id"]

case_a_final = case_a_customer_id.reset_index(drop=True)

case_a_final.to_csv("case_a_customer_id.csv")

#----------

case_b = new_table[((new_table["interested_in_categories_12"].str.contains("ERKEK"))) & (new_table["interested_in_categories_12"].str.contains("ÇOCUK")) & (new_table["segment"] == "cant_loose") | (new_table["segment"] == "about_to_sleep") | (new_table["segment"]=="new_customers")]

case_b_customer_id=case_a["master_id"]

case_b_final = case_b_customer_id.reset_index(drop=True)

case_b_final.to_csv("case_b_customer_id.csv")
