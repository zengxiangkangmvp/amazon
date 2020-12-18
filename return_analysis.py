# coding=utf-8
import pandas as pd
import json
from datetime import datetime
from core.data_extraction import get_sort_amazon_dict



# global field_mapping, standard_field_mapping, shop_site_list
asin_leader_df = pd.read_excel(r"conf/映射&参数.xlsx", sheet_name="ASIN负责人")
asin_leader_df["店铺_站点"] = asin_leader_df["店铺"] + "_" + asin_leader_df["站点"]
shop_site_list = asin_leader_df["店铺_站点"].unique()
with open(r"conf/field_mapping.json") as fp:
    field_mapping = json.load(fp)
with open(r"conf/standard_field_mapping.json", encoding="utf-8") as fp:
    standard_field_mapping = json.load(fp)


sort_amazon_dict = get_sort_amazon_dict(r"C:\Users\Administrator\Desktop\售后数据分析\202011\数据源\退货报告")
df = pd.concat(sort_amazon_dict["退货报告"])
df["退货日期"] = df["退货日期"].apply(lambda string: datetime.strptime(string[:10], "%Y-%m-%d"))
df["退货原因中文"] = df["退货原因"].apply(lambda string: field_mapping["退货报告_退货原因映射"][string] if string in field_mapping["退货报告_退货原因映射"].keys() else None)
df.to_excel("df.xlsx", index=False)