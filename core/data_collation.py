import json
import os
import numpy as np
import pandas as pd
from collections import Counter


def arrange_order_df(order_df:pd.DataFrame):
    '''
    整理 df<亚马逊-订单>
    :param order_df: df<亚马逊-订单>
    :return: df<亚马逊-订单-整理>
    '''
    order_df["运费"] = order_df["运费抵扣"] + order_df["运费抵扣税"]
    order_df["礼品"] = order_df["礼品包装抵扣"] + order_df["礼品包装抵扣税"]
    agg_field_list = ["销量","产品销售","运费","礼品","促销折扣","促销返点","销售税","市场税","佣金","FBA费用","其他交易费用","其他","合计"]
    agg = {agg_field:"sum" for agg_field in agg_field_list}
    order_arrange_df = order_df.groupby(["店铺", "站点", "订单号", "SKU"], as_index=False).agg(agg)
    return order_arrange_df


def gather_order_arrange_df(order_arrange_df: pd.DataFrame, erp_df: pd.DataFrame):
    '''
    汇总 df<亚马逊-订单-整理> & df<ERP-各项费用>
    :param order_arrange_df: df亚马逊-订单-整理>
    :param erp_df: df<ERP-各项费用>
    :return: df<亚马逊-订单-整理-汇总>
    '''

    def drop_duplicates(df:pd.DataFrame, num1:int, num2:int):
        # 去重 df<一单多品-切片>
        index_list = []
        for i in range(num1):
            if i < num2:
                index_list.append(i * (num2 + 1))
            else:
                df.loc[i * num2, "SKU成本"] = 0
                df.loc[i * num2, "包材成本"] = 0
                df.loc[i * num2, "采购物流费"] = 0
                df.loc[i * num2, "头程费"] = 0
                index_list.append(i * num2)
        df = df.iloc[index_list]
        return df

    def clean_df(df:pd.DataFrame):
        # 清洗 df<一单多品>
        df_list = []
        order_id_list = df["订单号"].unique()
        for order_id in order_id_list:
            slice_df = df[df["订单号"] == order_id].reset_index(drop=True)
            num1 = order_id_amazon[order_id]
            num2 = order_id_erp[order_id]
            slice_df = drop_duplicates(slice_df, num1, num2)
            df_list.append(slice_df)
        df = pd.concat(df_list)
        return df

    def group_df(df:pd.DataFrame):
        # 整理汇总
        for index, row in df.iterrows():
            order_id = str(row["订单号"])
            asin = str(row["ASIN"])
            if order_id[0] == "S":
                df.loc[index, "ASIN"] = "平摊"
                df.loc[index, "SKU成本"] = 0
                df.loc[index, "包材成本"] = 0
                df.loc[index, "采购物流费"] = 0
                df.loc[index, "头程费"] = 0
            if asin == "nan" and order_id[0] != "S":
                df.loc[index, "是否核算"] = "否"
            else:
                df.loc[index, "是否核算"] = "是"
        agg = {
            "产品销售": "sum",
            "销量": "sum",
            "运费": "sum",
            "礼品": "sum",
            "促销折扣": "sum",
            "促销返点": "sum",
            "销售税": "sum",
            "市场税": "sum",
            "其他交易费用": "sum",
            "其他": "sum",
            "合计": "sum",
            "佣金": "sum",
            "FBA费用": "sum",
            "SKU成本": "sum",
            "包材成本": "sum",
            "采购物流费": "sum",
            "头程费": "sum"
        }
        group_df = df[order_group_merge_df["是否核算"] == "是"].groupby(["店铺", "站点", "ASIN"], as_index=False).agg(agg)
        return group_df

    erp_group_df = erp_df.groupby(["平台订单号", "销售记录编号(Asin)"], as_index=False).agg(
        {"SKU成本": "sum", "包材成本": "sum", "采购物流费": "sum", "头程费": "sum"})
    erp_group_df.columns = ["订单号", "ASIN", "SKU成本", "包材成本", "采购物流费", "头程费"]
    order_group_merge_df = order_arrange_df.merge(erp_group_df, on="订单号", how="left")
    order_id_amazon = dict(order_arrange_df["订单号"].value_counts())
    order_id_erp = dict(erp_group_df["订单号"].value_counts())
    order_group_merge_df["是否一单多品"] = order_group_merge_df["订单号"].apply(lambda order_id:"是" if order_id_amazon[order_id] > 1 else "否")
    order_group_merge_fdf = order_group_merge_df[~((order_group_merge_df["是否一单多品"] == "是") & (order_group_merge_df["ASIN"] != None))]
    order_group_merge_tdf = order_group_merge_df[(order_group_merge_df["是否一单多品"] == "是") & (order_group_merge_df["ASIN"] != None)]
    if order_group_merge_tdf.shape[0] > 0:
        order_group_merge_tdf = clean_df(order_group_merge_tdf)
        order_group_merge_df = pd.concat([order_group_merge_fdf, order_group_merge_tdf])
    order_group_merge_df["是否核算"] = None
    order_group_df = group_df(order_group_merge_df)
    return order_group_merge_df, order_group_df


def gather_refund_df(refund_df:pd.DataFrame, erp_df:pd.DataFrame):
    '''
    汇总 df<亚马逊-退款>
    :param refund_df: df<亚马逊-退款>
    :param erp_df: df<ERP-各项费用>
    :return: df<亚马逊-退款-汇总>
    '''
    refund_group_df = refund_df.groupby(["店铺", "站点", "订单号"], as_index=False).agg({"合计":"sum"})
    erp_group_df = pd.merge(left=erp_df.groupby(["平台订单号", "销售记录编号(Asin)"], as_index=False).agg({"SKU成本":"sum"}),
                            right = erp_df.groupby(["平台订单号"]).agg({"SKU成本":"sum"}),
                            on="平台订单号", how="left"
                            )
    erp_group_df["ASIN成本占比"] = erp_group_df["SKU成本_x"] / erp_group_df["SKU成本_y"]
    erp_group_df = erp_group_df[["平台订单号", "销售记录编号(Asin)", "ASIN成本占比"]]
    erp_group_df.columns = ["订单号", "ASIN", "ASIN成本占比"]
    refund_group_df = refund_group_df.merge(erp_group_df, on="订单号")
    refund_group_df["合计-ASIN"] = refund_group_df["合计"] * refund_group_df["ASIN成本占比"]
    refund_group_df = refund_group_df.groupby(["店铺", "站点", "ASIN"], as_index=False).agg({"合计-ASIN":"sum"})
    refund_group_df.columns = ["店铺", "站点", "ASIN", "退款"]
    return refund_group_df


def gather_advertising_df(advertising_df:pd.DataFrame):
    '''
    汇总 df<亚马逊-广告>
    :param advertising_df: df<亚马逊-广告>
    :return: df<亚马逊-广告-汇总>
    '''
    advertising_group_df = advertising_df.groupby(["店铺", "站点", "广告ASIN"], as_index=False).agg({"花费":"sum"})
    advertising_group_df["花费"] = -advertising_group_df["花费"]
    advertising_group_df.columns = ["店铺", "站点", "ASIN", "广告费用"]
    return advertising_group_df


def gather_other_df(other_df:pd.DataFrame):
    '''
    汇总 df<亚马逊-其他>
    :param other_df: df<亚马逊-其他>
    :return: df<亚马逊-其他-汇总-中间> & df<亚马逊-其他-汇总>
    '''
    column_list = ["店铺", "站点", "ASIN", "FBA退货费","FBA长期仓储费","仓储费","处置费","订阅","库存安置服务费","顾客退货FBA费","秒杀","退货损坏赔偿","一般调整","优惠","早期评论人计划"]
    other_group_df = pd.pivot_table(other_df, values=["合计"], index=["店铺", "站点"], columns=["交易类型"], aggfunc=np.sum)
    other_group_df.columns = other_group_df.columns.droplevel(0)  # remove amount
    other_group_df.columns.name = None  # remove categories
    other_group_middle_df = other_group_df.reset_index()  # index to columns
    other_group_middle_df["ASIN"] = "平摊"
    other_group_df = other_group_middle_df.reindex(columns=column_list)
    other_group_df = other_group_df[column_list]
    return other_group_middle_df, other_group_df


def gather_evaluation_df(evaluation_df:pd.DataFrame, fxrate_df:pd.DataFrame=None):
    '''
    汇总 df<测评费用>
    :param evaluation_df: df<测评费用>
    :return: df<测评费用(RMB)-汇总>
    '''
    evaluation_df["站点"] = evaluation_df["站点"].apply(lambda string: string.upper())
    evaluation_merge_df = evaluation_df.merge(fxrate_df, on="币种", how="left")
    evaluation_merge_df["测评费用(RMB)"] = evaluation_merge_df["测评费用"] * evaluation_merge_df["汇率"]
    evaluation_group_df = evaluation_merge_df.groupby(["店铺", "站点", "ASIN"], as_index=False).agg({"测评费用(RMB)":"sum"})
    return evaluation_group_df


def matching_parameters(profit_calculate_df:pd.DataFrame,
                        fxrate_df:pd.DataFrame,
                        asin_erpSKU_df:pd.DataFrame,
                        asin_leader_df:pd.DataFrame,
                        erpSKU_df:pd.DataFrame,
                        clearance_asin_df:pd.DataFrame):
    '''
    匹配属性信息 df<利润测算>
    :param profit_calculate_df:   df<利润测算>
    :param fxrate_df:             df<汇率>
    :param asin_erpSKU_df:        df<ASIN&本地SKU>
    :param asin_leader_df:        df<ASIN负责人>
    :param erpSKU_df:             df<本地SKU属性>
    :param clearance_asin_df:     df<清货ASIN列表>
    :return:
    '''
    with open(r"conf/site_currency_mapping.json") as fp:
        site_currency_mapping = json.load(fp)
    dimension_list = ["店铺", "站点", "ASIN"]
    index_list = list(set(profit_calculate_df.columns.tolist()).difference(set(dimension_list)))
    for index in index_list: profit_calculate_df[index].fillna(0, inplace=True)
    profit_calculate_df = profit_calculate_df.groupby(dimension_list, as_index=False).agg({index: "sum" for index in index_list})
    profit_calculate_df["店铺_站点_ASIN"] = profit_calculate_df["店铺"] + profit_calculate_df["站点"] + profit_calculate_df["ASIN"]
    profit_calculate_df["店铺_站点"] = profit_calculate_df["店铺"] + profit_calculate_df["站点"]
    asin_leader_df["店铺_站点_ASIN"] = asin_leader_df["店铺"] + asin_leader_df["站点"] + asin_leader_df["ASIN"]
    asin_leader_df = asin_leader_df[["店铺_站点_ASIN", "部门", "运营负责人"]]
    clearance_asin_df["店铺_站点_ASIN"] = clearance_asin_df["店铺"] + clearance_asin_df["站点"] + clearance_asin_df["ASIN"]
    clearance_asin_df = clearance_asin_df[["店铺_站点_ASIN", "是否清货"]]
    profit_calculate_df["币种"] = profit_calculate_df["站点"].map(site_currency_mapping)
    profit_calculate_df = profit_calculate_df\
                            .merge(fxrate_df, on="币种", how="left")\
                            .merge(asin_erpSKU_df, on="ASIN", how="left")\
                            .merge(erpSKU_df, on="本地SKU", how="left")\
                            .merge(asin_leader_df, on="店铺_站点_ASIN", how="left")\
                            .merge(clearance_asin_df, on="店铺_站点_ASIN", how="left")
    return profit_calculate_df


def add_calculate_field(profit_calculate_df:pd.DataFrame):
    '''
    新增计算字段 df<利润测算>
    :param profit_calculate_df:
    :return:
    '''
    def generate_sale_price_quantity(row):
        # 销售价格*数量(RMB)
        if row["ASIN"] == "平摊":
            sale_price_quantity = 0
        else:
            sale_price_quantity = (row["合计"] -
            (row["运费"] + row["礼品"]+ row["促销折扣"]+ row["促销返点"]+ row["销售税"]+ row["市场税"]+ row["其他交易费用"]+ row["其他"] + row["佣金"] + row["FBA费用"]))\
            * row["汇率"]
        return sale_price_quantity

    def generate_gmv(row):
        # 销售额(RMB)
        if row["ASIN"] == "平摊":
            gmv = 0
        else:
            gmv = (row["合计"] - (row["佣金"] + row["FBA费用"])) * row["汇率"]
        return gmv

    def generate_cost(row):
        # 成本(RMB)
        cost = (row["佣金"] + row["FBA费用"] + row["退款"] + row["FBA退货费"] + row["FBA长期仓储费"] + row["仓储费"] + row["处置费"] +
                row["订阅"] + row["库存安置服务费"] + row["顾客退货FBA费"] + row["秒杀"] + row["退货损坏赔偿"] + row["一般调整"] + row["优惠"] + row["早期评论人计划"] + row["广告费用"]) * row["汇率"] -\
                (row["SKU成本"] + row["包材成本"] + row["采购物流费"] + row["头程费"] + row["测评费用(RMB)"])
        return cost

    def generate_gross_profit(row):
        # 毛利(RMB)
        gross_profit = row["销售额(RMB)"] + row["成本(RMB)"]
        return gross_profit

    def generate_gross_margin(row):
        # 毛利率
        if row["销售额(RMB)"] == 0:
            gross_margin = None
        else:
            gross_margin = row["毛利(RMB)"] / row["销售额(RMB)"]
        return gross_margin

    def generate_advertising(row):
        # 广告费用(RMB)
        return row["广告费用"] * row["汇率"] * -1

    def generate_promotion(row):
        # 促销(RMB)
        return row["促销折扣"] * row["汇率"] * -1

    def generate_spike(row):
        # 活动(RMB)
        return row["秒杀"] * row["汇率"] * -1

    def generate_refund(row):
        # 退款(RMB)
        return row["退款"] * row["汇率"] * -1

    profit_calculate_df["广告费用(RMB)"] = profit_calculate_df.apply(lambda row: generate_advertising(row), axis=1)
    profit_calculate_df["促销(RMB)"] = profit_calculate_df.apply(lambda row: generate_promotion(row), axis=1)
    profit_calculate_df["活动(RMB)"] = profit_calculate_df.apply(lambda row: generate_spike(row), axis=1)
    profit_calculate_df["退款(RMB)"] = profit_calculate_df.apply(lambda row: generate_refund(row), axis=1)
    profit_calculate_df["销售价格*数量(RMB)"] = profit_calculate_df.apply(lambda row: generate_sale_price_quantity(row), axis=1)
    profit_calculate_df["销售额(RMB)"] = profit_calculate_df.apply(lambda row: generate_gmv(row), axis=1)
    profit_calculate_df["成本(RMB)"] = profit_calculate_df.apply(lambda row: generate_cost(row), axis=1)
    profit_calculate_df["毛利(RMB)"] = profit_calculate_df.apply(lambda row: generate_gross_profit(row), axis=1)
    profit_calculate_df["毛利率"] = profit_calculate_df.apply(lambda row: generate_gross_margin(row), axis=1)
    return profit_calculate_df


def split_expenses_equally(profit_calculate_df:pd.DataFrame, asin_leader_df:pd.DataFrame=None, column_list:list=None):
    '''
    拆分平摊费用
    :param profit_calculate_df:
    :param asin_leader_df:
    :return:
    '''
    index_list = [
        "产品销售",
        "运费",
        "礼品",
        "促销折扣",
        "促销返点",
        "销售税",
        "市场税",
        "其他交易费用",
        "其他",
        "合计",
        "佣金",
        "FBA费用",
        "退款",
        "FBA退货费",
        "FBA长期仓储费",
        "仓储费",
        "处置费",
        "订阅",
        "库存安置服务费",
        "顾客退货FBA费",
        "秒杀",
        "退货损坏赔偿",
        "一般调整",
        "优惠",
        "早期评论人计划",
        "广告费用",
        "SKU成本",
        "包材成本",
        "采购物流费",
        "头程费",
        "测评费用(RMB)",
        "销售价格*数量(RMB)",
        "销售额(RMB)",
        "成本(RMB)",
        "毛利(RMB)",
        "广告费用(RMB)",
        "促销(RMB)",
        "活动(RMB)",
        "退款(RMB)"
    ]

    def generate_department(row):
        # 部门
        if str(row["部门_1"]) != "nan":
            return row["部门_1"]
        else:
            return row["部门_2"]

    def generate_leader(row):
        # 运营负责人
        if str(row["部门_1"]) != "nan":
            return row["运营负责人_1"]
        else:
            return row["运营负责人_2"]


    def generate_gmv_rate(row):
        # GMV占比
        if str(row["部门_1"]) != "nan":
            return row["GMV占比_1"]
        else:
            return row["GMV占比_2"]

    # 拆分 df<利润测算> df<利润测算-非平摊>&df<利润测算-平摊>
    profit_calculate_df = profit_calculate_df[profit_calculate_df["成本(RMB)"] != 0]
    profit_calculate_fdf = profit_calculate_df[profit_calculate_df["ASIN"] != "平摊"]
    profit_calculate_tdf = profit_calculate_df[profit_calculate_df["ASIN"] == "平摊"]
    # 合成 店铺_站点 对应的 部门&运营负责人 GMV占比(主要)
    profit_calculate_four_group_fdf = profit_calculate_fdf.groupby(["店铺", "站点", "部门", "运营负责人"], as_index=False).agg({"销售额(RMB)":"sum"})
    profit_calculate_four_group_fdf["店铺_站点"] = profit_calculate_four_group_fdf["店铺"] +  profit_calculate_four_group_fdf["站点"]
    profit_calculate_four_group_fdf = profit_calculate_four_group_fdf[["店铺_站点", "部门",  "运营负责人", "销售额(RMB)"]]
    profit_calculate_two_group_fdf = profit_calculate_fdf.groupby(["店铺", "站点"], as_index=False).agg({"销售额(RMB)":"sum"})
    profit_calculate_two_group_fdf["店铺_站点"] = profit_calculate_two_group_fdf["店铺"] + profit_calculate_two_group_fdf["站点"]
    profit_calculate_two_group_fdf = profit_calculate_two_group_fdf[["店铺_站点", "销售额(RMB)"]]
    profit_calculate_merge_fdf = profit_calculate_four_group_fdf.merge(profit_calculate_two_group_fdf, on="店铺_站点")
    profit_calculate_merge_fdf["GMV占比"] = profit_calculate_merge_fdf["销售额(RMB)_x"] / profit_calculate_merge_fdf["销售额(RMB)_y"]
    profit_calculate_group_fdf = profit_calculate_merge_fdf[["店铺_站点", "部门", "运营负责人", "GMV占比"]]
    profit_calculate_group_fdf.columns = ["店铺_站点", "部门_1", "运营负责人_1", "GMV占比_1"]
    # 合成 店铺_站点 对应的 部门&运营负责人 GMV占比(备用)
    asin_leader_df["店铺_站点"] = asin_leader_df["店铺"] + asin_leader_df["站点"]
    shop_site_leader_df = asin_leader_df[["店铺_站点", "部门", "运营负责人"]]
    shop_site_leader_df.drop_duplicates(inplace=True)
    shop_site_leader_dict = dict(shop_site_leader_df["店铺_站点"].value_counts())
    shop_site_leader_df["GMV占比"] = shop_site_leader_df["店铺_站点"].apply(lambda string: 1 / shop_site_leader_dict[string] if string in shop_site_leader_dict.keys() else None)
    shop_site_leader_df.columns = ["店铺_站点", "部门_2", "运营负责人_2", "GMV占比_2"]
    # 重新合成 df<利润测算>
    profit_calculate_tdf["店铺_站点"] = profit_calculate_tdf["店铺"] + profit_calculate_tdf["站点"]
    profit_calculate_merge_tdf = profit_calculate_tdf.merge(profit_calculate_group_fdf, on="店铺_站点", how="left").merge(shop_site_leader_df, on="店铺_站点", how="left")
    profit_calculate_merge_tdf["部门"] = profit_calculate_merge_tdf.apply(lambda row: generate_department(row), axis=1)
    profit_calculate_merge_tdf["运营负责人"] = profit_calculate_merge_tdf.apply(lambda row: generate_leader(row), axis=1)
    profit_calculate_merge_tdf["GMV占比"] = profit_calculate_merge_tdf.apply(lambda row: generate_gmv_rate(row), axis=1)
    profit_calculate_merge_tdf.reindex(columns=column_list)
    for index in index_list: profit_calculate_merge_tdf[index] = profit_calculate_merge_tdf[index] * profit_calculate_merge_tdf["GMV占比"]
    profit_calculate_tdf = profit_calculate_merge_tdf[column_list]
    profit_calculate_tdf.drop_duplicates(inplace=True)
    profit_calculate_df = pd.concat([profit_calculate_fdf, profit_calculate_tdf])
    return profit_calculate_df


def save_middle_df(middle_file_path:str, middle_df_dict:dict):
    '''
    保存 df<中间汇总表>
    :param middle_file_path: 中间汇总表路径
    :param middle_df_dict: 中间汇总表映射
    :return:
    '''
    excel_writer = pd.ExcelWriter(middle_file_path)
    for middle_df_name, middle_df in middle_df_dict.items():
        middle_df.to_excel(excel_writer, sheet_name=middle_df_name, index=False)
    excel_writer.save()


def save_profit_calculate_df(profit_calculate_file_path:str, profit_calculate_df:pd.DataFrame):
    '''
    保存 df<利润测算> & 数据透视
    :param profit_calculate_file_path: 利润测算路径
    :param profit_calculate_df: df<利润测算>
    :return:
    '''
    excel_writer = pd.ExcelWriter(profit_calculate_file_path)
    profit_calculate_df.to_excel(excel_writer, sheet_name="明细", index=False)
    excel_writer.save()


def get_profit_calculate_df(order_df:pd.DataFrame=None,
                            refund_df:pd.DataFrame=None,
                            advertising_df:pd.DataFrame=None,
                            other_df:pd.DataFrame=None,
                            erp_df:pd.DataFrame=None,
                            evaluation_df:pd.DataFrame=None,
                            fxrate_df:pd.DataFrame=None,
                            asin_erpSKU_df:pd.DataFrame=None,
                            asin_leader_df:pd.DataFrame=None,
                            erpSKU_df:pd.DataFrame=None,
                            clearance_asin_df:pd.DataFrame=None,
                            folder_path:str=None):
    '''
    获取 df<利润测算>
    :param order_df:          df<亚马逊-订单>
    :param refund_df:         df<亚马逊-退款>
    :param advertising_df:    df<亚马逊-广告>
    :param other_df:          df<亚马逊-其他费用>
    :param erp_df:            df<ERP-各项费用>
    :param evaluation_df:     df<测评费用>
    :param fxrate_df:         df<汇率>
    :param asin_erpSKU_df:    df<ASIN&本地SKU>
    :param asin_leader_df:    df<ASIN负责人>
    :param erpSKU_df:         df<本地SKU属性>
    :param clearance_asin_df: df<清货ASIN列表>
    :return:df<利润测算>
    '''
    column_list = [
        "部门",
        "运营负责人",
        "站点",
        "币种",
        "汇率",
        "店铺",
        "ASIN",
        "本地SKU",
        "产品名称",
        "产品型号",
        "是否清货",
        "产品销售",
        "运费",
        "礼品",
        "促销折扣",
        "促销返点",
        "销售税",
        "市场税",
        "其他交易费用",
        "其他",
        "合计",
        "佣金",
        "FBA费用",
        "退款",
        "FBA退货费",
        "FBA长期仓储费",
        "仓储费",
        "处置费",
        "订阅",
        "库存安置服务费",
        "顾客退货FBA费",
        "秒杀",
        "退货损坏赔偿",
        "一般调整",
        "优惠",
        "早期评论人计划",
        "广告费用",
        "SKU成本",
        "包材成本",
        "采购物流费",
        "头程费",
        "测评费用(RMB)",
        "销量",
        "销售价格*数量(RMB)",
        "销售额(RMB)",
        "成本(RMB)",
        "毛利(RMB)",
        "毛利率",
        "广告费用(RMB)",
        "促销(RMB)",
        "活动(RMB)",
        "退款(RMB)"
    ]
    print("------------------->整理 df<亚马逊-订单>")
    order_arrange_df = arrange_order_df(order_df)
    print("------------------->汇总 df<亚马逊-订单-整理> & df<ERP-各项费用>")
    order_group_merge_df, order_group_df = gather_order_arrange_df(order_arrange_df, erp_df)
    print("------------------->汇总 df<亚马逊-退款>")
    refund_group_df = gather_refund_df(refund_df, erp_df)
    print("------------------->汇总 df<亚马逊-广告>")
    advertising_group_df = gather_advertising_df(advertising_df)
    print("------------------->汇总 df<亚马逊-其他>")
    other_group_middle_df, other_group_df = gather_other_df(other_df)
    print("------------------->汇总 df<测评费用>")
    evaluation_group_df = gather_evaluation_df(evaluation_df, fxrate_df)
    profit_calculate_df = pd.concat([order_group_df, refund_group_df, advertising_group_df, other_group_df, evaluation_group_df])
    print("------------------->保存 df<中间汇总表>")
    middle_df_dict =  { "order_df": order_df,
                        "order_arrange_df": order_arrange_df,
                        "order_group_merge_df": order_group_merge_df,
                        "order_group_df": order_group_df,
                        "refund_df": refund_df,
                        "refund_group_df": refund_group_df,
                        "advertising_df": advertising_df,
                        "advertising_group_df": advertising_group_df,
                        "other_df": other_df,
                        "other_group_middle_df": other_group_middle_df,
                        "other_group_df": other_group_df,
                        "evaluation_df": evaluation_df,
                        "evaluation_group_df": evaluation_group_df }
    middle_file_path = r"{}\流程数据\中间汇总表.xlsx".format(folder_path)
    save_middle_df(middle_file_path, middle_df_dict)
    print("------------------->匹配属性信息 df<利润测算>")
    profit_calculate_df =matching_parameters(profit_calculate_df, fxrate_df, asin_erpSKU_df, asin_leader_df, erpSKU_df, clearance_asin_df)
    print("------------------->新增计算字段 df<利润测算> & 调整列顺序选取需求字段")
    profit_calculate_df = add_calculate_field(profit_calculate_df)
    profit_calculate_df.reindex(columns=column_list)
    profit_calculate_df = profit_calculate_df[column_list]
    print("------------------->保存 拆分平摊费用 & df<利润测算>")
    profit_calculate_df = split_expenses_equally(profit_calculate_df, asin_leader_df, column_list)
    profit_calculate_file_path = "{}\{}.xlsx".format(folder_path, os.path.split(folder_path)[1].split(".")[0])
    save_profit_calculate_df(profit_calculate_file_path, profit_calculate_df)