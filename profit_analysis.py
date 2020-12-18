import json
import os
import pandas as pd
from core.data_extraction import read_amazon_data_by_folder
from core.data_collation import get_profit_calculate_df


def get_profit_calculation_path():
    # 获取利润测算目录路径
    with open("log/profit_calculation_path.txt", "r+") as fp:
        profit_calculation_path_list = fp.read().split("\n")
        if len(profit_calculation_path_list) > 1:
            profit_calculation_path = profit_calculation_path_list[-2].split("\t")[0]
        else:
            profit_calculation_path = None
    return profit_calculation_path




def get_dfs(profit_calculation_path):
    # 获取利润测算需要的数据
    amazon_sheets = pd.read_excel(r"{}\流程数据\亚马逊合并报告.xlsx".format(profit_calculation_path), sheet_name=None)
    order_df = amazon_sheets["订单"]
    refund_df = amazon_sheets["退款"]
    advertising_df = amazon_sheets["广告"]
    other_df = amazon_sheets["其他"]
    erp_df = pd.read_excel(r"{}\公司数据\order.xlsx".format(profit_calculation_path), sheet_name="整理结果")
    evaluation_df = pd.read_excel(r"{}\公司数据\测评费用明细.xlsx".format(profit_calculation_path))
    fxrate_df = pd.read_excel(r"{}\公司数据\汇率.xlsx".format(profit_calculation_path))
    mapping_parameter_sheets = pd.read_excel(r"conf\映射&参数.xlsx", sheet_name=None)
    asin_erpSKU_df = mapping_parameter_sheets["ASIN&本地SKU"]
    asin_leader_df = mapping_parameter_sheets["ASIN负责人"]
    erpSKU_df = mapping_parameter_sheets["本地SKU属性"]
    clearance_asin_df = mapping_parameter_sheets["清货ASIN列表"]
    return order_df, refund_df, advertising_df, other_df,  erp_df, evaluation_df, fxrate_df, asin_erpSKU_df, asin_leader_df, erpSKU_df, clearance_asin_df

def main():
    '''
    利润测算主程序
    *. 获取利润测算目录
    1. 读取亚马逊字段映射 --> 通过文件夹方式读取亚马逊后台报告
    '''
    print("*. 获取利润测算目录 ")
    profit_calculation_path = get_profit_calculation_path()
    # print("1. 读取亚马逊字段映射 --> 通过文件夹方式读取亚马逊后台报告")
    # read_field_mapping()
    # read_amazon_data_by_folder(profit_calculation_path)
    print("2. 获取利润测算")
    order_df, refund_df, advertising_df, other_df,  erp_df, evaluation_df, fxrate_df, asin_erpSKU_df, asin_leader_df, erpSKU_df, clearance_asin_df = get_dfs(profit_calculation_path)
    get_profit_calculate_df(  order_df=order_df,
                              refund_df=refund_df,
                              advertising_df=advertising_df,
                              other_df=other_df,
                              erp_df=erp_df,
                              evaluation_df=evaluation_df,
                              fxrate_df=fxrate_df,
                              asin_erpSKU_df=asin_erpSKU_df,
                              asin_leader_df=asin_leader_df,
                              erpSKU_df=erpSKU_df,
                              clearance_asin_df=clearance_asin_df,
                              folder_path=profit_calculation_path,
                            )





if __name__ == '__main__':

    main()


