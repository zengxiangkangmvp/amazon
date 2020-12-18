import os
import winreg
import json
import pandas as pd
from datetime import datetime


def generate_profit_calculate_path(start_data:str, end_data:str):
    '''
    生成利润测算目录
    :param start_data: 开始日期
    :param end_data:   结束日期
    :return:
    '''
    desktop_path = winreg.QueryValueEx(winreg.OpenKey(winreg.HKEY_CURRENT_USER,r'Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'), "Desktop")[0]
    root_path = desktop_path + "\{}至{}利润测算".format(start_data, end_data)
    if os.path.isdir(root_path): return root_path
    amazon_data_path = desktop_path + "\{}至{}利润测算\亚马逊数据".format(start_data, end_data)
    company_data_path = desktop_path + "\{}至{}利润测算\公司数据".format(start_data, end_data)
    process_data_path = desktop_path + "\{}至{}利润测算\流程数据".format(start_data, end_data)
    split_data_path = desktop_path + "\{}至{}利润测算\拆分报表数据".format(start_data, end_data)
    path_list = [root_path, amazon_data_path, company_data_path, process_data_path, split_data_path]
    for path in path_list: os.mkdir(path)
    return root_path

def create_profit_calculate():

    current_date_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_data = input("请输入利润测算初始日期(格式YYYYMMDD):")
    end_data = input("请输入利润测算结束日期(格式YYYYMMDD):")
    profit_calculate_path = generate_profit_calculate_path(start_data, end_data)
    insert_string = "{}\t{}\n".format(profit_calculate_path, current_date_string)
    with open("log/profit_calculate_path.txt", "a+") as fp: fp.write(insert_string)

def update_field_mapping():
    # 读取亚马逊字段映射
    field_mapping = {}
    sheets = pd.read_excel(r"conf\amazon_mapping.xlsx", sheet_name=None)
    for sheet_name, df in sheets.items():
        field_mapping[sheet_name] = {}
        for index, row in df.iterrows():
            field = str(row["field"])
            standard_field = str(row["standard_field"])
            field_mapping[sheet_name][field] = standard_field
    json_str = json.dumps(field_mapping)
    with open(r'conf\field_mapping.json', 'w') as json_file: json_file.write(json_str)

if __name__ == '__main__':

    update_field_mapping()