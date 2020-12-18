import os
import re
import json
import pandas as pd


def transform_account_string(account):
    '''
    转化账单金额
    :param account_string: 账单金额-字符串
    :return: 账单金额-浮点数
    '''
    account_string = str(account)
    if account_string[0] in ["−", "-"]:
        account_string = account_string[1:]
        rate = -1
    else:
        rate = 1
    if account_string == "nan":
        return 0.0
    elif "\xa0" in account_string:
        return float(account_string.replace("\xa0", "").replace(",",".")) * rate
    elif account_string.count(".") > 1:
        account_string_list = account_string.split(".")
        nee_account_string = "".join(account_string_list[:len(account_string_list)-1]) + "." + account_string_list[-1]
        return float(nee_account_string) * rate
    elif "," in account_string and "." in account_string:
        for s in account_string:
            if s == ".":
                return float(account_string.replace(".", "").replace(",", ".")) * rate
            elif s == ",":
                return float(account_string.replace(",", "")) * rate
    elif "," in account_string and "." not in account_string:
        return float(account_string.replace(",",".")) * rate
    else:
        return float(account_string) * rate


def get_file_path_list(folder_path: str, file_path_list: list = None):
    '''
    获取文件路径列表
    :param folder_path: 文件夹路径
    :param file_path_list: 初始文件路径列表
    :return:
    '''
    if not file_path_list: file_path_list = []
    for dir in os.listdir(folder_path):
        dir_path = folder_path + "/" + dir
        if os.path.isfile(dir_path):
            file_path_list.append(dir_path)
        else:
            file_path_list = get_file_path_list(dir_path, file_path_list)
    return file_path_list


def read_data(file_path:str, file_type:str, skiprows:int=0):
    '''
    :param file_path: 文件路径
    :param file_type: 文件类型
    :param skiprows:  忽略前几行
    :return:
    '''
    try:
        if file_type == ".csv":
            df = pd.read_csv(file_path, skiprows=skiprows, encoding="utf-8", error_bad_lines=False)
        else:
            df = pd.read_excel(file_path, skiprows=skiprows, encoding="utf-8", error_bad_lines=False)
    except:
        try:
            if file_type == ".csv":
                df = pd.read_csv(file_path, skiprows=skiprows, encoding="gbk", error_bad_lines=False)
            else:
                df = pd.read_excel(file_path, skiprows=skiprows, encoding="gbk", error_bad_lines=False)
        except:
            if file_type == ".csv":
                df = pd.read_csv(file_path, skiprows=skiprows, encoding="unicode_escape", error_bad_lines=False)
            else:
                df = pd.read_excel(file_path, skiprows=skiprows, encoding="unicode_escape", error_bad_lines=False)
    return df


def read_amazon_data_by_file(file_path:str, data_type:str):
    '''
    通过文件方式读取亚马逊后台报告
    :param file_path: 文件路径
    :param data_type: 亚马逊后台报告类型
    :param field_mapping: 亚马逊字段映射
    :return:
    '''
    def parse_shop_site(file_name:str):
        '''
        从文件名中解析出 店铺&站点
        :param file_name:
        :return:
        '''
        li = re.split(r" |-|_", file_name)
        if len(li) > 2:
            if data_type == "退货报告":
                shop = li[0]
                site = li[1].upper()
                return shop, site
            else:
                shop_unknown = li[0].lower()
                site_unknown = li[1].lower()
                shop_site_unknown = shop_unknown + "_" + site_unknown
                for shop_site in shop_site_list:
                    if shop_site_unknown == shop_site.lower():
                        shop = shop_site.split("_")[0]
                        site = shop_site.split("_")[1]
                        return shop, site
                return "店铺参数不存在此店铺&站点", shop_site_unknown
        else:
            return "文件名解析出错", ""

    file_name = os.path.split(file_path)[1]
    file_type = os.path.splitext(file_path)[1]
    column_field_mapping = field_mapping["{}_字段映射".format(data_type)]
    try:
        df = read_data(file_path, file_type)
    except:
        print(file_name)
    if "Unnamed" in "".join(df.columns.tolist()) or df.shape[1] == 1:
        df = read_data(file_path, file_type, skiprows=6)
        if "Unnamed" in "".join(df.columns.tolist()) or df.shape[1] == 1:
            df = read_data(file_path, file_type, skiprows=7)
    if df.shape[1] > 0:
        rename_dict = {column: column_field_mapping[column] for column in df.columns.tolist()}
        df.rename(columns=rename_dict, inplace=True)
        df["文件名"] = file_name
        df["店铺"], df["站点"] = parse_shop_site(file_name)
    else:
        df = None
    return df


def generate_transaction_type(type:str, description:str):
    '''
    生成交易类型字段
    :param type: 亚马逊交易报告type字段
    :param description: 亚马逊交易报告description字段
    :return:
    '''
    type_dict = field_mapping["交易报告_交易类型-type映射"]
    description_dict = field_mapping["交易报告_交易类型-description映射"]
    type_value = type_dict[type] if type in type_dict.keys() else None
    description_value = description_dict[description] if description in description_dict.keys() else None
    if description_value:
        return description_value
    elif type_value:
        return type_value
    elif description[:4] == "Save":
        return "优惠"
    else:
        return None


def get_sort_amazon_dict(amazon_data_folder:str):
    '''
    获取亚马逊报告df分类字典
    :param folder_path: 亚马逊数据文件夹路径
    :return: sort_amazon_dict
    '''
    global field_mapping, standard_field_mapping, shop_site_list
    asin_leader_df = pd.read_excel(r"conf/映射&参数.xlsx", sheet_name="ASIN负责人")
    asin_leader_df["店铺_站点"] = asin_leader_df["店铺"] + "_" + asin_leader_df["站点"]
    shop_site_list = asin_leader_df["店铺_站点"].unique()
    with open(r"conf/field_mapping.json") as fp:
        field_mapping = json.load(fp)
    with open(r"conf/standard_field_mapping.json", encoding="utf-8") as fp:
        standard_field_mapping = json.load(fp)
    sort_amazon_dict = {"交易报告": [], "广告报告": [], "退货报告":[]}
    file_path_list = get_file_path_list(amazon_data_folder)
    print("读取到该路径<{}>下文件总数[{}]".format(amazon_data_folder, len(file_path_list)))
    sort_pattern = re.compile(r"(customtransaction|customunifiedtransaction|交易|订单|推广|广告|advertised|beworbenes produkt|退货)")
    for file_path in file_path_list:
        file_name = os.path.split(file_path)[1].lower()
        sort_list = re.findall(sort_pattern, file_name)
        if sort_list:
            if sort_list[0] in ["customtransaction", "customunifiedtransaction", "交易", "订单"]:
                df = read_amazon_data_by_file(file_path, "交易报告")
                sort_amazon_dict["交易报告"].append(df)
            elif sort_list[0] in ["推广", "广告", "advertised", "beworbenes produkt"]:
                df = read_amazon_data_by_file(file_path, "广告报告")
                sort_amazon_dict["广告报告"].append(df)
            elif sort_list[0] in ["退货"]:
                df = read_amazon_data_by_file(file_path, "退货报告")
                sort_amazon_dict["退货报告"].append(df)
        else:
            print("未识别到的文件名:<{}>".format(file_name))
    return sort_amazon_dict


def save_amazon_data(sort_amazon_dict:dict, process_data_folder:str):
    '''
    保存亚马逊数据
    :param sort_amazon_dict: 亚马逊报告df分类字典
    :param process_data_folder: 流程数据文件夹路径
    :return:
    '''
    transform_account_field_list = ["产品销售", "运费抵扣", "运费抵扣税", "礼品包装抵扣", "礼品包装抵扣税", "促销折扣", "促销返点", "销售税", "市场税", "佣金", "FBA费用", "其他交易费用", "其他", "合计"]
    excel_writer = pd.ExcelWriter(r"{}/亚马逊合并报告.xlsx".format(process_data_folder))
    for data_type, df_list in sort_amazon_dict.items():
        print("亚马逊报告类型<{}>文件数量[{}]".format(data_type, len(df_list)))
        if len(df_list) == 0: break
        df = pd.concat(df_list)
        standard_field_list = standard_field_mapping["{}_标准字段".format(data_type)]
        df = df.reindex(columns=standard_field_list)
        if data_type == "交易报告":
            for transform_account_field in transform_account_field_list:
                df[transform_account_field] = df[transform_account_field].apply(transform_account_string)
            df["交易类型"] = df.apply(lambda row: generate_transaction_type(row["类型"], row["描述"]), axis=1)
            order_df = df[df["交易类型"] == "订单"]
            refund_df = df[df["交易类型"] == "退款"]
            other_df = df[(df["交易类型"] != "订单") & (df["交易类型"] != "退款")]
            order_df.to_excel(excel_writer, sheet_name="订单", index=False)
            refund_df.to_excel(excel_writer, sheet_name="退款", index=False)
            other_df.to_excel(excel_writer, sheet_name="其他", index=False)
            order_id_df = pd.DataFrame([(order_id, ",") for order_id in list(set(order_df["订单号"].tolist() + refund_df["订单号"].tolist()))], columns=["order_id", "comma"])
            order_id_df.to_excel(excel_writer, sheet_name="亚马逊订单号", index=False)
        elif data_type == "广告报告":
            df["花费"] = df["花费"].apply(transform_account_string)
            df.to_excel(excel_writer, sheet_name="广告", index=False)
    excel_writer.save()


def read_amazon_data_by_folder(folder_path:str):
    '''
    通过文件夹方式读取亚马逊后台报告
    :param folder_path: 文件夹路径
    :return:
    '''
    amazon_data_folder = folder_path + "/亚马逊数据"
    process_data_folder = folder_path + "/流程数据"
    sort_amazon_dict = get_sort_amazon_dict(amazon_data_folder)
    save_amazon_data(sort_amazon_dict, process_data_folder)


