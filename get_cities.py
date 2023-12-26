import requests
from lxml import etree
import random
import time
import re
import pandas as pd
from tqdm import tqdm
import os


def fetch(url):
    sleep_time = random.uniform(10, 12)  # 0到0.5秒之间的随机休眠时间
    time.sleep(sleep_time)
    cookies = {
        "userid360_xml": "286B7A09023A74E6428941322477FB8D",
        "time_create": "1698408777360",
        "SECKEY_ABVK": "nJ86d1MPkHuBaiaKextSNKZw9i82p0QdzI1MVipbR3I%3D",
        "BMAP_SECKEY": "X7wAuUy355p3weAVI-SQFi735v_DDUJU8oMxqIT33cPYmiYXhjXuWMTvlOppuXDKeOfovQVeHuFqFbaOTbV_0eM-aHaw2mEOMjkmeDtMpxW9I6PZioX3-hA1rmCjKBN4cL63wYVPJdr7udwBVU5FDTdb_hCAxAbxnkeXsGchCMOkvCnEnNP8a6mBDrUP4zoM",
        "f": "n",
        "commontopbar_new_city_info": "414%7C%E9%95%BF%E6%B2%99%7Ccs",
        "id58": "CocIQ2TgJESjB/A2IrTJAg==",
        "58tj_uuid": "97b888d1-2dc5-4338-b2b8-498934f834d2",
        "als": "0",
        "aQQ_ajkguid": "1E62B72A-D352-419C-9800-028C6DA53C7F",
        "sessid": "0609C838-4FF8-4914-9DCC-49595CC810CB",
        "ajk-appVersion": "",
        "wmda_uuid": "1ed2b4b2ed4b2604c73b4a68c1a3785c",
        "wmda_new_uuid": "1",
        "wmda_visited_projects": "%3B10104579731767",
        "xxzlclientid": "e863bd04-6f60-4cc5-b0e7-1697352675990",
        "xxzlxxid": "pfmxdmU6o/X23ABjn1hlxoYnWGYULKaaAchywYnzMf288dG6NZalG+k01BB9MTpyOsw3",
        "xxzl_smartid": "9ec58e210fc87d13ec0ea2b15993f3c0",
        "xxzl_cid": "86ab111f684f4862a1f98d351c11064a",
        "xxzl_deviceid": "etIVx88ptdtmJVhnEtyb+Cyia9H/xxsypEplZLyOdXynb2mskWUSucUlT0vBLevv",
        "fzq_h": "cf4c1428066cf82ae2a7f922bca202a2_1698245390757_7daf5b117eca41bf8422c2ba43e9e6c5_1709519922",
        "www58com": "UserID=99065225231363&UserName=tctc_",
        "58cooper": "userid=99065225231363&username=tctc_",
        "58uname": "tctc_",
        "passportAccount": "atype=0&bstate=0",
        "new_uv": "27",
        "spm": "",
        "init_refer": "",
        "utm_source": "link",
        "ctid": "414",
        "new_session": "0",
        "commontopbar_ipcity": "sh%7C%E4%B8%8A%E6%B5%B7%7C0",
        "city": "cs",
        "58home": "cs",
        "xxzlbbid": "pfmbM3wxMDI5MnwxLjMuMHwxNjk4MzMwNTI0NjkyfGZpMDhMSTViSzZzeWZYYmRmc05ObXhsVS9ZZXVONW9mY2o2ZDVwKzhOZkk9fDk5OGNlMDQxMWJkMmYxMzRkMmFhZDkxMDYwNWU1NTcyXzE2OTgzMzA1MjMyMTFfM2NiZDVkMWZjOTRlNDBmYTk1ZWE0YmY4NzAzYzM0NmFfMTcwOTUxOTkyMnwxYzc1ZjU0MTQ2M2JmZDM4ODcxZGQyMGVmNDAyMGM4MV8xNjk4MzMwNTI0NTE1XzI1NQ==",
        "PPU": "UID=99065225231363&UN=tctc_&TT=4eb15c38bfa28309f3c3ad7b113d08c2&PBODY=An81iKcdoVRQXVWafkGGlWm26PG_6miRSopriSYiW_8iSnqwQslrdXbvqv_zrD1Vl4TJy7RHhhsfja6BLnh5NUHewTffM2nR7h1h_L6Rihg1Ls7FgJMDZtCf9wLy-Qj40v2rddKCBFTo-6sORJ7XFPKir1xgfGT0dhfSLCJk70U&VER=1&CUID=kuLScWTP23sRbJJwWPqLiA"
    }

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Cache-Control": "max-age=0",
        "Referer": "https://callback.58.com/",
        "Sec-Ch-Ua": "\"Chromium\";v=\"118\", \"Microsoft Edge\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.46"
    }
    try:
        response = requests.get(url, headers=headers, cookies=cookies, allow_redirects=False)
        response.raise_for_status()  # 如果响应的状态码不是200，就主动抛出异常
        tqdm.write("HTTP Status Code:", response.status_code)
        if response.status_code in (300, 301, 302, 303, 307, 308):
            # 这是一个重定向响应
            redirect_location = response.headers["Location"]
            print(f"url = {url}, Redirected to:{redirect_location}")
            with open("failed_urls.txt", 'a') as file:
                file.write(url + '\n')
            return None
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}. Error: {e}")
        with open("failed_urls.txt", 'a') as file:
            file.write(url + '\n')
        return None


def get_cities_data(province_name):
    get_city_url = "https://www.58.com/changecity.html?catepath=ershoufang&catename=%E4%BA%8C%E6%89%8B%E6%88%BF&fullpath=1,12&PGTID=0d30000c-0010-946b-60f0-26b7b9c5de50&ClickID=1"
    response_text = fetch(get_city_url)
    
    if response_text:
        # 构建动态的正则表达式
        pattern = r'"{}":\{{.*?\}}'.format(province_name)
        matches = re.finditer(pattern, response_text, re.DOTALL)
        
        province_city_dict = {}
        for match in matches:
            province_data = match.group()
            city_codes = re.findall(r'"([^"]+)":"([^"]+)"', province_data)  # 提取城市名称和代码
            
            # 构建城市字典，城市名为key，城市代码为value
            city_dict = {name: code.split('|')[0] for name, code in city_codes}
            
            # 将城市字典添加到省份城市字典
            province_city_dict[province_name] = city_dict
            
        return province_city_dict

    else:
        print("No data fetched from URL.")
        return {}



def main(provinces):
    # 用于存储所有省份及其城市的列表
    data_for_df = []
    # 使用 tqdm 包装器来显示进度条
    for province_name in tqdm(provinces, desc="Fetching Cities"):
        cities_dict = get_cities_data(province_name)
        if cities_dict:
            for city, code in cities_dict[province_name].items():
                # city即城市名称，code即城市代码前缀
                data_for_df.append({
                    "province": province_name,
                    "city_name": city,
                    "city_code_prefix": code
                })

    # 将收集的数据转换为DataFrame
    df = pd.DataFrame(data_for_df)

    # 重命名列以匹配要求的输出
    df.rename(columns={'city_code_prefix': 'city_name_prefix'}, inplace=True)

    # 确保输出目录存在
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # 输出DataFrame到CSV文件
    output_path = os.path.join(output_dir, 'cities.csv')
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"Data has been written to {output_path}")





def get_city_data_from_csv(input_name, ignore_city_names=[], csv_path='output/cities.csv'):
    # 从CSV文件中读取数据
    df = pd.read_csv(csv_path)
    
    # 初始化结果列表
    result = []
    
    # 近似匹配函数
    def is_approx_match(row_name, name):
        return name in row_name or row_name in name

    # 排除忽略城市列表中的近似匹配项
    def is_ignored(city_name):
        return any(is_approx_match(ignore_city, city_name) for ignore_city in ignore_city_names)

    # 应用近似匹配函数筛选数据
    approx_matches = df[df.apply(lambda row: (is_approx_match(row['province'], input_name) or 
                                              is_approx_match(row['city_name'], input_name)) and 
                                              not is_ignored(row['city_name']), axis=1)]

    # 将匹配项添加到结果列表中
    for _, row in approx_matches.iterrows():
        result.append({row['city_name']: row['city_name_prefix']})

    return result



# 当脚本直接运行时调用main函数
if __name__ == "__main__":
    # # 使用函数
    # provinces = [
    #     "安徽","福建","广东","广西",
    #     "贵州","甘肃","海南","河南",
    #     "黑龙江","湖北","湖南","河北",
    #     "江苏","江西","吉林","辽宁",
    #     "宁夏","内蒙古","青海","山东",
    #     "山西","陕西","四川","新疆",
    #     "西藏","云南","浙江","其他"]
    # main(provinces)

    # 例如使用
    province_or_city = "湖南"  # 或者是一个城市名称
    ignore_city_names = ["长沙"]
    city_data = get_city_data_from_csv(province_or_city, ignore_city_names)
    print(city_data)
    print(len(city_data))




