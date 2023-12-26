import new.sele
import time
import new.getCommunityUrlsNew
import new.getCommunityInfoNew
import new.getHouseUrlsNew
import new.getHouseInfoNew
import csv
import pandas as pd

import requests
from lxml import etree
import random
import time
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os

myDriver = new.sele.setup_driver()
myDriver.get("https://cs.58.com/xiaoqu/?PGTID=0d003508-0000-0b03-05f8-42852eed7dbe&ClickID=1")
time.sleep(20)
# 点击登录，通过短信登录/ 手机58扫码登录


# 获取 小区 urls list----------------------------
city = "cs"
kind = "xiaoqu"
main_url = "https://{}.58.com/{}/".format(city, kind)
myDriver.get(main_url)
page = myDriver.page_source
first_layer_urls = new.getCommunityUrlsNew.first_get_urls(page)
first_layer_urls = list(set(first_layer_urls))

second_layer_urls = new.getCommunityUrlsNew.second_get_urls(main_url, first_layer_urls, myDriver)
second_layer_urls = list(set(second_layer_urls))
second_layer_urls_filtered = [item for item in second_layer_urls if item not in first_layer_urls]

all_final_urls = new.getCommunityUrlsNew.third_get_urls(main_url, second_layer_urls_filtered, myDriver)
all_final_urls = list(set(all_final_urls))

filename = '{}_{}.csv'.format(city, kind)

# 使用 'with' 语句确保文件正确关闭
with open(filename, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # 写入数据，每个元素一行
    for item in all_final_urls:
        writer.writerow([item])  # 注意每个元素被放在列表中

print(f'数据已保存到 {filename}')

# 获取 小区 info -----------------------

file_path = '{}_xiaoqu.csv'.format(city)  # 替换为您的文件路径

# 读取 CSV 文件
# 假设文件只有一列，没有头部（列名）
df = pd.read_csv(file_path, header=None)
one_dimensional_list = df[0].tolist()
one_dimensional_list = one_dimensional_list[0:20]
for i in range(len(one_dimensional_list)):
    try:
        new.getCommunityInfoNew.getCommunityInfo(i, city, one_dimensional_list[i],
                                                 new.sele.fetch(one_dimensional_list[i], myDriver))
    except Exception as e:
        print(f"在处理元素 {one_dimensional_list[i]} 时发生了异常：{e}")


# 获取 二手房 url  -----------------------

def get_urls_set_and_save(city_data_list, task_name, is_delete=True):
    date_prefix = datetime.now().strftime('%Y%m%d')
    if not os.path.exists('output'):
        os.makedirs('output')
    file_name = f'output/{date_prefix}_{task_name}_58_urls.csv'

    # 根据is_delete决定是否删除文件
    if is_delete and os.path.exists(file_name):
        os.remove(file_name)
        print(f"已存在的文件 {file_name} 已被删除.")

    for city_data in tqdm(city_data_list, desc="Processing cities", unit="city"):
        for city_name, city_prefix in city_data.items():
            while True:  # 添加一个while循环以便在获取数据失败时重新尝试
                try:
                    url = f"https://{city_prefix}.58.com/ershoufang/"
                    urls = new.getHouseUrlsNew.parse_first_layer(url, myDriver)
                    tqdm.write(f"获取{city_name}第一层数据完成,共{len(urls)}个子分区")
                    print(urls)

                    results = []
                    for url in tqdm(urls, desc=f"Fetching URLs for {city_name}", leave=False, unit="子分区"):
                        data = new.getHouseUrlsNew.get_url_data(url, myDriver)
                        if data:
                            results.append(data)
                        else:
                            tqdm.write(f"{city_name}该子分区{url}没有数据")

                    results = new.getHouseUrlsNew.flatten(results)
                    if len(results) == 0:
                        print("获取页面为空，原有cookie过，请获取最新cookie替换——————")
                        time.sleep(600)  # 如果没有结果，休息10分钟
                        continue  # 跳过当前循环的剩余部分并重新尝试

                    if results:
                        df = pd.DataFrame(results)
                        pattern2 = r'/(\d+)x\.shtml'
                        df['数字'] = df[0].str.extract(pattern2)[0]
                        df_unique = df.drop_duplicates(subset='数字')
                        df_unique = df_unique.reset_index(drop=True)
                        df_unique = df_unique.rename(columns={0: 'url'})
                        df_unique.drop(columns=['数字'], inplace=True)
                        df_unique['city_name'] = city_name
                        df_unique = df_unique[['city_name', 'url']]

                        # with open(file_name, 'a', encoding='utf-8-sig', newline='') as f:
                        #     df_unique.to_csv(f, header=not os.path.exists(file_name), index=False)

                        with open(file_name, 'a', encoding='utf-8-sig', newline='') as f:
                            # Check if the file exists and if it is empty (which implies a header is needed).
                            # If the file does not exist or is empty, write the header, otherwise do not.
                            file_exists = os.path.exists(file_name) and os.path.getsize(file_name) > 0
                            # Write the dataframe to the CSV without an index.
                            df_unique.to_csv(f, header=not file_exists, index=False)

                        tqdm.write(f"{city_name}数据已保存为 {file_name}")
                        break  # 成功获取数据后退出while循环
                    else:
                        print(f"获取页面为空，原有cookie过期，请获取最新cookie替换。City: {city_name}")
                        time.sleep(600)  # 如果没有结果，休息10分钟
                        continue  # 跳过当前循环的剩余部分并重新尝试
                except Exception as e:
                    print(f"处理 {city_name} 时遇到错误: {e}")
                    time.sleep(1800)  # 遇到异常，休息30分钟后继续


# 获取 二手房 info  -----------------------
urls = new.getHouseInfoNew.read_csv_to_list("20231107_湖南_58_urls.csv")
for i, url in enumerate(urls):
    new.getHouseInfoNew.get_url_data(url, i, myDriver)

myDriver.quit()
