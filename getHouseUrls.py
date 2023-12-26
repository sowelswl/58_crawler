import requests
from lxml import etree
import random
import time
import re
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os

def fetch(url):
    # sleep_time = random.uniform(10, 12)  # 0到0.5秒之间的随机休眠时间
    sleep_time = random.uniform(3, 5)  # 0到0.5秒之间的随机休眠时间
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
        if response.status_code == 200:
            tqdm.write(f"返回成功 for {url},")
        if response.status_code in (300, 301, 302, 303, 307, 308):
            # 这是一个重定向响应
            redirect_location = response.headers["Location"]
            tqdm.write(f"url = {url}, Redirected to:{redirect_location}")
            with open("failed_urls.txt", 'a') as file:
                file.write(url + '\n')
            return None
        return response.text
    except requests.RequestException as e:
        tqdm.write(f"Error fetching {url}. Error: {e}")
        with open("failed_urls.txt", 'a') as file:
            file.write(url + '\n')
        return None


def parse_second_layer(current_url):
    hrefs = []
    # Create the output directory if it doesn't exist
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Create a CSV file with a date prefix in the 'output' directory
    date_prefix = datetime.now().strftime('%Y%m%d_')
    file_path = os.path.join(output_dir, 'raw', f'{date_prefix}raw_58_urls.csv')

    while True:
        text = fetch(current_url)
        if not text:
            return []
        html = etree.HTML(text)
        if html is not None:
            links = html.xpath('//*[@id="esfMain"]/section/section[3]/section[1]/section[2]//@href')
            # print(f"current url={current_url}, len={len(links)} urls={links}")
            hrefs.extend(links)
            # Append links to the CSV file
            with open(file_path, 'a', encoding='utf-8') as f:
                for link in links:
                    f.write(link + '\n')
            # Check for the "next page" button
            next_page_links = html.xpath("//a[contains(text(), '下一页')]/@href")
            tqdm.write(f"Next page url = {next_page_links}")
            if next_page_links and next_page_links[0] != "javascript:void(0);":
                current_url = next_page_links[0]
            else:
                break
        else:
            break
    return hrefs


def extract_sub_json(src):
    # 找到关键字的最后一次出现的位置
    keyword_pos = src.rfind("地铁线路二手房")
    if keyword_pos == -1:
        return None

    # 从关键字位置开始，向后查找 JSON 的结束位置
    stack = []
    end_pos = None
    for i in range(keyword_pos, len(src)):
        if src[i] == '{':
            stack.append('{')
        elif src[i] == '}':
            if len(stack) == 0:
                end_pos = i
                break
            else:
                stack.pop()

    if end_pos is None:
        return None

    # 从关键字位置开始，向前查找 JSON 的起始位置
    stack = []
    start_pos = None
    for i in range(keyword_pos, 0, -1):
        if src[i] == '}':
            stack.append('}')
        elif src[i] == '{':
            if stack:
                stack.pop()
            if not stack:  # 如果堆栈为空，我们找到了起始位置
                start_pos = i
                break

    if start_pos is None:
        return None
    # 提取 JSON
    json_str = src[start_pos:end_pos + 1]

    # 用双引号替换单引号
    json_str = re.sub(r"(\w+)'", r'"\1"', json_str)
    json_str = json_str.replace("'", '"')
    url_matches = re.findall(r'url:"(https:\\u002F\\u002F[^"]+)"', json_str)

    # Convert the Unicode escape sequences to their corresponding characters
    urls = [url.encode('utf-8').decode('unicode_escape') for url in url_matches]
    return urls


def parse_first_layer(current_url):
    text = fetch(current_url)
    if not text:
        return []
    html = etree.HTML(text)
    hrefs = html.xpath('/html/body/div[1]/div/div/section/section[2]/div/section/div[1]/section/ul//a/@href')
    hrefs = hrefs[1:]
    sub_url = extract_sub_json(text)
    hrefs = hrefs + sub_url
    return hrefs


def get_url_data(url):
    text = fetch(url)
    if not text:
        return []
    html = etree.HTML(text)
    if html is not None:
        links = html.xpath('//*[@id="esfMain"]/section/section[2]/div/section/div[1]/section/ul[2]//a/@href')
        links.pop(0)
        return [parse_second_layer(new_url) for new_url in links]
    else:
        return []


def flatten(lst):
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result

def get_city_data_from_csv(input_name, ignore_city_names=[], csv_path='src/cities.csv'):
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


# def get_urls_set(city_list):
#     for city_name in city_list:
#         url = "https://" + city_name + ".58.com/ershoufang/"
#         urls = parse_first_layer(url)
#         print(urls)
#         print(len(urls))
#         results = [get_url_data(url) for url in urls]
#         flat_list = flatten(results)

#         if len(flat_list) == 0:
#             print("获取页面为空，原有cookie过，请获取最新cookie替换——————")
#     file_name = '58_hunan_urls.csv'

#     # 读取CSV文件
#     df = pd.read_csv(file_name, header=None, names=['url'])

#     # 定义正则表达式模式
#     pattern = r'ershoufang/(\d+)x\.shtml'

#     # 应用正则表达式提取数字
#     df['extracted_number'] = df['url'].apply(
#         lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else None)

#     # 去除重复项
#     df_unique = df.drop_duplicates(subset='extracted_number')

#     # 只保留提取的数字列
#     df_unique = df_unique[['url']]

#     # 保存结果到新的CSV文件
#     df_unique.to_csv('58_hunan_unique_url.csv', index=False)



# def get_urls_set_and_save(city_data_list, task_name):
#     # 生成文件名，包含当前日期和城市名称
#     date_prefix = datetime.now().strftime('%Y%m%d')  # 获取当前日期
#     # 确保output目录存在
#     if not os.path.exists('output'):
#         os.makedirs('output')
#     file_name = f'output/{date_prefix}_{task_name}_58_urls.csv'
#     # 在for循环之前重置csv文件，如果文件存在则删除

#     # if os.path.exists(file_name):
#     #     os.remove(file_name)
#     #     print(f"已存在的文件 {file_name} 已被删除.")

#     # 遍历每个城市的数据
#     for city_data in tqdm(city_data_list, desc="Processing cities", unit="city"):
#         for city_name, city_prefix in city_data.items():
#             url = f"https://{city_prefix}.58.com/ershoufang/"
#             urls = parse_first_layer(url)
            
#             tqdm.write(f"获取{city_name}第一层数据完成,共{len(urls)}个子分区")
#             print(urls)

#             results = []
#             for url in tqdm(urls, desc=f"Fetching URLs for {city_name}", leave=False, unit="子分区"):
#                 data = get_url_data(url)
#                 if data:
#                     results.append(data)
#                 else:
#                     tqdm.write(f"{city_name}该子分区{url}没有数据")
            
#             results = flatten(results)
#             if len(results) == 0:
#                 print("获取页面为空，原有cookie过，请获取最新cookie替换——————")
                

#             if results:
#                 # 将数据转换为DataFrame
#                 df = pd.DataFrame(results)
#                 # 定义正则表达式模式
#                 # pattern = r'ershoufang/(\d+)x\.shtml'
#                 pattern2 = r'/(\d+)x\.shtml'
#                 # 使用正则表达式从唯一的列中提取第一次遇到的连续数字
#                 df['数字'] = df[0].str.extract(pattern2)[0]
#                 # 根据 '数字' 列去重
#                 df_unique = df.drop_duplicates(subset='数字')
#                 # reset index
#                 df_unique = df_unique.reset_index(drop=True)
#                 # rename 0 column to url 
#                 df_unique = df_unique.rename(columns={0:'url'})
#                 # drop '数字' 
#                 df_unique.drop(columns=['数字'], inplace=True)


#                 # file_name = f'output/{date_prefix}_{city_name}_58_urls.csv'
#                 df_unique['city_name'] = city_name  # 直接在这里新增城市名称

#                 df_unique = df_unique[['city_name','url']]  # 重置列顺序,把城市名称列放在前面

#                 # 如果是首次写入，header设置为True以写入列名，否则设置为False
#                 with open(file_name, 'a', encoding='utf-8-sig', newline='') as f:
#                     df_unique.to_csv(f, header=not os.path.exists(file_name), index=False)

#                 tqdm.write(f"{city_name}数据已保存为 {file_name}")
#             else:
#                 print(f"获取页面为空，原有cookie过期，请获取最新cookie替换。City: {city_name}")


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
                    urls = parse_first_layer(url)
                    tqdm.write(f"获取{city_name}第一层数据完成,共{len(urls)}个子分区")
                    print(urls)

                    results = []
                    for url in tqdm(urls, desc=f"Fetching URLs for {city_name}", leave=False, unit="子分区"):
                        data = get_url_data(url)
                        if data:
                            results.append(data)
                        else:
                            tqdm.write(f"{city_name}该子分区{url}没有数据")

                    results = flatten(results)
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
                        df_unique = df_unique.rename(columns={0:'url'})
                        df_unique.drop(columns=['数字'], inplace=True)
                        df_unique['city_name'] = city_name
                        df_unique = df_unique[['city_name','url']]

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

# 调用函数的示例：
# 假设 city_data_list 是 get_city_data_from_csv_approx 函数的输出
if __name__ == '__main__':
    # city_data_list = get_city_data_from_csv("湖南", ignore_city_names=["长沙"])
    city_data_list = get_city_data_from_csv("湖南")
    # city_data_list = get_city_data_from_csv("长沙")
    # city_data_list = get_city_data_from_csv("沅江")
    # get_urls_set_and_save(city_data_list)
    print(city_data_list)
    # get_urls_set_and_save(city_data_list[22:], task_name='湖南', is_delete=False)


    