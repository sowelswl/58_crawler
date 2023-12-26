import requests
from lxml import etree
import random
import csv
import time
import re
import pandas as pd
import os 
from tqdm import tqdm 
from datetime import datetime 


def fetch(url):
    sleep_time = random.uniform(12, 15)  # 0到0.5秒之间的随机休眠时间
    time.sleep(sleep_time)
    cookies = {
        "f": "n",
        "commontopbar_new_city_info": "414%7C%E9%95%BF%E6%B2%99%7Ccs",
        "commontopbar_ipcity": "sh%7C%E4%B8%8A%E6%B5%B7%7C0",
        "userid360_xml": "286B7A09023A74E6428941322477FB8D",
        "time_create": "1699944838389",
        "id58": "CocIQ2TgJESjB/A2IrTJAg==",
        "city": "sh",
        "58home": "sh",
        "58tj_uuid": "97b888d1-2dc5-4338-b2b8-498934f834d2",
        "als": "0",
        "aQQ_ajkguid": "1E62B72A-D352-419C-9800-028C6DA53C7F",
        "sessid": "0609C838-4FF8-4914-9DCC-49595CC810CB",
        "ajk-appVersion": "",
        "myLat": "",
        "myLon": "",
        "mcity": "sh",
        "new_uv": "2",
        "utm_source": "market",
        "spm": "u-2d2yxv86y3v43nkddh1.BDPCPZ_BT",
        "init_refer": "https%253A%252F%252Fwww.baidu.com%252Fother.php%253Fsc.Ks0000aqqdzCLIcx9e4A67cPyrx8PIF7DTdLLAnPZGMOUKP9QfrTb8Jb-fitIj8e_uHvGh3BF1MWzG0hNPqNPZSIvTgJO7vOqo--PvOy1bccYPhzp9IQ3mXU0Kx-cJImYHlkZHhJRVfaq2AqqVmSVIIOo41VwV_YlymDYBGMHt5IUfXx7jKl8t4WOMl5a60KoEuOfc0yq-C8HyCSXD4UWUBFPn8p.DY_NR2Ar5Od66z3PrrW6ButVvkDj3n-vHwYxw_vU85YIMAQV8qhORGyAp7WIu8L6.TLFWgv-b5HDkrfK1ThPGujYknHb0THY0IAYqPH60IgP-T-qYXgK-5H00mywxIZ-suHY10ZIEThfqPH60ThPv5H00IgF_gv-b5HDdPjRzrH03njb0UgNxpyfqnHRzPHm1rHf0UNqGujYknWDYn1nkP6KVIZK_gv-b5HDzrjcv0ZKvgv-b5H00pywW5R42i-n0TA-b5Hc0mv-b5Hfsr0KWThnqPWT3n0%2526dt%253D1697351605%2526wd%253D58%2526tpl%253Dtpl_12826_",
        "fzq_h": "e7b324af1769cf0ce94bc13c2fdd31a8_1697351613209_c8d98de484b34165b613594146257639_1709519511",
        "new_session": "0",
        "xxzl_cid": "86ab111f684f4862a1f98d351c11064a",
        "xxzl_deviceid": "etIVx88ptdtmJVhnEtyb+Cyia9H/xxsypEplZLyOdXynb2mskWUSucUlT0vBLevv",
        "wmda_uuid": "1ed2b4b2ed4b2604c73b4a68c1a3785c",
        "wmda_new_uuid": "1",
        "wmda_session_id_10104579731767": "1697352675014-e8f1b5f3-003f-27a7",
        "wmda_visited_projects": "%3B10104579731767",
        "xxzlclientid": "e863bd04-6f60-4cc5-b0e7-1697352675990",
        "xxzlxxid": "pfmxdmU6o/X23ABjn1hlxoYnWGYULKaaAchywYnzMf288dG6NZalG+k01BB9MTpyOsw3",
        "www58com": "UserID=99065225231363&UserName=tctc_",
        "58cooper": "userid=99065225231363&username=tctc_",
        "58uname": "tctc_",
        "passportAccount": "atype=0&bstate=0",
        "xxzlbbid": "pfmbM3wxMDI5MnwxLjMuMXwxNjk3MzUyNzAzMjI0fEFVQklyUVZLbkMwdHBpRTMwYlE2aUpocmhRUXpkaHBnZTZsdnpCL2lxL1k9fDYxZTAyYjFmMTBmOThmY2RiYzgyMzg0MTJlNmFjOTRkXzE2OTczNTI2OTEyMTFfMTNkNjdmNjNmMmUxNGJiMzgxZDM1ODBkNDgwOTI1ZTNfMTcwOTUxOTUxMXxlNjZhNWY3OGFmZDA0OTNlOGM1ODk2ODhiODQxNzk5MV8xNjk3MzUyNzAyNjU0XzI1NQ==",
        "58_ctid": "414",
        "is_58_pc": "1",
        "commontopbar_new_city_info": "27%7C%E9%95%BF%E6%B2%99%7Ccs",
        "ctid": "414",
        "PPU": "UID=99065225231363&UN=tctc_&TT=c574a32193b8cfb7c56a02f1162c2f8f&PBODY=CwJSCk5_OxtVYbv0GIkNAqVI8Vc2bKgGy3VfTTuns7D_VYLIH23HisfdbCjSKcrGlmQ8O-NQu12H1_aTx5DAKGLmXxPq7osq0PMtmb3aXcJNGB29hSyq17M_NFgcCjeeCZUpmvU4NvE3pJuzqNhqpH1r8hoh0iAg38gCeCwn3a8"
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
        # tqdm.write(f"成功！HTTP Status Code:{response.status_code} success for {url}")
        tqdm.write(f"成功！HTTP for {url}")
        if response.status_code in (300, 301, 302, 303, 307, 308):
            # 这是一个重定向响应
            redirect_location = response.headers["Location"]
            print(f"url = {url}, Redirected to:{redirect_location}")
            with open("failed_houseInfo_urls.txt", 'a') as file:
                file.write(url + '\n')
            return None
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}. Error: {e}")
        with open("failed_houseInfo_urls.txt", 'a') as file:
            file.write(url + '\n')
        return None


def get_url_data_and_save(url, index, date_prefix, task_name, info_folder_name='output2'):
    text = fetch(url)
    if text is None:  # 检查fetch的结果是否为None
        tqdm.write(f"无法获取URL的内容：{url}")
        return
    html = etree.HTML(text)
    # print(f'执行 index={index} url={url}')
    if html is None:  # 检查是否解析到HTML内容
        tqdm.write(f"Failed to parse HTML for URL: {url}")
        return
    else:
        detail = {'城市': '',
                  '链接': url,
                  '行政区/所属区域': "",
                  '所属小区': html.xpath(
                      "//div[@class='maininfo-community-item'][.//span[contains(text(),'所属小区')]]/a[contains(@class, 'anchor anchor-weak')]/text()"),
                  '房源名称': html.xpath("/html/body/div[1]/div/div/div[3]/div[1]/div[3]/h1/text()"),
                  '户型': '',
                  '面积': '',
                  '朝向': html.xpath(
                      "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[3]/div[1]/i/text()"),
                  '楼层': html.xpath(
                      "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[1]/div[2]/text()"),
                  '建造年份': '',
                  '总价(万元)': html.xpath(
                      "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[1]/div[1]/span[1]/text()"),
                  '单价（元/平）': html.xpath(
                      "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/div[1]/text()"),
                  '安选标识': html.xpath(
                      "//div[@class='maininfo-title maininfo-title-premierhouse']//span[@class='maininfo-title-tag-name']/text()"),
                  '房源编码': '',
                  '装饰装修': html.xpath(
                      "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[2]/div[2]/text()"),
                  '房源用途': '',
                  '房源特点': "",
                  '产权性质': html.xpath(
                      "//td[@class='houseInfo-main-item-first'][.//span[contains(text(),'产权性质')]]/span[@class='houseInfo-main-item-name']/text()"),
                  '物业类型': html.xpath(
                      "//td[.//span[contains(text(),'物业类型')]]/span[@class='houseInfo-main-item-name']/text()"),
                  '产权年限': html.xpath(
                      "//td[@class='houseInfo-main-item-first'][.//span[contains(text(),'产权年限')]]/span[@class='houseInfo-main-item-name']/text()"),
                  '房本年限': html.xpath(
                      "//td[.//span[contains(text(),'房本年限')]]/span[@class='houseInfo-main-item-name']/text()"),
                  '房源核验编码': html.xpath(
                      "/html/body/div[1]/div/div/div[3]/div[2]/div[1]/div[2]/table/tbody/tr[7]/td[2]/span[2]/text()"),
                  '核验时间': '',
                  '权证号': '',
                  '房屋用途': '',
                  '房源概况': ''
                  }
        city = html.xpath("/html/body/div[1]/div/div/div[3]/div[1]/div[2]/div/a[3]/text()")
        if len(city) > 0:
            detail['城市'] = city[0].replace('二手房', '')

        house_code = html.xpath("/html/body/div[1]/div/div/div[3]/div[1]/div[4]/div[2]/text()")
        if len(house_code) > 0:
            detail['房源编码'] = house_code[0].replace('房屋编码：', '')

        area = html.xpath(
            "//div[@class='maininfo-community']/div[span[@class='maininfo-community-item-label' and contains(text(),'所属区域')]]/span[@class='maininfo-community-item-name']/a/text()")
        detail["行政区/所属区域"] = area[0]

        i_elements = html.xpath(
            '/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]//i[@class="maininfo-model-strong-num"]')
        span_elements = html.xpath(
            '/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]//span[@class="maininfo-model-strong-unit"]')
        combined_text = "".join(
            i_element.text + span_element.text for i_element, span_element in zip(i_elements, span_elements))
        detail["户型"] = "".join(combined_text)

        if detail["户型"] == '':
            elements = html.xpath(
                "//div[@class='maininfo-model-item maininfo-model-item-1']/div[@class='maininfo-model-strong']//text()")
            detail["户型"] = "".join(elements)

        i_element = html.xpath(
            '/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[2]/div[@class="maininfo-model-strong"]/i[@class="maininfo-model-strong-num"]')
        span_element = html.xpath(
            '/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[2]/div[@class="maininfo-model-strong"]/span[@class="maininfo-model-strong-unit"]')
        if len(i_element) > 0 and len(span_element) > 0:
            i_text = i_element[0].text
            span_text = span_element[0].text
            detail["面积"] = i_text + span_text

        # 补充数据获取
        if detail["面积"] == '':
            match = re.search(r'area_num:"(\d+(\.\d+)?)"', text)
            area_num = match.group(1) if match else None
            detail["面积"] = area_num

        if not detail["朝向"]:
            detail["朝向"] = html.xpath(
                "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[3]/div[1]/i/text()")

        if not detail["楼层"]:
            detail["楼层"] = html.xpath(
                "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[1]/div[2]/text()")

        age_info = html.xpath("/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[3]/div[3]/div[2]/text()")
        if len(age_info) > 0:
            detail["建造年份"] = age_info[0].split("/")[0]
            detail["房源用途"] = age_info[0].split("/")[1]

        if detail["建造年份"] == '':
            age_info = html.xpath(
                "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[3]/div[2]/text()")
            if len(age_info) > 0:
                detail["建造年份"] = age_info[0].split("/")[0]
                detail["房源用途"] = age_info[0].split("/")[1]

        if not detail["单价（元/平）"]:
            detail["单价（元/平）"] = html.xpath(
                "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[1]/text()")

        if detail["装饰装修"] == [' ']:
            detail["装饰装修"] = html.xpath(
                "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/text()")

        if detail["总价(万元)"] == ['室']:
            detail["总价(万元)"] = html.xpath("//div[@class='maininfo-price-wrap']//text()")

        house_feature = html.xpath('//div[@class="maininfo-tags"]/span[@class="maininfo-tags-item"]/text()')
        detail["房源特点"] = " ".join(house_feature)

        div_element = html.xpath(
            '/html/body/div[1]/div/div/div[3]/div[2]/div[1]/div[3]/div[2]/div/div/div[2]/div[2]/div')
        if len(div_element) > 0:
            text_content = div_element[0].xpath('.//text()')
            detail["房源概况"] = ' '.join(text.strip() for text in text_content if text.strip())

        for key in detail:
            if isinstance(detail[key], list) and len(detail[key]) > 0:
                detail[key] = detail[key][0].replace(' ', '').replace('\n', '')
            elif isinstance(detail[key], list) and len(detail[key]) == 0:
                detail[key] = ""
        df = pd.DataFrame([detail])
        # print(detail)

        # Add collection date by date_prefix string YYYYmmdd to datetime date YYYY-mm-dd
        # df['采集日期'] = datetime.strptime(date_prefix, '%Y%m%d').date()

        if index == 0:
            df.to_csv(f'{info_folder_name}/{date_prefix}_{task_name}_58 房源级（挂牌）-数据表格.csv', mode='a', index=False, header=True,
                      encoding='utf-8-sig')
        else:
            df.to_csv(f'{info_folder_name}/{date_prefix}_{task_name}_58 房源级（挂牌）-数据表格.csv', mode='a', index=False, header=False,
                      encoding='utf-8-sig')


def read_csv_to_list(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        return [row[0] for row in reader]


def replace_now_time_in_url(url: str) -> str:
    """
    替换URL中的now_time值为当前的Unix时间戳
    """
    current_time = int(time.time())
    if 'now_time=' in url:
        old_time = url.split('now_time=')[1].split('&')[0]  # 提取URL中的now_time的值
        return url.replace(f'now_time={old_time}', f'now_time={current_time}')
    return url



def get_csv_paths(folder_path):
    # Initialize an empty DataFrame with the desired column names
    df = pd.DataFrame(columns=['date', 'city_name', 'path'])
    
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print("The folder path does not exist.")
        return df
    
    # Check if the folder is indeed a directory
    if not os.path.isdir(folder_path):
        print("The provided path is not a directory.")
        return df
    
    records = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                parts = file.split('_')
                # Check if the filename split contains at least two elements (date and city name)
                if len(parts) >= 3:
                    date = parts[0]
                    # Combine all parts except the first (date) and the last (.csv) to get the city name
                    # city_name = '_'.join(parts[1:-1])
                    city_name = parts[1]
                    record = {
                        'date': date,
                        'city_name': city_name,
                        'path': os.path.join(root, file)
                    }
                    records.append(record)

    # Only if records were found, create the DataFrame
    if records:
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y%m%d')
        df = df.sort_values(by=['date', 'city_name'])
    
    return df



def process_urls(urls, task_name, date_prefix, is_delete=False, info_folder_name='output2'):
    """
    Process a list of URLs, updating their timestamp and saving their data.

    :param urls: List of URLs to process.
    :param task_name: Task name to be used in saving the data.
    :param date_prefix: Maximum date to use as a prefix in filenames.
    :param is_delete: Boolean to indicate if the existing CSV file should be deleted before starting.
    :param info_folder_name: Folder name where the information will be saved.
    """
    tqdm_desc = "Processing URLs"
    tqdm_total = len(urls)
    file_name = f'{info_folder_name}/{date_prefix}_{task_name}_58 房源级（挂牌）-数据表格.csv'
    consecutive_failures = 0  # Initialize a counter for consecutive failures

    # Check if is_delete is True and if the file exists, then delete it
    if is_delete and os.path.exists(file_name):
        os.remove(file_name)
        print(f"Existing file {file_name} has been deleted.")

    with tqdm(total=tqdm_total, desc=tqdm_desc) as pbar:
        for index, url in enumerate(urls):
            while True:
                try:
                    updated_url = replace_now_time_in_url(url)
                    get_url_data_and_save(updated_url, index, date_prefix, task_name, info_folder_name=info_folder_name)
                    consecutive_failures = 0  # Reset the failure counter after a success
                    break  # Break the while loop on success
                except Exception as e:
                    consecutive_failures += 1  # Increment the failure counter
                    print(f"An error occurred with URL {index}: {e}")
                    if consecutive_failures < 3:
                        print("Skipping to the next URL...")
                        break  # Skip the rest of the while loop and continue with the next URL
                    else:
                        print(f"Pausing for 30 minutes before retrying due to {consecutive_failures} consecutive failures...")
                        time.sleep(1800)  # Pause for 30 minutes
                        consecutive_failures = 0  # Reset the failure counter after the pause
                        # Do not break here; the while loop will attempt the same URL again
            # Manually update the progress bar after attempting each URL
            pbar.update(1)


def main():
    url_folder_name = 'output'
    info_folder_name = 'output2'
    task_name = '湖南'
    updated_url = "https://cs.58.com/ershoufang/3209739933475850x.shtml?auction=220&hpType=60&entry=102&position=22&kwtype=shangquan&now_time=1699336344&typecode=220&region_ids=418&trading_area_ids=8188&spread=commsearch_c&epauction=&stats_key=47001569-7a26-4923-b097-8251ba35f304_22&from=from_esf_List_screen&index=22"
    get_url_data_and_save(updated_url, 1, "test", task_name, info_folder_name=info_folder_name)
    # Retrieve paths to CSV files containing URLs
    # df_url_paths = get_csv_paths(os.path.join(os.getcwd(), url_folder_name))
    # # df_info_paths = get_csv_paths(os.path.join(os.getcwd(), info_folder_name))
    #
    #
    #
    # # Find paths and the maximum date for the given task name
    # paths = df_url_paths[df_url_paths['city_name'] == task_name]['path'].tolist()
    #
    #
    #
    # max_date = df_url_paths[df_url_paths['city_name'] == task_name]['date'].max()
    #
    # def read_csv_check_header(path):
    #     # 尝试读取第一行来判断是否存在列名称
    #     with open(path, 'r', encoding='utf-8-sig') as file:
    #         first_line = file.readline()
    #         # 假设如果列名存在，那么第一个逗号之前的内容应该是其中一个预期的列名
    #         if 'city_name' in first_line or 'url' in first_line:
    #             # 如果第一行是列名，直接读取CSV
    #             df = pd.read_csv(path)
    #         else:
    #             # 如果第一行不是列名，指定列名并从文件头部开始读取
    #             df = pd.read_csv(path, names=['city_name', 'url'], header=None)
    #
    #     return df
    #
    # # Compile all URLs from the CSV files into a list
    # urls = []
    #
    # for path in paths:
    #     # 使用定义好的函数读取CSV文件
    #     url_df = read_csv_check_header(path)
    #     # 只获取url列
    #     urls.extend(url_df['url'].tolist())
    #
    # # # drop duplicate in urls
    # urls = list(set(urls))
    #
    # print(len(urls))
    #
    # # Use the function to process URLs
    # process_urls(urls, task_name, max_date, is_delete=False, info_folder_name='output2')

if __name__ == '__main__':
    main()




# if __name__ == '__main__':
#     url_folder_name = 'output'
#     info_folder_name = 'output2'
#     df_url_paths = get_csv_paths(os.path.join(os.getcwd(), url_folder_name))
#     # df_info_paths = get_csv_paths(os.path.join(os.getcwd(), info_folder_name))

#     # print(df_url_paths)

#     task_name = '湖南part'

#     paths = df_url_paths[df_url_paths['city_name'] == task_name]['path'].tolist()

#     max_date = df_url_paths[df_url_paths['city_name'] == task_name]['date'].max()

#     urls = []
#     for path in paths:
#         urls.extend(pd.read_csv(path)['url'].tolist())

#     print(urls)

#     # for i, url in tqdm(enumerate(urls[0:])):
#     #     updated_urls = replace_now_time_in_url(url)
#     #     get_url_data(updated_urls, i)

#     # Initialize tqdm progress bar with the total count and a description
#     tqdm_desc = "Processing URLs"

#     tqdm_total = len(urls)
#     date_prefix = max_date
#     with tqdm(total=tqdm_total, desc=tqdm_desc) as pbar:
#         # Start processing the URLs
#         for index, url in enumerate(urls):
#             # Update the URL to replace the timestamp with the current time
#             updated_url = replace_now_time_in_url(url)

            
#             # Get data from the updated URL
#             get_url_data_and_save(updated_url, index, date_prefix, task_name)
#             # Manually update the progress bar after processing each URL
#             pbar.update(1)
