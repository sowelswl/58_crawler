import requests
from lxml import etree
import random
import time
import re
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os
import sele


def parse_second_layer(current_url, driver):
    hrefs = []
    # Create the output directory if it doesn't exist
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    # Create a CSV file with a date prefix in the 'output' directory
    date_prefix = datetime.now().strftime('%Y%m%d_')
    file_path = os.path.join(output_dir, 'raw', f'{date_prefix}raw_58_urls.csv')

    while True:
        text = sele.fetch(current_url, driver)
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


def parse_first_layer(current_url, driver):
    text = sele.fetch(current_url, driver)
    if not text:
        return []
    html = etree.HTML(text)
    hrefs = html.xpath('/html/body/div[1]/div/div/section/section[2]/div/section/div[1]/section/ul//a/@href')
    hrefs = hrefs[1:]
    sub_url = extract_sub_json(text)
    hrefs = hrefs + sub_url
    return hrefs


def get_url_data(url, driver):
    text = sele.fetch(url, driver)
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