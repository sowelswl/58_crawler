from selenium import webdriver
import time
import random
import re
import sele
from lxml import etree


def first_get_urls(page):
    first_layer_urls = []
    regex_url_mark = r'url_mark:"([^"]+)"'
    regex_xiaoqu = r'xiaoqu\/(\d+)\/'

    # 找出所有匹配的 url_mark
    matches_url_mark = re.findall(regex_url_mark, page)
    first_layer_urls.extend(matches_url_mark)

    # 找出所有匹配的 xiaoqu 数字
    matches_xiaoqu = re.findall(regex_xiaoqu, page)
    first_layer_urls.extend(matches_xiaoqu)
    print(len(first_layer_urls))
    print(first_layer_urls)
    print("第一层解析执行结束")
    return first_layer_urls


def second_get_urls(prefix_url, first_layer_urls, driver):
    second_layer_urls = []
    for url in first_layer_urls:
        if "ditiefang" in url or "huanxian" in url:
            continue
        print("currentUrl= " + prefix_url + url)
        page = sele.fetch(prefix_url + url, driver)
        sub_second_layer_urls = first_get_urls(page)
        second_layer_urls.extend(sub_second_layer_urls)
        print(len(sub_second_layer_urls))
        print(sub_second_layer_urls)
    print("第二层解析执行结束")
    return second_layer_urls


def third_get_urls(prefix_url, second_layer_urls, driver):
    final_urls = []
    for url in second_layer_urls:
        current_url = prefix_url + url
        while True:
            page = sele.fetch(current_url, driver)
            tree = etree.HTML(page)

            # 使用 XPath 定位到所有 class="list-cell" 下的 class="li-row" 的 a 标签
            li_rows = tree.xpath('//div[@class="list-cell"]/a[@class="li-row"]/@href')
            final_urls.extend(li_rows)
            next_page = tree.xpath('//a[@class="next next-active"]/@href')

            # 检查是否找到该元素 判断是否存在下一页
            if next_page:
                # 获取 href 属性
                current_url = next_page[0]
            else:
                break
    print(len(final_urls))
    print(final_urls)
    return final_urls
