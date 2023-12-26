from selenium import webdriver
import time
import random
import csv
import pandas as pd
from lxml import etree
import re


def read_csv_to_list(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        return [row[0] for row in reader]


def fetch(url, driver):
    driver.get(url)
    time.sleep(random.uniform(10, 14))  # 等待页面加载
    return driver.page_source


def get_url_data(url, index, driver):
    try:
        text = fetch(url, driver)
        if text is None:  # 检查fetch的结果是否为None
            print(f"无法获取URL的内容：{url}")
            return
        html = etree.HTML(text)
        print(f'执行 index={index} url={url}')
        if html is None:  # 检查是否解析到HTML内容
            print(f"Failed to parse HTML for URL: {url}")
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
            print(detail)
            if index == 0:
                df.to_csv(f'58 房源级（挂牌）-数据表格.csv', mode='a', index=False, header=True,
                          encoding='utf-8-sig')
            else:
                df.to_csv(f'58 房源级（挂牌）-数据表格.csv', mode='a', index=False, header=False,
                          encoding='utf-8-sig')
    except Exception as e:
        print(f"在处理URL {url} 时发生错误: {e}")

def setup_driver():
    CHROMEDRIVER_PATH = './chromedriver-win64/chromedriver.exe'
    chrome_options = webdriver.ChromeOptions()
    # 指定 Chrome 的路径
    chrome_options.binary_location = "./chrome-win64/chrome.exe"  # 请用你的 Chrome 路径替换这里
    # 添加其他您需要的浏览器设置
    driver = webdriver.Chrome(executable_path=CHROMEDRIVER_PATH, options=chrome_options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
        Object.defineProperty(navigator, 'webdriver', {
          get: () => undefined
        })
        """
    })
    return driver


def main():
    # Setup Selenium WebDriver
    driver = setup_driver()
    driver.get("https://ld.58.com/house.shtml?PGTID=0d100000-0250-9cc9-051a-50117ce84039&ClickID=5")
    time.sleep(50)
    urls = read_csv_to_list("20231107_湖南_58_urls.csv")
    urls.reverse()
    for i, url in enumerate(urls[13000:]):
        get_url_data(url, i, driver)

    driver.quit()

if __name__ == "__main__":
    main()
