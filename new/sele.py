import time
import random
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def fetch(url, driver):
    driver.get(url)
    time.sleep(random.uniform(12, 18))  # 等待页面加载
    try:
        # 等待 总价(万元) 这个元素完全加载，然后继续处理，如果已出现，则直接不等待
        element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[1]/div/div/div[3]/div[2]/div[2]/div[1]/div[2]/div[1]/div[1]/span[1]"))
        )
    except Exception as e:
        # with open("{}.html".format(url), "w", encoding="utf-8-sig") as f:
        #     f.write(driver.page_source)
        print(e)
        print("获取元素 总价 超时")

    return driver.page_source

def setup_driver():
    CHROMEDRIVER_PATH = '../chromedriver-win64/chromedriver.exe'
    chrome_options = webdriver.ChromeOptions()
    # 指定 Chrome 的路径
    chrome_options.binary_location = "../chrome-win64/chrome.exe"  # 请用你的 Chrome 路径替换这里
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
