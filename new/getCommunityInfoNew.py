from lxml import etree
import pandas as pd


def getCommunityInfo(index, city, url, page):
    tree = etree.HTML(page)
    print(page)

    detail = {'城市': city, '链接': url,
              '行政区/所属区域': tree.xpath('//*[@id="__layout"]/div/div[2]/div[1]/div[2]/div/a[3]/text()')[0].replace(
                  "小区", ""),
              "小区名": tree.xpath('//*[@id="__layout"]/div/div[2]/div[2]/div/h1/text()'),
              "地址": tree.xpath('//*[@id="__layout"]/div/div[2]/div[2]/div/p/text()'),
              "均价": tree.xpath('//*[@id="__layout"]/div/div[2]/div[3]/div[2]/div[1]/div[1]/div/span[1]/text()'),
              "物业类型": "",
              "权属类别": "",
              "竣工时间": "",
              "产权年限": "",
              "总户数": "",
              "总建面积": "",
              "容积率": "",
              "绿化率": "",
              "建筑类型": "",
              "所属商圈": "",
              "统一供暖": "",
              "供水供电": "",
              "停车位": "",
              "物业费": "",
              "车位管理费": "",
              "物业公司": "",
              "小区地址": "",
              "开发商": "",
              }

    # 使用 XPath 提取 label 和 value，构建字典
    info_dict = {}
    for column in tree.xpath(
            '//div[@class="info-list"]/div[contains(@class, "column-2") or contains(@class, "column-1")]'):
        label = column.xpath('.//div[@class="label"]/text()')
        value = column.xpath('.//div[contains(@class, "value")]/text()')
        if label and value:
            info_dict[label[0].strip()] = value[0].strip()

    for key, value in info_dict.items():
        # 如果 dictB 也有这个键，则更新其值
        if key in detail:
            detail[key] = value

    down_flag = tree.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), 'arrow down')]")
    up_flag = tree.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), 'arrow up')]")

    if len(down_flag) > 0:
        detail["趋势"] = "下降"
    elif len(up_flag) > 0:
        detail["趋势"] = "上涨"
    else:
        detail["趋势"] = "持平"

    detail["幅度"] = tree.xpath('//*[@id="__layout"]/div/div[2]/div[3]/div[2]/div[1]/div[1]/div/span[5]/text()')

    for key, value in detail.items():
        if isinstance(value, list) and len(value) > 0:
            detail[key] = value[0]
    df = pd.DataFrame([detail])

    # CSV文件的名称
    filename = '{}_community_output.csv'.format(city)

    # 判断是否需要包含头部
    if index == 0:
        df.to_csv(filename, index=False, mode='a', encoding='utf-8-sig')  # 包含头部
    else:
        df.to_csv(filename, index=False, header=False, mode='a', encoding='utf-8-sig')  # 不包含头部
