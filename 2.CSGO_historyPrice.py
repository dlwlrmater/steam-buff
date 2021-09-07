import requests
import time
import datetime
import pandas as pd
from tqdm import trange
import json

pd.set_option('display.width',None)

# 基于buff的历史售价爬取，通过对历史数据的统计分析现在价格是否合理，是否有利润空间操作

headers = {
    'Connection': 'close',
    'Cookie': '',
    'Host': 'buff.163.com',
    'Referer': 'https://buff.163.com/market/?game=csgo',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
            }
start = datetime.datetime.now()
time_now = time.time()
t = time.strftime('%Y%m%d_%H', time.localtime(time_now))
DF = pd.DataFrame()
#
buyer_id = []
buyer_pay_time = []
pay_method_text = []
pay_method = []
seller_id = []
paintwear = []
inspect_url = []
price = []
assetid = []
type1 = []
type2 = []
name = []
localized_name = []
localized_name2 = []
localized_name3 = []


sticker_1 = []
sticker_2 = []
sticker_3 = []
sticker_4 = []

for v in trange(1,735):
    url = 'https://buff.163.com/api/market/goods?game=csgo&page_num=' + str(v)
    # print(url)
    # 重连次数
    requests.adapters.DEFAULT_RETRIES = 5
    s = requests.Session()
    s.proxies = {'http': '121.237.148.96', 'http': '180.109.200.30'}
    s.headers = headers
    # 设置连接活跃状态为False
    s.keep_alive = False
    r = s.get(url=url, timeout=10)
    if r.content:
        r = r.json()
        # print(r)
        for index in range(len(r['data']['items'])):
            id_ = r['data']['items'][index]['id']
            print(id_)
            url1 = 'https://buff.163.com/api/market/goods/bill_order?game=csgo&goods_id='+ str(id_) +'&_=1589003079434'
            # print(url1)
            s1 = requests.Session()
            s1.proxies = {'http': '121.237.148.96', 'http': '180.109.200.30'}
            s1.headers = headers
            # 设置连接活跃状态为False
            s1.keep_alive = False
            r1 = s1.get(url=url1, timeout=10)
            r1 = r1.json()
            data_ = r1['data']
            items_ = data_['items']


            # print(items_)



            for z in range(len(items_)):
                assetid_ = items_[z]['asset_info']['assetid']
                assetid.append(assetid_)
                price_ = items_[z]['price']
                price.append(price_)
                buyer_id_ = items_[z]['buyer_id']
                buyer_id.append(buyer_id_)
                buyer_pay_time_ = items_[z]['buyer_pay_time']
                buyer_pay_time.append(time.strftime('%Y-%m-%d',time.localtime(buyer_pay_time_)))
                pay_method_text_ = items_[z]['pay_method_text']
                pay_method_text.append(pay_method_text_)
                pay_method_ = items_[z]['pay_method']
                pay_method.append(pay_method_)
                seller_id_ = items_[z]['seller_id']
                seller_id.append(seller_id_)

                #
                paintwear_ = items_[z]['asset_info']['paintwear']
                paintwear.append(paintwear_)

                info_ = items_[z]['asset_info']['info']

                if 'inspect_url' in info_.keys():
                    inspect_url_ = info_['inspect_url']
                    inspect_url.append(inspect_url_)
                else:
                    inspect_url_ = ""
                    inspect_url.append(inspect_url_)
                stickers_ = info_['stickers']
                # print(stickers_)
                ss = ['', '', '', '']
                number = []
                # print(id_)
                # print(info_)
                # print(stickers_)
                # print(stickers_['category'])
                # if stickers_['category'] is 'sticker':
                for s in range(len(stickers_)):
                    # print(stickers_[s])
                    if 'slot' in stickers_[s].keys():
                       x = stickers_[s]['slot']
                       number.append(x)
                    else:
                        number = []
                        # print('没有slot')
                if len(number) != 0:
                    for c in range(len(stickers_)):
                        for times in range(5):
                            slot_ = stickers_[c]['slot']
                            if slot_ == times:
                                ss[slot_ - 1] = stickers_[c]['name']

                        # print(ss)

                        # ss = np.array(ss).reshape(1,4)
                else:
                    ss = ss



                sticker_1.append(ss[0])
                sticker_2.append(ss[1])
                sticker_3.append(ss[2])
                sticker_4.append(ss[3])
                url2 = 'https://buff.163.com/api/market/goods/sell_order?game=csgo&goods_id=' + str(id_)
                s2 = requests.Session()
                s2.proxies = {'http': '121.237.148.96', 'http': '180.109.200.30'}
                s2.headers = headers
                # 设置连接活跃状态为False
                s2.keep_alive = False
                r2 = s2.get(url=url2, timeout=10)
                r2 = r2.json()
                # print(url2)
                # print(r2)
                name_ = r2['data']['goods_infos'][str(id_)]['name']
                name.append(name_)
                if 'category' in r2['data']['goods_infos'][str(id_)]['tags'].keys():
                    localized_name_ = r2['data']['goods_infos'][str(id_)]['tags']['category']['localized_name']
                    localized_name.append(localized_name_)
                else:
                    localized_name_ = ''
                    localized_name.append(localized_name_)
                if 'category_group' in r2['data']['goods_infos'][str(id_)]['tags'].keys():
                    localized_name2_ = r2['data']['goods_infos'][str(id_)]['tags']['category_group']['localized_name']
                    localized_name2.append(localized_name2_)
                else:
                    localized_name2 = ''
                    localized_name2.append(localized_name2_)
                # print(r2['data']['goods_infos'][str(id_)]['tags']['exterior']['localized_name'])
                if 'exterior' in r2['data']['goods_infos'][str(id_)]['tags'].keys():
                    localized_name3_ = r2['data']['goods_infos'][str(id_)]['tags']['exterior']['localized_name']
                    localized_name3.append(localized_name3_)
                else:
                    localized_name3_ = ''
                    localized_name3.append(localized_name3_)
                # print(localized_name3)
                # print('-------------------------------------------------------------------')
            print('*****************************')
    else:
        print('contect loss')
df = pd.DataFrame({'assetid':assetid,'name':name,'type1':localized_name,'type2':localized_name2,'type3':localized_name3,
                   'price':price,'买的时间':buyer_pay_time,'付费方式代码':pay_method,'付费方式':pay_method_text,
                   '磨损':paintwear,'检视link':inspect_url,
                   'sticker1':sticker_1,'sticker2':sticker_2,'sticker3':sticker_3,'sticker4':sticker_4})
df.to_json(r'C:\Users\steve\Desktop\history.json',mode='a')
df.to_csv(r'C:\Users\steve\Desktop\history.csv',mode='a')
