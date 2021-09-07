import requests
import time
import pandas as pd
import datetime
import numpy as np
from tqdm import trange
from whatibuy import Buy
import pymysql

# 基于buff平台的定时爬取
# 通过正点定时 爬取所有种类的物品详细信息及价格并导入Mysql中，方便后续分析处理
# 但是每次爬取之间需要2s间隔，防止buff发现爬虫并封锁账号

# 每次爬取之前需要更新cookie
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
# 735
for i in trange(1,750):
# for i in trange(1,3):
    url = 'https://buff.163.com/api/market/goods?game=csgo&page_num=' + str(i)
    # print(url)
    # 重连次数
    requests.adapters.DEFAULT_RETRIES = 5
    # 保持会话
    s = requests.Session()
    s.proxies = {'http':'121.237.148.96','http':'180.109.200.30'}
    s.headers=headers
    # 设置连接活跃状态为False
    s.keep_alive =False
    r = s.get(url=url,timeout = 10)
    r= r.json()
    # print(r)
    # print(r)
    # print(url)
    # print(i)
    code_ = r['code']
    df = []
    try:
        if code_ == 'OK':

            data_ = r['data']
            # print(url)

            # print('1')
            items_ = data_['items']
            buy_max_price = []
            buy_num = []

            id = []
            market_hash_name = []
            market_min_price = []
            name = []
            quick_price = []
            sell_min_price = []
            sell_num = []
            sell_reference_price = []
            steam_market_url = []
            steam_price = []
            steam_price_cny = []

            exterior_internal_name = []
            exterior_localized_name = []

            quality_internal_name = []
            quality_localized_name = []

            rarity_internal_name = []
            rarity_localized_name = []

            type_internal_name = []
            type_localized_name = []

            weapon_internal_name = []
            weapon_localized_name = []


            buff_url = []


            for index in range(len(items_)):
                buy_max_price_ = items_[index]['buy_max_price']
                buy_num_ = items_[index]['buy_num']
                id_ = items_[index]['id']
                market_hash_name_ = items_[index]['market_hash_name']
                market_min_price_ = items_[index]['market_min_price']
                name_ = items_[index]['name']
                quick_price_ = items_[index]['quick_price']
                sell_min_price_ = items_[index]['sell_min_price']
                sell_num_ = items_[index]['sell_num']
                sell_reference_price_ = items_[index]['sell_reference_price']
                steam_market_url_ = items_[index]['steam_market_url']
                goods_info_ = items_[index]['goods_info']
                steam_price_ = goods_info_['steam_price']
                steam_price_cny_ = goods_info_['steam_price_cny']
                tags_ = goods_info_['info']['tags']

                if 'exterior' in tags_.keys():
                    exterior_internal_name_ = tags_['exterior']['internal_name']
                    exterior_localized_name_ = tags_['exterior']['localized_name']
                else:
                    exterior_internal_name_ = ''
                    exterior_localized_name_ = ''

                quality_internal_name_ = tags_['quality']['internal_name']
                quality_localized_name_ = tags_['quality']['localized_name']

                rarity_internal_name_ = tags_['rarity']['internal_name']
                rarity_localized_name_ = tags_['rarity']['localized_name']

                if 'type' in tags_.keys():
                    type_internal_name_ = tags_['type']['internal_name']
                    type_localized_name_ = tags_['type']['localized_name']
                else:
                    type_internal_name_ = ''
                    type_localized_name_ = ''

                if 'weapon' in tags_.keys():
                    weapon_internal_name_ = tags_['weapon']['internal_name']
                    weapon_localized_name_ = tags_['weapon']['localized_name']
                else:
                    weapon_internal_name_ = ''
                    weapon_localized_name_ = ''

                buff_url_ = 'https://buff.163.com/market/goods?goods_id=' + str(id_)

                buy_max_price.append(buy_max_price_)
                buy_num.append(buy_num_)
                id.append(id_)
                market_hash_name.append(market_hash_name_)
                market_min_price.append(market_min_price_)
                name.append(name_)
                quick_price.append(quick_price_)
                sell_min_price.append(sell_min_price_)
                sell_num.append(sell_num_)
                sell_reference_price.append(sell_reference_price_)
                steam_market_url.append(steam_market_url_)
                steam_price.append(steam_price_)
                steam_price_cny.append(steam_price_cny_)

                exterior_internal_name.append(exterior_internal_name_)
                exterior_localized_name.append(exterior_localized_name_)

                quality_internal_name.append(quality_internal_name_)
                quality_localized_name.append(quality_localized_name_)

                rarity_internal_name.append(rarity_internal_name_)
                rarity_localized_name.append(rarity_localized_name_)

                type_internal_name.append(type_internal_name_)
                type_localized_name.append(type_localized_name_)

                weapon_internal_name.append(weapon_internal_name_)
                weapon_localized_name.append(weapon_localized_name_)
                buff_url.append(buff_url_)


            df = pd.DataFrame({'buff_id':id,'中文名':name,'英文名':market_hash_name,'buff_最低售价':sell_min_price,'buff_在售数':sell_num,'buff_最高求购价':buy_max_price,'buff_求购数':buy_num,
                               'steam价格_rmb':steam_price_cny,
                               '外观_中文':exterior_localized_name,'外观_英文':exterior_internal_name,'类别_中文':quality_localized_name,'类别_英文':quality_internal_name,
                               '品质_中文':rarity_localized_name,'品质_英文':rarity_internal_name,'武器种类_中文':type_localized_name,'武器种类_英文':rarity_internal_name,
                               '武器名称_中文':weapon_localized_name,'武器名称_英文':weapon_internal_name,'steamurl':steam_market_url,'buff_url':buff_url})
            # 把 steam价格_rmb & buff_最低售价 列从object 变成 float
            df.loc[:, 'steam价格_rmb'] = pd.DataFrame(df.loc[:,'steam价格_rmb'],dtype=np.float)
            df.loc[:, 'buff_最低售价'] = pd.DataFrame(df.loc[:,'buff_最低售价'], dtype=np.float)
            df.loc[:, 'buff_最高求购价'] = pd.DataFrame(df.loc[:, 'buff_最高求购价'], dtype=np.float)
            steamgetpirce = df.loc[:,'steam价格_rmb'] * 0.8659
            buffprice = df.loc[:,'buff_最低售价']
            buyprice = df.loc[:,'buff_最高求购价']
            # print(steamgetpirce)
            # print(buffprice)
            ratio = buffprice / steamgetpirce
            df['比值'] = ratio
            profit = buffprice * 0.975 - buyprice - 1
            df['利润'] = profit
            df['年'] = start.year
            df['月'] = start.month
            df['日'] = start.day
            df['小时'] = start.hour
            DF = DF.append(df.replace(np.inf,0))
            # df.to_csv(r'C:\Users\steve\OneDrive\!steamforselling\CSGO\CSGO_{}点.csv'.format(t), mode='a', index=0)
            time.sleep(2)


        else:
            print(r['error'])
            break
    except:
        print('gg')



config = dict(
    host = 'localhost',
    user = 'root',
    password = 'root',
    cursorclass = pymysql.cursors.DictCursor
)

conn = pymysql.Connect(**config)
conn.autocommit(1)
cursor = conn.cursor()

def make_table_sql(df):
    c = df.columns.tolist()
    t = df.dtypes
    # print(c)
    # print(t)
    make_table = []
    for item in c:
        if 'int64' in str(t[item]):
            char = item + ' INT'
        elif 'object' in str(t[item]):
            char = item + ' VARCHAR(255)'
        elif 'float64'  in str(t[item]):
            char = item + ' FLOAT'
        make_table.append(char)
    return ','.join(make_table)


# cursor.execute('CREATE DATABASE IF NOT EXISTS buff')
conn.select_db('buff')
# cursor.execute('CREATE TABLE buff_csgo({})'.format(make_table_sql(DF)))
values = DF.values.tolist()
s = ','.join(['%s' for _ in range(len(DF.columns))])
cursor.executemany('INSERT INTO buff_csgo VALUE ({})'.format(s),values)

cursor.execute('select * from buff_csgo')





# filename = r'C:\Users\steve\OneDrive\!steamforselling\CSGO\CSGO_{}oclock.csv'.format(t)
filename = r'/Users/ternencekk/OneDrive/!steamforselling/CSGO/CSGO_{}oclock.csv'.format(t)


# resultname = r'C:\Users\steve\OneDrive\!steamforselling\CSGO\CSGO_{}oclock_Analysis.csv'.format(t)
resultname =r'/Users/ternencekk/OneDrive/!steamforselling/CSGO/CSGO_{}oclock_Analysis.csv'.format(t)


DF.drop_duplicates(['中文名'],'last').to_csv(filename, mode='a', index=0)
# drop_title(r'C:\Users\steve\OneDrive\!steamforselling\CSGO\CSGO_{}点.csv'.format(t), mode='a', index=0)

bbby = Buy(filename,resultname)
end = datetime.datetime.now()
print('Running time: %s' % (end - start))