import pandas as pd
import numpy as np

def Buy(address1,address2):
    #显示所有
    pd.set_option('display.width',None)
    # file = pd.read_csv(r'C:\Users\steve\OneDrive\!steamforselling\CSGO\CSGO_20200318_13点.csv')
    file = pd.read_csv(address1)
    newfile = file[['中文名','英文名','buff_最低售价','buff_在售数','buff_最高求购价','buff_求购数','steam价格_rmb','steamurl','buff_url','比值','利润']]
    #lst里面是利润率=利润/最高求购价
    lst =[]
    for a1,b1 in zip(newfile['利润'],newfile['buff_最高求购价']):
        if b1 != 0:
            c1 = a1/b1
            lst.append(c1)
        else:
            c1 = np.nan
            lst.append(c1)
    profit = pd.DataFrame(lst,columns = ['利润率'])
    # print(newfile)
    print(profit)
    result = pd.merge(newfile,profit,left_index=True,right_index=True)
    print(result)
    final = result[(result['steam价格_rmb']<300)&(result['steam价格_rmb']>100)&(result['比值']>0.45)&(result['比值']<0.65)]
    # final.to_csv(r'C:\Users\steve\Desktop\csgooo.csv')
    final.sort_values(by=['比值','buff_在售数'],ascending=False).to_csv(address2)
    print('Anaysis finished')
