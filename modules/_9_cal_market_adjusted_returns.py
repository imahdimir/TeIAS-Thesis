"""

    """

from pathlib import Path

import numpy as np
import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_prq

from main import c
from main import cn
from main import fpn
from main import gdu

class Params :
    window_shift_size = {
            cn.rm1      : (1 , 1) ,
            cn.rm2m5    : (5 , 2) ,
            cn.rm6m27   : (22 , 6) ,
            cn.rm28m119 : (92 , 28) ,
            }

def keep_relevant_cols(df) :
    cols = {
            c.ftic        : None ,
            c.jd          : None ,
            c.is_tic_open : None ,
            cn.ret        : None ,
            }

    return df[list(cols.keys())]

def cal_cum_ret(df ,
                date_col ,
                groupby_col ,
                ret_col ,
                cum_ret_col ,
                window_size ,
                shift_size
                ) :
    """
    calculate cumulative return, with window size and shift size

    """
    dtc = date_col
    gpc = groupby_col
    rc = ret_col
    crc = cum_ret_col
    ws = window_size
    ss = shift_size

    df = df.sort_values(by = dtc)

    df[crc] = df[rc] + 1

    df1 = df.groupby(gpc)[crc].rolling(ws).apply(np.prod , raw = True)
    df[crc] = df1.reset_index(0 , drop = True)

    df[crc] = df[crc] - 1

    df[crc] = df.groupby(gpc)[crc].shift(ss)

    return df

def cal_all_cum_rets(df) :
    cols = Params().window_shift_size

    for cum_ret_col , (window_size , shift_size) in cols.items() :
        df = cal_cum_ret(df ,
                         c.jd ,
                         c.ftic ,
                         cn.ret ,
                         cum_ret_col ,
                         window_size ,
                         shift_size)

    return df

def get_market_index_data() :
    gdr = GitHubDataRepo(gdu.mkt_indx_s)
    df = gdr.read_data()
    return df

def cal_market_return(df_mkt) :
    df = df_mkt[[c.jd , c.tedpix_close]]
    df = df.sort_values(c.jd)
    df[c.tedpix_ret] = df[c.tedpix_close].astype(float).pct_change()
    return df

def cal_mkt_cum_rets(df) :
    ws = Params().window_shift_size

    cols = {
            cn.mrm1      : None ,
            cn.mrm2m5    : None ,
            cn.mrm6m27   : None ,
            cn.mrm28m119 : None ,
            }

    h = 'h'
    df['h'] = 1

    zo = zip(cols.keys() , ws.values())

    for cum_ret_col , (window_size , shift_size) in zo :
        print(cum_ret_col , window_size , shift_size)

        df = cal_cum_ret(df ,
                         c.jd ,
                         h ,
                         c.tedpix_ret ,
                         cum_ret_col ,
                         window_size ,
                         shift_size)

    df = df.drop(columns = h)

    return df

def cal_market_adjusted_returns(df) :
    cols = {
            cn.arm1      : (cn.rm1 , cn.mrm1) ,
            cn.arm2m5    : (cn.rm2m5 , cn.mrm2m5) ,
            cn.arm6m27   : (cn.rm6m27 , cn.mrm6m27) ,
            cn.arm28m119 : (cn.rm28m119 , cn.mrm28m119) ,
            }

    for cum_ret_col , (cum_ret_col1 , cum_ret_col2) in cols.items() :
        df[cum_ret_col] = df[cum_ret_col1] - df[cum_ret_col2]

    return df

def main() :
    pass

    ##
    df = pd.read_parquet(fpn.t0)

    ##
    df = keep_relevant_cols(df)

    ##
    df = cal_all_cum_rets(df)

    ##
    dfm = get_market_index_data()

    ##
    dfm = cal_market_return(dfm)

    ##
    dfm = cal_mkt_cum_rets(dfm)

    ##
    df = df.merge(dfm , on = c.jd , how = 'left')

    ##
    df = cal_market_adjusted_returns(df)

    ##
    save_df_as_prq(df , fpn.t9)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
