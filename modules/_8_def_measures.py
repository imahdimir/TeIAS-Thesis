"""

    """

from pathlib import Path

import numpy as np
import pandas as pd
from githubdata import GitHubDataRepo as GDR

from main import c
from main import cn
from main import fpn
from main import gdu
from main import tse_ns

# namespace  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
cd = tse_ns.DIndInsCols()

def drop_not_balanced_rows(df: pd.DataFrame) -> pd.DataFrame :
    """

    drop rows that are not balanced in the sense that the sum of buy and sell
    values are not equal market wise

    """

    a1 = df[cd.bdva].astype('Int64')
    b1 = df[cd.bsva].astype('Int64')
    c1 = df[cd.sdva].astype('Int64')
    d1 = df[cd.ssva].astype('Int64')

    df['c'] = (a1 + b1) - (c1 + d1)

    msk = df['c'].ne(0)

    df = df[~ msk]

    df = df.drop(columns = ['c'])

    return df

def gen_trade_imbalance(df: pd.DataFrame) -> pd.DataFrame :
    """
    cal trade imbalance = (buy - sell) / (buy + sell)
    """
    cols = {
            (cd.bsva , cd.ssva) : (cn.sti , cn.sti0) ,
            (cd.bdva , cd.sdva) : (cn.dti , cn.dti0) ,
            }

    for ky , vl in cols.items() :
        a1 = df[ky[0]].astype('Int64')
        b1 = df[ky[1]].astype('Int64')

        df[vl[0]] = (a1 - b1) / (a1 + b1)

        df[vl[0]] = df[vl[0]].astype(float)

        # fill nan with 0 on another col
        df[vl[1]] = df[vl[0]].fillna(0)

    return df

def gen_daily_average_conditional_on_sold_bougth(df) :
    cols = {
            0 : (cn.sti0 , 1 , cd.bsva , cn.scb) ,
            1 : (cn.sti0 , -1 , cd.ssva , cn.scs) ,
            2 : (cn.dti0 , 1 , cd.bdva , cn.dcb) ,
            3 : (cn.dti0 , -1 , cd.sdva , cn.dcs) ,
            }

    for vl in cols.values() :
        df['h'] = df[vl[0]].copy()

        df['h'] = df['h'] * vl[1]

        msk = df[vl[2]].astype('Int64').eq(0)
        df.loc[msk , 'h'] = np.nan

        df[vl[3]] = df.groupby(c.d)['h'].transform('mean')

    df = df.drop(columns = 'h')

    return df

def gen_excess_buy_sell(df) :
    cols = {
            cn.sxb : (cn.sti0 , 1 , cn.scb) ,
            cn.sxs : (cn.sti0 , -1 , cn.scs) ,
            cn.dxb : (cn.dti0 , 1 , cn.dcb) ,
            cn.dxs : (cn.dti0 , -1 , cn.dcs) ,
            }

    for ky , vl in cols.items() :
        df[ky] = df[vl[0]] * vl[1] - df[vl[2]]

    return df

def gen_count_imbalance(df: pd.DataFrame) -> pd.DataFrame :
    """
    count imbalance = (buy_count - sell_count) / (buy_count + sell_count)
    """
    cols = {
            (cd.bsc , cd.ssc) : cn.sci0 ,
            (cd.bdc , cd.sdc) : cn.dci0 ,
            }

    for ky , vl in cols.items() :
        a1 = df[ky[0]].astype('Int64')
        b1 = df[ky[1]].astype('Int64')

        df[vl] = (a1 - b1) / (a1 + b1)

        df[vl] = df[vl].astype(float)

        df[vl] = df[vl].fillna(0)

    return df

def main() :
    pass

    ##

    # get all Individual-Institutional daily trade data
    gdr = GDR(gdu.src_d_ins_ind)

    ##
    gdr.clone_overwrite()

    ##
    # read Individual-Institutional daily trade data
    df = gdr.read_data()

    ##
    df = drop_not_balanced_rows(df)

    ##
    df = gen_trade_imbalance(df)

    ##
    df = gen_daily_average_conditional_on_sold_bougth(df)

    ##
    df = gen_excess_buy_sell(df)

    ##
    df = gen_count_imbalance(df)

    ##
    df.to_parquet(fpn.t8 , index = False)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
