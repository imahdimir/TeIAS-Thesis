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

def cal_net_buy_share(df: pd.DataFrame) -> pd.DataFrame :
    """
    Cal net buy share for ins and ind
    """
    cols = {
            (cd.bsva , cd.ssva) : cn.nbss ,
            (cd.bdva , cd.sdva) : cn.nbsd ,
            }

    for ky , vl in cols.items() :
        a1 = df[ky[0]].astype('Int64')
        b1 = df[ky[1]].astype('Int64')
        df[vl] = (a1 - b1) / (a1 + b1)

        df[vl] = df[vl].astype(float)
        df[vl] = df[vl].fillna(0)

    return df

def cal_net_sell_share(df: pd.DataFrame) -> pd.DataFrame :
    df[cn.nss] = - df[cn.nbss]
    df[cn.nsd] = - df[cn.nbsd]
    return df

def cal_excess_buy_and_sell(df) :
    cols = {
            0 : (cn.nbss , 1 , cd.bsva , cn.avex_buy_s) ,
            1 : (cn.nbss , -1 , cd.ssva , cn.avex_sell_s) ,
            2 : (cn.nbsd , 1 , cd.bdva , cn.avex_buy_d) ,
            3 : (cn.nbsd , -1 , cd.sdva , cn.avex_sell_d) ,
            }

    for vl in cols.values() :
        df['h'] = df[vl[0]].copy()

        df['h'] = df['h'] * vl[1]

        msk = df[vl[2]].astype('Int64').eq(0)
        df.loc[msk , 'h'] = np.nan

        df[vl[3]] = df.groupby(c.d)['h'].transform('mean')

    df = df.drop(columns = 'h')

def cal_simple_daily_average(df) :
    cols = {
            cn.nbss : cn.avs ,
            cn.nbsd : cn.avd ,
            }

    for ky , vl in cols.items() :
        df[vl] = df.groupby(c.d)[ky].transform('mean')

    return df

def cal_daily_average_for_traded(df) :
    cols = {
            cn.nbss : cn.avts ,
            cn.nbsd : cn.avtd ,
            }

    for ky , vl in cols.items() :
        df['h'] = df[ky].copy()
        df['h'] = df['h'].replace(0 , np.nan)
        df[vl] = df.groupby(c.d)['h'].transform('mean')

    df = df.drop(columns = 'h')

    return df

def cal_daily_average_excluding_zeros(df) :
    cols = {
            0 : (cn.nbss , 1 , cd.bsva , cn.avex_buy_s) ,
            1 : (cn.nbss , -1 , cd.ssva , cn.avex_sell_s) ,
            2 : (cn.nbsd , 1 , cd.bdva , cn.avex_buy_d) ,
            3 : (cn.nbsd , -1 , cd.sdva , cn.avex_sell_d) ,
            }

    for vl in cols.values() :
        df['h'] = df[vl[0]].copy()

        df['h'] = df['h'] * vl[1]

        msk = df[vl[2]].astype('Int64').eq(0)
        df.loc[msk , 'h'] = np.nan

        df[vl[3]] = df.groupby(c.d)['h'].transform('mean')

    df = df.drop(columns = 'h')

    return df

def cal_excess_buy_and_sell(df) :
    cols = {
            cn.xb_smpl_s     : (cn.nbss , 1 , cn.avs) ,
            cn.xb_smpl_d     : (cn.nbsd , 1 , cn.avd) ,
            cn.xb_smpl_trd_s : (cn.nbss , 1 , cn.avts) ,
            cn.xb_smpl_trd_d : (cn.nbsd , 1 , cn.avtd) ,
            cn.xb_s          : (cn.nbss , 1 , cn.avex_buy_s) ,
            cn.xs_s          : (cn.nbss , -1 , cn.avex_sell_s) ,
            cn.xb_d          : (cn.nbsd , 1 , cn.avex_buy_d) ,
            cn.xs_d          : (cn.nbsd , -1 , cn.avex_sell_d) ,
            }

    for ky , vl in cols.items() :
        df[ky] = df[vl[0]] * vl[1] - df[vl[2]]

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
    df = cal_net_buy_share(df)

    ##
    e_buy = 'Expected_Buy'

    df[e_buy] = df[[cn.nbss , cn.nbsd]].mean(axis = 1)

    ##
    df[cn.xb_s] = df[cn.nbss] - df[e_buy]
    df[cn.xb_d] = df[cn.nbsd] - df[e_buy]

    ##

    ##

    ##
    df.to_parquet(fpn.t8 , index = False)

    ##

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##
def pre_version() :
    pass

    ##
    df = cal_simple_daily_average(df)

    ##
    df = cal_daily_average_for_traded(df)

    ##
    df = cal_daily_average_excluding_zeros(df)

    ##
    df = cal_excess_buy_and_sell(df)
