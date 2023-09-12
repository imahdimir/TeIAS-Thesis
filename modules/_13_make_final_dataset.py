"""

    """

import struct
from pathlib import Path

import pandas as pd
import requests
from mirutil.df import save_df_as_prq
from namespace_mahdimir import tse as tse_ns

from main import c
from main import cn
from main import fpn
from main import nws_type

cl = tse_ns.DAllCodalLetters()
cd = tse_ns.DIndInsCols()

def get_measures_data() :
    df = pd.read_parquet(fpn.t8)
    return df

def get_market_adjusted_returns_data_and_keep_relevant_cols() :
    df1 = pd.read_parquet(fpn.t9)

    cols = {
            c.ftic        : None ,
            c.jd          : None ,
            c.is_tic_open : None ,
            cn.arm1       : None ,
            cn.arm2m5     : None ,
            cn.arm6m27    : None ,
            cn.arm28m119  : None ,
            }

    return df1[cols.keys()]

def read_news_data_keep_revelant_cols() :
    df = pd.read_parquet(fpn.t7)

    # remove excess rows
    df = df.sort_values(by = cl.PublishDateTime)

    df = df.drop_duplicates(subset = [c.ftic , cn.nws_eff_jd])

    cols = {
            c.ftic        : None ,
            cn.nws_eff_jd : None ,
            cn.nws_type   : None ,
            }

    df = df[cols.keys()]

    ren = {
            cn.nws_eff_jd : c.jd ,
            }

    df = df.rename(columns = ren)

    df = df.dropna()

    return df

def add_news_data_to_baseline_data(df , df_news) :
    df = df.merge(df_news , how = 'left' , on = [c.ftic , c.jd])

    # mark no news days

    min_d = df_news[c.jd].min()
    print(min_d)

    maxd = df_news[c.jd].max()
    print(maxd)

    msk = df[c.jd].ge(min_d)
    msk &= df[c.jd].le(maxd)
    msk &= df[cn.nws_type].isna()

    len(msk[msk])

    df.loc[msk , cn.nws_type] = nws_type.no_news

    return df

def read_weekday_data() :
    return pd.read_parquet(fpn.t10)

def read_refrence_points_data() :
    df = pd.read_parquet(fpn.t11)

    cols = {
            c.ftic : None ,
            c.jd   : None ,
            cn.mh  : None ,
            cn.ml  : None
            }

    df = df[cols.keys()]

    return df

def read_firm_size_terciles_data() :
    df = pd.read_parquet(fpn.t12)

    cols = {
            c.ftic        : None ,
            c.d           : None ,
            c.jd          : None ,
            cn.fs_tercile : None ,
            }

    df = df[cols.keys()]

    return df

def filter_out_non_eligible_rows_for_set_1(df) :
    msk = df[c.is_tic_open].eq(True)
    msk &= ~ df[cn.nws_type].isin([nws_type.unk , nws_type.neutral])
    msk &= df[cn.nws_type].notna()

    return df[msk]

def change_adjusted_returns_col_names(df) :
    ren = {
            cn.arm1      : 'r1' ,
            cn.arm2m5    : 'r2' ,
            cn.arm6m27   : 'r6' ,
            cn.arm28m119 : 'r28' ,
            }

    df = df.rename(columns = ren)

    df = df.convert_dtypes()

    return df

def add_one_and_two_workday_lags_of_news_type(df) :
    """ shifting this way is not 100% correct, but it is good enough for now
    there might be open days not in the data set. but lag in stata does not work
    because it considers dates and not workdays.
    """
    df[cn.l1n] = df.groupby(c.ftic)[cn.nws_type].shift(-1)
    df[cn.l2n] = df.groupby(c.ftic)[cn.nws_type].shift(-2)

    return df

def main() :
    pass

    ##
    df = get_measures_data()

    ##
    df_r = get_market_adjusted_returns_data_and_keep_relevant_cols()

    ##

    # add the market adjusted returns data to baseline data

    df = df.merge(df_r , on = [c.ftic , c.jd] , how = 'left')

    ##
    df_nws = read_news_data_keep_revelant_cols()

    ##
    df = add_news_data_to_baseline_data(df , df_nws)

    ##
    df_wd = read_weekday_data()

    ##

    # add the weekday data to baseline data
    df = df.merge(df_wd , on = [c.jd] , how = 'left')

    ##
    df_ref = read_refrence_points_data()

    ##

    # add the reference points data to baseline data
    df = df.merge(df_ref , on = [c.ftic , c.jd] , how = 'left')

    ##
    df_fs = read_firm_size_terciles_data()

    ##
    df = df.merge(df_fs , how = 'left')

    ##
    save_df_as_prq(df , fpn.t13)

    ##
    df = pd.read_parquet(fpn.t13)

    ##
    df = df.sort_values(by = [c.d] , ascending = False)

    ##
    df = add_one_and_two_workday_lags_of_news_type(df)

    ##
    df = filter_out_non_eligible_rows_for_set_1(df)

    ##
    df = change_adjusted_returns_col_names(df)

    ##
    df.to_csv(fpn.ts1 , index = False)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


def test() :
    pass

    ##
    def get_sum_stat_of_final_data() :
        pass

        ##
        df = pd.read_parquet(fpn.t13)

        ##
        df[c.d].min()
        ##
        df[c.d].max()

        ##
        df[c.d].nunique()

        ##
        cd = tse_ns.DIndInsCols()

        for cl in [cd.bdc , cd.bsc , cd.sdc , cd.ssc] :
            msk = df[cl].ne('0')
            df1 = df[msk]
            print(len(df1))

        ##
        vals = []
        for cl in [cd.bdv , cd.bsv , cd.sdv , cd.ssv] :
            sr = df[cl].astype(int)
            print(sr.sum())
            vals.append(sr.sum())

        ##
        vals

        ##
        v1 = [x / 10 ** 9 for x in vals]

        ##
        v1

        ##
        v2 = [x.round(1) for x in v1]
        v2

        ##
        vals = []
        for cl in [cd.bdva , cd.bsva , cd.sdva , cd.ssva] :
            sr = df[cl].astype(int)
            print(sr.sum())
            vals.append(sr.sum())

        ##
        vals

        ##
        v1 = [x / 10 ** 13 for x in vals]

        ##
        v1

        ##
        v2 = [x.round(1) for x in v1]
        v2

        ##
        vals = []
        for cl in [cd.bdc , cd.bsc , cd.sdc , cd.ssc] :
            sr = df[cl].astype(int)
            print(sr.mean())
            vals.append(sr.mean())

        ##

        vals

        ##
        v1 = [x for x in vals]

        ##
        v1

        ##
        v2 = [x.round(1) for x in v1]
        v2

##
