"""

    """

import pandas as pd
from githubdata import get_data_wo_double_clone
from mirutil.df import save_df_as_prq

from main import *

cl = tse_ns.DAllCodalLetters()
cd = tse_ns.DIndInsCols()

def get_measures_data() :
    df = pd.read_parquet(fpn.t8)
    return df

def get_mkt_adj_rtns() :
    return get_data_wo_double_clone(gdu.thesis_mkt_adj_ret)

def keep_relevant_cols(df) :
    c2k = {
            c.ftic  : None ,
            c.d     : None ,
            c.jd    : None ,
            cn.sti0 : None ,
            cn.dti0 : None ,
            cn.sci0 : None ,
            cn.dci0 : None ,
            }

    return df[c2k.keys()]

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

def gen_jyear(df) :
    df[c.jyr] = df[c.jd].str[:4]
    return df

def add_one_and_two_workday_lags_of_news_type(df) :
    """ shifting this way is not 100% correct, but it is good enough for now
    there might be open days not in the data set. but lag in stata does not work either
    because it considers dates and not workdays.
    """
    df[cn.l1n] = df.groupby(c.ftic)[cn.nws_type].shift(-1)
    df[cn.l2n] = df.groupby(c.ftic)[cn.nws_type].shift(-2)

    return df

def filter_out_non_eligible_rows_for_set_1(df) :
    msk = df[c.is_tic_open].eq(True)

    for col in [cn.nws_type , cn.l1n , cn.l2n] :
        msk &= ~ df[col].isin([nws_type.unk , nws_type.neutral])
        msk &= df[col].notna()

    return df[msk]

def main() :
    pass

    ##

    df = get_measures_data()
    df = keep_relevant_cols(df)

    ##

    dfr = get_mkt_adj_rtns()

    ##

    # add market adjusted returns data to baseline data
    df = df.merge(dfr , on = [c.ftic , c.d , c.jd] , how = 'left')

    ##

    dfn = read_news_data_keep_revelant_cols()

    ##

    df = add_news_data_to_baseline_data(df , dfn)

    ##

    df_ref = read_refrence_points_data()

    ##

    # add the reference points data to baseline data
    df = df.merge(df_ref , on = [c.ftic , c.jd] , how = 'left')

    ##

    save_df_as_prq(df , fpn.t13_0)

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

    df = gen_jyear(df)

    ##

    df = add_one_and_two_workday_lags_of_news_type(df)

    ##

    df = filter_out_non_eligible_rows_for_set_1(df)

    ##

    df.to_csv(fpn.main , index = False)

##
if __name__ == "__main__" :
    main()
