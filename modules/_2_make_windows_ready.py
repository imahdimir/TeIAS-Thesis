"""

    """

from pathlib import Path

import pandas as pd

from main import c
from main import cn
from main import fpn
from main import pa

def find_window_start_and_end_jdate_each_day(df) :
    df = df[[c.jd]]
    df = df.drop_duplicates()

    df = df.sort_values(c.jd)
    df = df.reset_index(drop = True)

    df[cn.w_strt_jd] = df[c.jd].shift(- pa.start_end_window[0])
    df[cn.w_end_jd] = df[c.jd].shift(- pa.start_end_window[1])

    return df

def make_index_ticker_jdate_df(df) :
    df = df[[c.ftic , c.jd]]
    return df.reset_index()

def add_window_start_end_index(df , df_idx , col_name , new_col_name) :
    df = df.merge(df_idx ,
                  left_on = [c.ftic , col_name] ,
                  right_on = [c.ftic , c.jd] ,
                  how = 'left')

    df = df.drop(c.jd + '_y' , axis = 1)

    rnm_cols = {
            'index'     : new_col_name ,
            c.jd + '_x' : c.jd ,
            }

    df = df.rename(columns = rnm_cols)

    return df

def count_notna_obs_and_unique_vals(row , df) :
    strt_idx = row[cn.w_strt_idx]
    end_idx = row[cn.w_end_idx]

    if pd.isna(strt_idx) or pd.isna(end_idx) :
        return None , None

    df = df.loc[strt_idx : end_idx]

    tick = row[c.ftic]
    msk = df[c.ftic].eq(tick)
    df = df[msk]

    msk = df[c.exss_ret].notna()

    notna_obs = len(df[msk])
    unique_vals = df.loc[msk , c.exss_ret].nunique()

    return notna_obs , unique_vals

def main() :
    pass

    ##
    # read temp data
    df = pd.read_parquet(fpn.t1)

    ##
    df_jd = find_window_start_and_end_jdate_each_day(df)

    ##
    # add window start and end jdate to data
    df = df.merge(df_jd , how = 'left')

    ##
    df_idx = make_index_ticker_jdate_df(df)

    ##
    # add window start and end index to data for making sub-df later on
    df = add_window_start_end_index(df , df_idx , cn.w_strt_jd , cn.w_strt_idx)

    df = add_window_start_end_index(df , df_idx , cn.w_end_jd , cn.w_end_idx)

    ##
    df = df.drop(columns = [cn.w_strt_jd , cn.w_end_jd])

    ##
    # make indices int
    for col in [cn.w_strt_idx , cn.w_end_idx] :
        df[col] = df[col].astype('Int64')

    ##
    # count obs and unique vals in each window for each ticker

    cols = [cn.obs , cn.nuniq]

    for idx , ro in df.iterrows() :
        df.loc[idx , cols] = count_notna_obs_and_unique_vals(ro , df)

        if idx % 1e4 == 0 :
            print(idx)

    ##
    # make indices int
    df[cols] = df[cols].astype('Int64')

    ##
    # save
    df.to_parquet(fpn.t2 , index = False)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
