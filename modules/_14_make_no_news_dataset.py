"""

    """

from pathlib import Path

import pandas as pd
from mirutil.df import save_df_as_prq
from namespace_mahdimir import tse as tse_ns

from main import c
from main import cn
from main import fpn
from main import nws_type
from modules._13_make_main_dataset import add_news_data_to_baseline_data
from modules._13_make_main_dataset import change_adjusted_returns_col_names
from modules._13_make_main_dataset import \
    get_market_adjusted_returns_keep_relevant_cols
from modules._13_make_main_dataset import get_measures_data
from modules._13_make_main_dataset import read_news_data_keep_revelant_cols
from modules._13_make_main_dataset import read_refrence_points_data
from modules._13_make_main_dataset import read_weekday_data

cl = tse_ns.DAllCodalLetters()
cd = tse_ns.DIndInsCols()

class Param :
    days_2_rm = 5

pa = Param()

def mark_news_neighborhood_by_firm(df) :
    has_news = 'Has_News'

    has_news_types = [nws_type.good , nws_type.neutral , nws_type.bad ,
                      nws_type.unk]

    df[has_news] = df[cn.nws_type].isin(has_news_types)

    msk = df[has_news].eq(False)

    df.loc[msk , has_news] = None

    nhbr1 = 'nhbr1'
    nhbr2 = 'nhbr2'

    df = df.sort_values(by = [c.d] , ascending = False)

    df[nhbr1] = df.groupby(c.ftic)[has_news].ffill(pa.days_2_rm)
    df[nhbr2] = df.groupby(c.ftic)[has_news].bfill(pa.days_2_rm)

    nhbr = 'News_Neighborhood'

    msk = df[nhbr1].eq(True)
    msk |= df[nhbr2].eq(True)

    df.loc[msk , nhbr] = True

    df = df.drop(columns = [has_news , nhbr1 , nhbr2])

    return df

def rm_non_eligible_rows(df) :
    msk = df[c.is_tic_open].eq(True)
    msk &= df[cn.nws_type].eq(nws_type.no_news)
    msk &= df[cn.nhbr].isna()

    df = df[msk]

    return df

def rm_excess_cols(df) :
    cols = {
            cn.nws_type : None ,
            cn.nhbr     : None ,
            }

    df = df.drop(columns = cols.keys())

    return df

def main() :
    pass

    ##
    df = get_measures_data()

    ##
    df_r = get_market_adjusted_returns_keep_relevant_cols()

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
    save_df_as_prq(df , fpn.t14)

    ##
    df = pd.read_parquet(fpn.t14)

    ##
    df = mark_news_neighborhood_by_firm(df)

    ##
    df = rm_non_eligible_rows(df)

    ##
    df = rm_excess_cols(df)

    ##
    df = change_adjusted_returns_col_names(df)

    ##
    df.to_csv(fpn.no_nws , index = False)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
