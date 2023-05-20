"""

    """

from pathlib import Path
import datetime
import warnings

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.jdate import make_datetime_from_iso_jdate_time

import ns
from main import ColName
from main import FPN
from main import Params

warnings.filterwarnings('ignore')

gdu = ns.GDU()

c = ns.Col()
cd = ns.DAllCodalLetters()

fpn = FPN()
pa = Params()
cn = ColName()

def get_tikcer_open_days_data() :
    df = pd.read_parquet(fpn.t2)

    cols_2_keep = {
            c.ftic        : None ,
            c.d           : None ,
            c.is_tic_open : None ,
            }
    df = df[list(cols_2_keep.keys())]

    return df

def keep_only_open_days(df) :
    """keep only open days"""
    msk = df[c.is_tic_open]
    df = df[msk]

    df = df.drop(columns = [c.is_tic_open])

    return df

def sort_by_date(df) :
    """sort by date"""
    df = df.sort_values(by = [c.d])

    return df

def add_datetime_col_at_1230pm(df) :
    """add a datetime col at 12:30 PM"""

    df[c.dt] = pd.to_datetime(df[c.d])

    # add a time shift to the datatime col
    td = datetime.timedelta(hours = 12 , minutes = 30)
    df[c.dt] = df[c.dt] + pd.Timedelta(td)

    df = df.drop(columns = [c.d])

    return df

def convert_publish_jdatetime_to_datetime(df) :
    df[c.dt] = df[cd.PublishDateTime].map(make_datetime_from_iso_jdate_time)
    return df

def find_news_effective_date(df_ftic , sr) :
    """find news effective date"""
    df = df_ftic

    df['h'] = sr[c.dt]

    df['h1'] = df['h'].le(df[c.dt])

    try :
        idx = df['h1'].idxmax()
        return df.at[idx , c.dt]
    except ValueError :
        """no news effective date found, empty dataframe"""
        return None

def filter_open_days_by_ftic(df , ftic) :
    """filter open days by ftic"""
    msk = df[c.ftic].eq(ftic)
    df = df[msk]
    return df

def find_effective_date_for_each_news(df_open_days , df_nws) :
    """find effective date for each news"""
    df = df_open_days

    fu = find_news_effective_date

    gps = df_nws.groupby(c.ftic)

    for name , group in gps :
        print(name)

        df_ftic = filter_open_days_by_ftic(df , name)

        for indx , ro in group.iterrows() :
            df_nws.at[indx , cn.nws_eff_d] = fu(df_ftic , ro)

    return df_nws

def main() :
    pass

    ##
    df = get_tikcer_open_days_data()

    ##
    df = keep_only_open_days(df)

    ##
    df = sort_by_date(df)

    ##
    df = add_datetime_col_at_1230pm(df)

    ##
    df_nws = pd.read_parquet(fpn.t5)

    ##
    df_nws = convert_publish_jdatetime_to_datetime(df_nws)

    ##
    df_nws = find_effective_date_for_each_news(df , df_nws)

    ##
    df_nws.to_parquet(fpn.t6 , index = False)

    ##

    ##

    ##

    ##

    ##

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
