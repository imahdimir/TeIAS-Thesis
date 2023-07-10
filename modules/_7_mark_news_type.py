"""

    """

from pathlib import Path

import pandas as pd
from persiantools.jdatetime import JalaliDate

from main import ColName , ns
from main import FPN
from main import NewsType
from main import Params

gdu = ns.GDU()

c = ns.Col()
cd = ns.DAllCodalLetters()

fpn = FPN()
pa = Params()
cn = ColName()
nws_type = NewsType()

def find_news_effective_jdate(df) :
    df[cn.nws_eff_d] = df[cn.nws_eff_d].dt.date

    # convert to jdate
    msk = df[cn.nws_eff_d].notna()

    df[cn.nws_eff_jd] = df.loc[msk , cn.nws_eff_d].map(JalaliDate.to_jalali)

    # drop excess columns
    df = df.drop(columns = [c.dt , cn.nws_eff_d])

    return df

def read_abnormal_returns_data() :
    df = pd.read_parquet(fpn.t4)

    cols_2_keep = {
            c.ftic                    : None ,
            c.jd                      : None ,
            cn.abnrml_ret             : None ,
            cn.is_abnrml_ret_signfcnt : None ,
            }
    df = df[list(cols_2_keep.keys())]

    return df

def add_abnormal_returns_data_to_news(df , df_abn) :
    # merge abnormal returns data to news data

    df[cn.nws_eff_jd] = df[cn.nws_eff_jd].astype('string')

    df = df.merge(df_abn ,
                  how = 'left' ,
                  left_on = [c.ftic , cn.nws_eff_jd] ,
                  right_on = [c.ftic , c.jd])

    df = df.drop(columns = [c.jd])

    return df

def mark_news_type(df) :
    # mark significant positive abnormal returns as good news, no need to check for having notna news effective date because I already merge on it, so if it is nan the abnormal return would be nan too.
    msk = df[cn.abnrml_ret].ge(0)
    msk &= df[cn.is_abnrml_ret_signfcnt]

    df.loc[msk , cn.nws_type] = nws_type.good

    # mark significant negative abnormal returns as bad news, again no need to check for having notna news effective date.
    msk = df[cn.abnrml_ret].lt(0)
    msk &= df[cn.is_abnrml_ret_signfcnt]

    df.loc[msk , cn.nws_type] = nws_type.bad

    # mark insignificant abnormal returns as neutral news, again no need to check for having notna news effective date.
    msk = df[cn.is_abnrml_ret_signfcnt].eq(False)

    df.loc[msk , cn.nws_type] = nws_type.neutral

    # mark having news effective date but no abnormal return as unknown news
    msk = df[cn.nws_eff_jd].notna()
    msk &= df[cn.abnrml_ret].isna()

    df.loc[msk , cn.nws_type] = nws_type.unknown

    return df

def main() :
    pass

    ##
    # read news data with effective date
    df = pd.read_parquet(fpn.t6)

    ##
    df = find_news_effective_jdate(df)

    ##
    df_abn = read_abnormal_returns_data()

    ##
    df = add_abnormal_returns_data_to_news(df , df_abn)

    ##
    df = mark_news_type(df)

    ##
    df.to_parquet(fpn.t7 , index = False)

    ##
    print('news type distribution:')
    print(df[cn.nws_type].value_counts())

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
