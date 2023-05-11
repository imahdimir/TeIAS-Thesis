"""

    """

from pathlib import Path
import datetime as dt
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
from githubdata import GitHubDataRepo
import statsmodels.api as sm
from persiantools.jdatetime import JalaliDate
import matplotlib.pyplot as plt
from scipy import stats

from mirutil.ns import update_ns_module

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()

class FPN :
    rets = Path('temp-rets.prq')

class Params :
    days_2_rm_after_ipo = 40
    start_end_window = (-60 , -1)

class ColName :
    ret = c.ar1dlf + '-modified'

fpn_adj_close_ffilled = Path('dta/adjclose-ffilled.prq')
fpn_risk_free_rate_apr_monthly = Path('dta/rf-m.xlsx')

btic = 'BaseTicker'
ipojd = 'IPOJDate'
bdtc = 'BaseDate'  # after months of IPO data for each ticker
jmc = 'JMonth'
jdc = 'JDate'
wstrt = 'WindowStart-RelativeDays'
wend = 'WindowEnd'
rfdc = 'RiskFreeRateDailyPct'
rfac = 'RiskFreeRateAPR'
odrc = '1DRet'
tic = 'Ticker'
aclc = 'AdjClose'
m1rc = 'M1DRet'
minc = "TEDPIXClose"
stdc = "StartDate"
endc = "EndDate"
sdec = "StartDateExist"
edec = "EndDateExist"
bdec = "BothDateExist"
exrc = "ExcessRet"
mexrc = "MExRet"
stid = stdc + 'Index'
enid = endc + 'Index'
nobsc = 'NObs'
nunqc = 'NUniquVals'
untonobs = 'NUniq/NObs'
valid_ols = 'ValidOLS'
expexr = '1DExpExRet'
expr = '1DExpRet'
rrmexr = 'Abs(RealR - ExpR)'
abnr = 'AbnormalReturn'

ols_res = {
        'alpha'   : None ,
        't_alpha' : None ,
        'p_alpha' : None ,

        'beta'    : None ,
        't_beta'  : None ,
        'p_beta'  : None ,
        }

def cal_daily_rate(apr) :
    x = np.log(1 + apr / 100)
    x = x / 365
    x = np.exp(x)
    x = x - 1
    x = x * 100
    return x

def nobs_and_unique_vals(ser , ref_df) :
    strt_iloc = ser[stid]
    end_iloc = ser[enid]
    tick = ser[tic]

    df = ref_df.iloc[strt_iloc : end_iloc]
    df = df[df[tic].eq(tick)]

    return len(df) , len(df[exrc].unique())

def filter_relevant_subdf(ser , ref_df) :
    strt_iloc = ser[stid]
    end_iloc = ser[enid]
    tick = ser[tic]

    df = ref_df.iloc[strt_iloc : end_iloc]
    df = df[df[tic].eq(tick)]

    return df

def read_ols_result(res) :
    if len(res.params) != 2 :
        return pd.Series(ols_res)

    ou = ols_res

    ou['alpha'] = res.params[0]
    ou['t_alpha'] = res.tvalues[0]
    ou['p_alpha'] = res.pvalues[0]

    ou['beta'] = res.params[1]
    ou['t_beta'] = res.tvalues[1]
    ou['p_beta'] = res.pvalues[1]

    return pd.Series(ou)

def ols_on_df(idf) :
    x = list(idf[exrc])
    y = list(idf[mexrc])

    x = sm.add_constant(x)

    res = sm.OLS(y , x).fit()

    return read_ols_result(res)

def wrapper_1(ser , ref_df) :
    sdf = filter_relevant_subdf(ser , ref_df)
    return ols_on_df(sdf)

def main() :
    pass

    ##

    ##

    ##
    df = df.drop(columns = bdtc)
    ##
    jdf = df[jdc].drop_duplicates().to_frame()

    jdf[wstrt] = start_end_window[0]
    jdf[stdc] = jdf[jdc].shift(- start_end_window[0])

    jdf[wend] = start_end_window[1]
    jdf[endc] = jdf[jdc].shift(- start_end_window[1])
    ##
    df = df.merge(jdf , how = 'left')
    ##
    ovindrepo = GithubData(overall_index_repo)
    ovindrepo.clone_overwrite_last_version()
    ##
    ovindfpn = ovindrepo.data_fps[0]
    ovindf = pd.read_parquet(ovindfpn)
    ovindf = ovindf.reset_index()
    ##
    ovindf = ovindf.sort_values(jdc)
    ovindf[m1rc] = ovindf[minc].pct_change()
    ovindf = ovindf.dropna()
    ##
    ovindf[m1rc].hist()
    plt.show()
    ##
    df = df.merge(ovindf , how = 'left')
    ##
    df[jmc] = df[jdc].str[:7]
    ##
    rfm = rdata(fpn_risk_free_rate_apr_monthly)
    ##
    rfm[rfdc] = rfm[rfac].apply(cal_daily_rate)
    ##
    df = df.merge(rfm , how = 'left')
    ##
    df = df.dropna()
    ##
    df[odrc] = df.groupby(tic)[aclc].pct_change()

    df = df.dropna(subset = odrc)
    ##
    df[odrc] = df[odrc].astype(float)
    z_scores = stats.zscore(df[odrc])
    abs_z_scores = np.abs(z_scores)
    filter_outliers = abs_z_scores < 3
    df = df[filter_outliers]
    ##
    df = df.dropna()
    dfv = df.head(100)
    ##
    df = df.drop(columns = [aclc , minc , jmc , rfac])
    dfv = df.head(100)
    ##
    df1 = df[[tic , jdc]]
    df1 = df1.reset_index()
    ##
    df = pd.merge(df ,
                  df1 ,
                  left_on = [tic , stdc] ,
                  right_on = [tic , jdc] ,
                  how = 'left' ,
                  indicator = True)

    df.drop(jdc + '_y' , inplace = True , axis = 1)

    df.rename(columns = {
            'index'    : stid ,
            jdc + '_x' : jdc ,
            '_merge'   : sdec
            } , inplace = True)

    df[sdec] = df[sdec].eq('both')

    df[stid] = df[stid].astype('Int64')

    msk = df[stid].notna()
    df.loc[msk , stid] = df.loc[msk , stid].astype(str)
    ##
    df = pd.merge(df ,
                  df1 ,
                  left_on = [tic , endc] ,
                  right_on = [tic , jdc] ,
                  how = 'left' ,
                  indicator = True)

    df.drop(jdc + '_y' , inplace = True , axis = 1)

    df.rename(columns = {
            'index'    : enid ,
            jdc + '_x' : jdc ,
            '_merge'   : edec
            } , inplace = True)

    df[edec] = df[edec].eq('both')

    df[enid] = df[enid].astype('Int64')

    msk = df[enid].notna()
    df.loc[msk , enid] = df.loc[msk , enid].astype(str)
    ##
    dfv = df.head(1000)
    ##
    df[bdec] = df[sdec] & df[edec]
    ##
    df[exrc] = df[odrc] - df[rfdc]
    df[mexrc] = df[m1rc] - df[rfdc]
    ##
    prntcols(df)
    ref_cols = {
            "JDate"     : None ,
            "Ticker"    : None ,
            "ExcessRet" : None ,
            "MExRet"    : None ,
            }

    ref_df = df[ref_cols.keys()]
    ##
    msk = df[bdec]
    dfv = df.head(100)
    len(msk[msk])
    ##
    df1 = df[msk].apply(lambda x : nobs_and_unique_vals(x , ref_df) ,
                        result_type = 'expand' ,
                        axis = 1)

    df1v = df1.head(1000)
    ##
    df = df.join(df1)
    ##
    df = df.rename(columns = {
            0 : nobsc ,
            1 : nunqc ,
            })
    ##
    for cn in [nobsc , nunqc] :
        df[cn] = df[cn].astype('Int64')

    dfv = df.head(1000)

    ##
    df[untonobs] = df[nunqc] / df[nobsc]
    ##
    hist = df[nobsc].hist()
    plt.show()
    ##
    msk = df[nobsc].ge(40)
    msk &= df[untonobs].ge(.5)
    len(msk[msk])
    ##
    df1 = df[msk]
    ##
    df2 = df1.apply(lambda x : wrapper_1(x , ref_df) ,
                    result_type = 'expand' ,
                    axis = 1)

    ##
    df = df.join(df2)

    dfv = df.head(10000)
    ##
    msk = df['p_beta'].le(.1)
    df.loc[msk , valid_ols] = True
    len(msk[msk])
    ##
    df[expexr] = df[mexrc] * df['beta']
    df[expr] = df[rfdc] + df[expexr]
    ##
    df[rrmexr] = df[expr] - df[odrc]
    df[rrmexr] = df[rrmexr].abs()

    df[abnr] = df[odrc] - df[expr]

    df[abnr].hist()
    plt.show()
    ##
    df1 = df[df[valid_ols].eq(True)]
    df1[rrmexr].hist()
    plt.show()
    ##
    df1 = df[msk]

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
    _tdf = pd.read_parquet('1.prq')

    ##
    tsedf = pd.read_excel('1.xlsx')
    _df = pd.read_excel('0.xlsx')

    ##
    _df['jd'] = _df[
        'JDate'].apply(lambda x : mf.persian_tools_jdate_from_iso_format_jdate_str(
            x))

    ##
    _df['Date'] = _df['jd'].apply(lambda x : x.to_gregorian())

    ##
    df0 = indf[['Date' , 'TEDPIXClose']]

    ##
    df0['Date'] = df0['Date'].apply(lambda x : dt.datetime.strptime(str(x) ,
                                                                    '%Y-%m-%d'))

    ##
    tsedf = tsedf.merge(df0 , how = 'outer')

    ##
    tsedf['fv'] = tsedf['Value']
    ##
    msk = tsedf['fv'].isna()
    tsedf.loc[msk , 'fv'] = tsedf['TEDPIXClose']
    ##
    tsedf = tsedf[['Date' , 'fv']]
    ##
    tsedf = tsedf.rename(columns = {
            'fv' : 'TEDPIXClose'
            })

    ##
    tsedf['JDate'] = tsedf['Date'].apply(lambda x : JalaliDate(x))
    ##
    tsedf['Date'] = tsedf['Date'].apply(lambda x : x.strftime('%Y-%m-%d'))
    ##
    tsedf = tsedf[['JDate' , 'Date' , 'TEDPIXClose']]
    ##
    tsedf = tsedf.drop_duplicates(subset = 'Date')
    ##
    tsedf = tsedf.sort_values('JDate')
    ##
    tsedf['r'] = tsedf['TEDPIXClose'].pct_change()
    ##
    tsedf.to_excel('temp.xlsx' , index = False)

    ##
    tsedf = pd.read_excel('temp.xlsx')
    ##
    df2 = df2.reset_index()

    ##
    msk = tsedf['JDate'].isin(df2['JDate'])
    tsedf1 = tsedf[msk]
    ##
    tsedf1 = tsedf1[['JDate' , 'Date' , 'TEDPIXClose']]

    ##
    tsedf1 = tsedf1.sort_values('JDate')

    ##
    tsedf1 = tsedf1.set_index(['JDate' , 'Date'])

    ##
    tsedf1.to_parquet('ted.prq')

    ##
    _tdf['eq'] = _tdf['Value'].eq(_tdf['TEDPIXClose'])

    ##
    _tdf['dif'] = _tdf['Value'] - _tdf['TEDPIXClose']

    ##
    _tdf1 = _tdf1.sort_values('Date')
    ##
    _tdf1['r'] = _tdf1['Value'].pct_change()

    ##
    df1 = pd.read_excel('1.xlsx')
    ##
    df1 = df1[['Date']]
    ##
    df1['JDate'] = df1['Date'].apply(lambda x : JalaliDate(x))
    ##
    df1 = df1[['JDate']]
    ##
    df1.to_excel('wd.xlsx' , index = False)

    ##
    df2 = pd.read_excel('2.xlsx')
    ##
    df1['JDate'].min()
    ##
    df1['JDate1'] = df1['JDate'].apply(lambda x : x.isoformat())

    ##
    df2['>min'] = df2['JDate'].ge(df1['JDate1'].min())

    ##
    df2['<max'] = df2['JDate'].le(df1['JDate1'].max())

    ##
    df2['isin'] = df2['JDate'].isin(df1['JDate1'])

    ##
    df2['con'] = df2['>min'] & df2['<max'] & ~ df2['isin']

    ##
    df2 = df2[df2['con'].ne(True)]

    ##
    df1['JDate'] = df1['JDate1']
    ##
    df2 = df2[['JDate' , 'Weekday']]

    ##
    df2 = pd.concat([df2 , df1['JDate']])
    ##
    df2 = df2.drop_duplicates()

    ##
    df2 = df2[['JDate']]

    ##
    df2 = df2.sort_values('JDate')
    ##
    df2['jd'] = df2[
        'JDate'].apply(lambda x : mf.persian_tools_jdate_from_iso_format_jdate_str(
            x))

    ##
    df2 = df2.dropna()

    ##
    df2['Date'] = df2['jd'].apply(lambda x : x.to_gregorian())

    ##
    df2['Weekday'] = df2['jd'].apply(lambda x : x.weekday())

    ##
    df1 = pd.read_excel('wd.xlsx')
    ##
    df1['JDate'] = df1['JDate'].astype(str)
    ##
    df1['JDate'].dtype
    ##
    df2 = pd.concat([df2 , df1])
    ##
    df2 = df2.drop_duplicates()

    ##
    df2['jd'] = df2[
        'JDate'].apply(lambda x : mf.persian_tools_jdate_from_iso_format_jdate_str(
            x))
    df2['Date'] = df2['jd'].apply(lambda x : x.to_gregorian())
    df2['Weekday'] = df2['jd'].apply(lambda x : x.weekday())
    ##
    df2 = df2[['JDate' , 'Date' , 'Weekday']]

    ##
    df2 = df2.set_index(['JDate' , 'Date' , 'Weekday'])

    ##
    df2 = df2.reset_index()
    ##
    df2.to_parquet('wd.prq' , index = False)
    ##
    df2 = df2.set_index(['JDate' , 'Date' , 'Weekday'])

    ##
    df2.to_parquet('wd.prq')

    ##
    df3 = pd.read_parquet('wd.prq')
    ##
    df3 = df3.reset_index()

    ##
    tsedf['con'] = tsedf['JDate'].lt(df3['JDate'].min())

    ##
    df4 = tsedf[tsedf['con']]
    ##
    df4 = df4[['JDate']]

    ##
    df3 = pd.concat([df3 , df4])

    ##
    df3 = df3.drop_duplicates(subset = 'JDate')

    ##
    df2 = df3

    ##
    ##
    df2['jd'] = df2[
        'JDate'].apply(lambda x : mf.persian_tools_jdate_from_iso_format_jdate_str(
            x))
    df2['Date'] = df2['jd'].apply(lambda x : x.to_gregorian())
    df2['Weekday'] = df2['jd'].apply(lambda x : x.weekday())
    ##
    df2 = df2[['JDate' , 'Date' , 'Weekday']]

    ##
    df2 = df2.sort_values('JDate')

    ##
    df2 = df2.drop_duplicates()
    ##
    df2 = df2.dropna()

    ##
    df2 = df2.set_index(['JDate' , 'Date' , 'Weekday'])

    ##
    df2.to_parquet('wd.prq')

    ##
    df1 = pd.read_parquet('ted.prq')
