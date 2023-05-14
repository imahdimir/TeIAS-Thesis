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
    t0 = Path('temp-0.prq')
    t1 = Path('temp-1.prq')
    t2 = Path('temp-2.prq')

class Params :
    days_2_rm_after_ipo = 40
    start_end_window = (-60 , -1)
    min_uniq_vals_in_window = 30

class ColName :
    ret = c.ar1dlf + '-modified'
    w_strt_jd = "Window-Start-JDate"
    w_end_jd = "Window-End-JDate"
    w_strt_idx = "Window-Start-Index"
    w_end_idx = "Window-End-Index"
    obs = "NObs"
    nuniq = "NUniqVals"
    to_est = "to-estimate-model"

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

def wrapper_1(ser , ref_df) :
    sdf = filter_relevant_subdf(ser , ref_df)
    return ols_on_df(sdf)

def main() :
    pass

    ##

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

    ##
