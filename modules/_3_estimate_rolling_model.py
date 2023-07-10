"""

    """

from pathlib import Path

import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.stattools import durbin_watson

import ns
from main import ColName
from main import FPN
from main import Params

gdu = ns.GDU()
c = ns.Col()
fpn = FPN()
pa = Params()
cn = ColName()

ols_res = {
        cn.alpha     : None ,
        cn.alpha_std : None ,
        cn.alpha_t   : None ,
        cn.alpha_p   : None ,

        cn.beta      : None ,
        cn.beta_std  : None ,
        cn.beta_t    : None ,
        cn.beta_p    : None ,

        cn.f_stat    : None ,
        cn.f_p       : None ,
        cn.r2        : None ,
        cn.r2_adj    : None ,
        cn.dw        : None ,
        }

def keep_relevant_cols(df) :
    cols = {
            c.ftic            : None ,
            c.jd              : None ,
            c.tedpix_exss_ret : None ,
            c.exss_ret        : None ,
            cn.w_strt_idx     : None ,
            cn.w_end_idx      : None ,
            cn.nuniq          : None ,
            }

    return df[list(cols.keys())]

def mark_to_estimate_or_not(df) :
    msk = df[cn.nuniq].ge(pa.min_uniq_vals_in_window)
    msk = msk.fillna(False)

    df[cn.to_est] = msk
    print(len(msk[msk]) , 'to estimate')
    return df

def filter_relevant_subdf_for_capm(row , df) :
    strt_idx = row[cn.w_strt_idx]
    end_idx = row[cn.w_end_idx]

    df = df.loc[strt_idx : end_idx]

    tick = row[c.ftic]

    msk = df[c.ftic].eq(tick)

    msk &= df[c.tedpix_exss_ret].notna()
    msk &= df[c.exss_ret].notna()

    df = df[msk]

    return df

def fit_simple_ols(df) :
    x = df[c.tedpix_exss_ret]
    y = df[c.exss_ret]

    x = sm.add_constant(x)

    res = sm.OLS(y , x).fit()

    return res

def ex_ols_result(res) :
    ou = ols_res.copy()

    if len(res.params) != 2 :
        return pd.Series(ou)

    ou[cn.alpha] = res.params[0]
    ou[cn.alpha_std] = res.bse[0]
    ou[cn.alpha_t] = res.tvalues[0]
    ou[cn.alpha_p] = res.pvalues[0]

    ou[cn.beta] = res.params[1]
    ou[cn.beta_std] = res.bse[1]
    ou[cn.beta_t] = res.tvalues[1]
    ou[cn.beta_p] = res.pvalues[1]

    ou[cn.f_stat] = res.fvalue
    ou[cn.f_p] = res.f_pvalue
    ou[cn.r2] = res.rsquared
    ou[cn.r2_adj] = res.rsquared_adj
    ou[cn.dw] = durbin_watson(res.resid)

    return pd.Series(ou)

def fit_model_on_a_row(row , df) :
    if not row[cn.to_est] :
        return pd.Series(ols_res)

    df_sub = filter_relevant_subdf_for_capm(row , df)
    res = fit_simple_ols(df_sub)

    return ex_ols_result(res)

def main() :
    pass

    ##
    # read temp data
    df = pd.read_parquet(fpn.t2)

    ##
    # keep relevant cols
    df = keep_relevant_cols(df)

    ##
    # mark to estimate or not
    df = mark_to_estimate_or_not(df)

    ##
    # fit model
    cols = ols_res.keys()

    ##
    for idx , ro in df.iterrows() :
        df.loc[idx , cols] = fit_model_on_a_row(ro , df)

        if idx % 1e4 == 0 :
            print(idx)

    ##
    df.to_parquet(fpn.t3 , index = False)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
