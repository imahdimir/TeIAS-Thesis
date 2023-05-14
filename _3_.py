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
        'alpha'     : None ,
        'alpha_std' : None ,
        'alpha_t'   : None ,
        'alpha_p'   : None ,

        'beta'      : None ,
        'beta_std'  : None ,
        'beta_t'    : None ,
        'beta_p'    : None ,

        'F-stat'    : None ,
        'F-p'       : None ,
        'R2'        : None ,
        'R2_adj'    : None ,
        'DW'        : None ,
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

    ou['alpha'] = res.params[0]
    ou['alpha_std'] = res.bse[0]
    ou['alpha_t'] = res.tvalues[0]
    ou['alpha_p'] = res.pvalues[0]

    ou['beta'] = res.params[1]
    ou['beta_std'] = res.bse[1]
    ou['beta_t'] = res.tvalues[1]
    ou['beta_p'] = res.pvalues[1]

    ou['F-stat'] = res.fvalue
    ou['F-p'] = res.f_pvalue
    ou['R2'] = res.rsquared
    ou['R2_adj'] = res.rsquared_adj
    ou['DW'] = durbin_watson(res.resid)

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

    for idx , ro in df.iterrows() :
        df.loc[idx , cols] = fit_model_on_a_row(ro , df)

        if idx % 1e4 == 0 :
            print(idx)
            break

    ##
    idx = 667733
    row = df.loc[idx]
    row

    ##
    df.loc[idx , cols] = fit_model_on_a_row(df.loc[idx] , df)
    row1 = df.loc[idx]

    ##
    df1 = filter_relevant_subdf_for_capm(row , df)

    ##
    res = fit_simple_ols(df1)
    res.summary()

    ##
    res1 = ex_ols_result(res)
    res1

    ##
    res.params

    ##
    res.summary()

    ##
    res.durbin_watson

    ##
    durbin_watson(res.resid)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


# noinspection PyUnreachableCode
if False :
    pass

    ##
