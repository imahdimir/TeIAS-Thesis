"""

    """

from pathlib import Path

import pandas as pd

from main import c
from main import cn
from main import fpn
from main import pa

def mark_credible_models(df) :
    """fiter out credible models with significant beta"""

    msk = df[cn.beta_p].le(pa.model_significance_model)

    msk = msk.fillna(False)

    print(len(msk[msk]) , " number of credible models.")

    df[cn.cred_model] = msk

    return df

def cal_expected_excess_return(df) :
    c1 = cn.expctd_exss_ret
    c2 = cn.alpha
    c3 = cn.beta
    c4 = c.tedpix_exss_ret

    msk = df[cn.cred_model]

    df[c1] = df[c2][msk] + df[c3][msk] * df[c4][msk]

    return df

def mark_significant_abnormal_retrurns(df) :
    msk = df[cn.abnrml_ret].abs().ge(pa.min_abs_abnormnal_ret)
    df[cn.is_abnrml_ret_signfcnt] = msk

    msk = df[cn.abnrml_ret].isna()
    df.loc[msk , cn.is_abnrml_ret_signfcnt] = None

    return df

def main() :
    pass

    ##
    # read model data
    df = pd.read_parquet(fpn.t3)

    ##
    # mark credible models
    df = mark_credible_models(df)

    ##
    # cal expected excess return for credible models
    df = cal_expected_excess_return(df)

    ##
    # cal abnormal return
    df[cn.abnrml_ret] = df[c.exss_ret] - df[cn.expctd_exss_ret]

    ##
    # mark significant abnormal returns as True
    df = mark_significant_abnormal_retrurns(df)

    ##
    # save
    df.to_parquet(fpn.t4 , index = False)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
