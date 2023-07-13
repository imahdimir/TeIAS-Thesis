"""

    """

from pathlib import Path

from namespace_mahdimir import tse_github_data_url as tgdu
from namespace_mahdimir import tse as tse_ns

# namespace   %%%%%%%%%%%%%%%%%%%%%%%%%%%
c = tse_ns.Col()

# class       %%%%%%%%%%%%%%%%%%%%%%%%%%%
class GDU :
    g = tgdu.GitHubDataUrl()

    slf = tgdu.m + 'TeIAS-Thesis'

    src_adj_rets = g.adj_ret
    src_rf = g.rf
    mkt_indx_s = g.tedpix
    src_codal_ltrs = g.codal_ltrs
    src_codal_tics_2_ftics = g.codal_tics_2_ftics
    src_d_ins_ind = g.ind_ins

class Dirs :
    td = Path('temp_data/')

class FPN :
    dyr = Dirs()

    t0 = dyr.td / 't0.prq'
    t1 = dyr.td / 't1.prq'
    t2 = dyr.td / 't2.prq'
    t3 = dyr.td / 't3.prq'
    t4 = dyr.td / 't4.prq'
    t5 = dyr.td / 't5.prq'
    t6 = dyr.td / 't6.prq'
    t7 = dyr.td / 't7.prq'
    t8 = dyr.td / 't8.prq'
    t9 = dyr.td / 't9.prq'

class Params :
    days_2_rm_after_ipo = 40
    start_end_window = (-60 , -1)
    min_uniq_vals_in_window = 30
    model_significance_model = .05
    min_abs_abnormnal_ret = .005  # 0.5% = min abonormal return which considered as a significant one

class ColName :
    ret = c.ar1dlf + '-modified'
    w_strt_jd = "Window-Start-JDate"
    w_end_jd = "Window-End-JDate"
    w_strt_idx = "Window-Start-Index"
    w_end_idx = "Window-End-Index"
    obs = "NObs"
    nuniq = "NUniqVals"
    to_est = "to-estimate-model"
    alpha = "alpha"
    alpha_std = "alpha-std"
    alpha_t = "alpha-t"
    alpha_p = "alpha-p"
    beta = "beta"
    beta_std = "beta-std"
    beta_t = "beta-t"
    beta_p = "beta-p"
    f_stat = "F-stat"
    f_p = "F-p"
    r2 = "R2"
    r2_adj = "R2-adj"
    dw = "DW"
    cred_model = 'is-Credible-Model'
    expctd_exss_ret = 'Expected-Excess-Return'
    abnrml_ret = 'Abnormal-Return'
    is_abnrml_ret_signfcnt = 'Is-Abnormal-Return-Significant'
    nws_eff_d = 'News-Effective-Date'
    nws_eff_jd = 'News-Effective-JDate'
    nws_type = 'NewsType'

    nbss = 'Net-Buy-Share-Ins'
    nbsd = 'Net-Buy-Share-Ind'

    avs = 'Daily-Avg-Net-Buy-Share-Ins'
    avd = 'Daily-Avg-Net-Buy-Share-Ind'

    avts = 'Daily-Avg-Net-Buy-Share-Ins-Traded'
    avtd = 'Daily-Avg-Net-Buy-Share-Ind-Traded'

    avex_buy_s = 'Daily-Avg-Net-Buy-Share-Ins-Excl-Zeros'
    avex_sell_s = 'Daily-Avg-Net-Sell-Share-Ins-Excl-Zeros'
    avex_buy_d = 'Daily-Avg-Net-Buy-Share-Ind-Excl-Zeros'
    avex_sell_d = 'Daily-Avg-Net-Sell-Share-Ind-Excl-Zeros'

    xb_smpl_s = 'Excess-Buy-Simple-Ins'
    xb_smpl_d = 'Excess-Buy-Simple-Ind'

    xb_smpl_trd_s = 'Excess-Buy-Simple-Ins-Traded'
    xb_smpl_trd_d = 'Excess-Buy-Simple-Ind-Traded'

    xb_s = 'Excess-Buy-Ins'
    xs_s = 'Excess-Sell-Ins'

    xb_d = 'Excess-Buy-Ind'
    xs_d = 'Excess-Sell-Ind'

    # module 9

    rm1 = 'R(-1)'
    rm2m5 = 'R(-2,-5)'
    rm6m27 = 'R(-6,-27)'
    rm28m119 = 'R(-28,-119)'

    mrm1 = 'MR(-1)'
    mrm2m5 = 'MR(-2,-5)'
    mrm6m27 = 'MR(-6,-27)'
    mrm28m119 = 'MR(-28,-119)'

    arm1 = 'MAR(-1)'
    arm2m5 = 'MAR(-2,-5)'
    arm6m27 = 'MAR(-6,-27)'
    arm28m119 = 'MAR(-28,-119)'

class NewsType :
    good = 'Good'
    bad = 'Bad'
    neutral = 'Neutral'
    unknown = 'Unknown'

# class instances   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dyr = Dirs()
fpn = FPN()
pa = Params()
gdu = GDU()
cn = ColName()
nws_type = NewsType()

def main() :
    pass

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

    ##
