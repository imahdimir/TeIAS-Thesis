/

set more off
clear all


**# Task 1

import delimited endline_child.csv, clear

drop if c_correct_mat_4 == -99


keep id

save endline_ids, replace


import delimited baseline.csv, clear

keep id treat

merge 1:1 id using endline_ids


sum
scalar base_n = r(N)
dis base_n

gen ov = 0
replace ov = 1 if _merge == 1

sum ov
scalar ov_atr = r(mean)
dis ov_atr

drop ov


gen t = 0 if treat == 1
replace t = 1 if treat == 1 & _merge == 1

sum t
scalar t_atr = r(mean)
dis t_atr

drop t

gen c = 0 if treat == 0
replace c = 1 if treat == 0 & _merge == 1

sum c
scalar c_atr = r(mean)
dis c_atr

drop c



