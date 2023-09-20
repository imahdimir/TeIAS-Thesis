/
**# Script Description

	/*

	 

	*/



**# Preamble
	
	* Packages to install (Do not re-install, it is time consuming)
	ssc install estout
	ssc install outreg2
	ssc install ftools
	ssc install xtscc

	set more off
	set varabbrev off
	clear all
	macro drop _all

	global dta "../STATA-Data"
	global tbl "../STATA-Tables/v3"
	
	global vt $tbl/v3.tex
	global vd $tbl/v3.doc
	global vt1 $tbl/v3-robustness.tex
	global vd1 $tbl/v3-robustness.doc
	global vt2 $tbl/v3-big-small.tex
	global vd2 $tbl/v3-big-small.doc



**# Prepare Data


	* import data
	import delimited $dta/main.csv, clear

	* encode contempronous news dummies
	gen nt0 = 1 if newstype == "No-News"
	replace nt0 = 2 if newstype == "Good"
	replace nt0 = 3 if newstype == "Bad"

	tostring nt0, replace
	encode nt0, gen(nt)
	
	tab nt
	
	* encode lag1_news
	gen l1nt0 = 1 if lag1_news == "No-News"
	replace l1nt0 = 2 if lag1_news == "Good"
	replace l1nt0 = 3 if lag1_news == "Bad"

	tostring l1nt0, replace
	encode l1nt0, gen(l1nt)
	
	tab l1nt
	
	* encode lag2_news
	gen l2nt0 = 1 if lag2_news == "No-News"
	replace l2nt0 = 2 if lag2_news == "Good"
	replace l2nt0 = 3 if lag2_news == "Bad"

	tostring l2nt0, replace
	encode l2nt0, gen(l2nt)
	
	tab l2nt
	
	
	* encode month_highest and month_lowest dummies
	encode month_highest, gen(mh)
	encode month_lowest, gen(ml)
	
	tab mh
	tab ml

	* encode firmtickers, for using as the panel variable
	encode firmticker, gen(fid)
	
	tab fid

	* encode weekday
	tostring weekday, replace
	encode weekday, gen(wd)
	
	tab wd
	
	* encode j-year
	tostring jyear, replace
	encode jyear, gen(jy)
	
	tab jy


	* make time variable
	egen time = group(date), label

	* set panel variables id and time
	xtset fid time

	* label vars
	label variable r1 "R(-1)"
	label variable r2 "R(-2, -5)"
	label variable r6 "R(-6, -27)"
	label variable r28 "R(-28, -119)"

	* make things easier
	rename instradeimbalancefillna_0 st
	rename indtradeimbalancefillna_0 dt
	
	rename inscountimbalancefillna_0 sc
	rename indcountimbalancefillna_0 dc


**# Estimations


	**# Ins
    
	eststo clear

	* both XTPCSE and MA
	
	eststo: xtscc st r1 r2 r6 r28 i.nt i.wd i.mh i.ml i.jy
	outreg2 using $vt, replace label ctitle("S") tex(pr)
	outreg2 using $vd, replace label ctitle("S") tex(pr)
	
	eststo: xtscc st r1 r2 r6 r28 i.nt i.l1nt i.wd i.mh i.ml i.jy
	outreg2 using $vt, append label ctitle("S") tex(pr)
	outreg2 using $vd, append label ctitle("S") tex(pr)
	
	eststo: xtscc st r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt, append label ctitle("S") tex(pr)
	outreg2 using $vd, append label ctitle("S") tex(pr)
	
	
	**# Ind

	* both XTPCSE and MA
	
	eststo: xtscc dt r1 r2 r6 r28 i.nt i.wd i.mh i.ml i.jy
	outreg2 using $vt, append label ctitle("D") tex(pr)
	outreg2 using $vd, append label ctitle("D") tex(pr)
	
	eststo: xtscc dt r1 r2 r6 r28 i.nt i.l1nt i.wd i.mh i.ml i.jy
	outreg2 using $vt, append label ctitle("D") tex(pr)
	outreg2 using $vd, append label ctitle("D") tex(pr)
	
	eststo: xtscc dt r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt, append label ctitle("D") tex(pr)
	outreg2 using $vd, append label ctitle("D") tex(pr)
	
	
	
**# Count imbalance robustness

	eststo clear

	* both XTPCSE and MA
	
	eststo: xtscc sc r1 r2 r6 r28 i.nt i.wd i.mh i.ml i.jy
	outreg2 using $vt1, replace label ctitle("S") tex(pr)
	outreg2 using $vd1, replace label ctitle("S") tex(pr)
	
	eststo: xtscc sc r1 r2 r6 r28 i.nt i.l1nt i.wd i.mh i.ml i.jy
	outreg2 using $vt1, append label ctitle("S") tex(pr)
	outreg2 using $vd1, append label ctitle("S") tex(pr)
	
	eststo: xtscc sc r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt1, append label ctitle("S") tex(pr)
	outreg2 using $vd1, append label ctitle("S") tex(pr)
	
	**# Ind

	* both XTPCSE and MA
	
	eststo: xtscc dc r1 r2 r6 r28 i.nt i.wd i.mh i.ml i.jy
	outreg2 using $vt1, append label ctitle("D") tex(pr)
	outreg2 using $vd1, append label ctitle("D") tex(pr)
	
	eststo: xtscc dc r1 r2 r6 r28 i.nt i.l1nt i.wd i.mh i.ml i.jy
	outreg2 using $vt1, append label ctitle("D") tex(pr)
	outreg2 using $vd1, append label ctitle("D") tex(pr)
	
	eststo: xtscc dc r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt1, append label ctitle("D") tex(pr)
	outreg2 using $vd1, append label ctitle("D") tex(pr)
	
	
**# Big & Small Firms Analysis

	* make big and small frames
	frame copy default big
	frame copy default small

	cwf big
	keep if firmsizetercile == 2

	* make time variable
	drop time
	egen time = group(date)

	* set panel variable id and time
	xtset fid time


	cwf small
	keep if firmsizetercile == 0

	* make time variable
	drop time
	egen time = group(date)

	* set panel variable id and time
	xtset fid time


	* Clear stored ests
	eststo clear
	
	* Ins
	
	* within big firms
	cwf big
	
	eststo: xtscc st r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt2, replace label ctitle("S-Big") tex(pr)
	outreg2 using $vd2, replace label ctitle("S-Big") tex(pr)
	
	* within small firms
	cwf small
	
	eststo: xtscc st r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt2, append label ctitle("S-Small") tex(pr)
	outreg2 using $vd2, append label ctitle("S-Small") tex(pr)
	
	* Ind
	
	* within big firms
	cwf big
	
	eststo: xtscc dt r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt2, append label ctitle("D-Big") tex(pr)
	outreg2 using $vd2, append label ctitle("D-Big") tex(pr)
	
	* within small firms
	cwf small
	
	eststo: xtscc dt r1 r2 r6 r28 i.nt i.l1nt i.l2nt i.wd i.mh i.ml i.jy
	outreg2 using $vt2, append label ctitle("D-Small") tex(pr)
	outreg2 using $vd2, append label ctitle("D-Small") tex(pr)
	
	
