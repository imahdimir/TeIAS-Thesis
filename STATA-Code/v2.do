/
**# Script Description

	/*

	For the second version of slides 

	*/



**# Preamble

	ssc install estout
	ssc install outreg2
	ssc install ftools
	ssc install xtscc

	set more off
	set varabbrev off
	clear all
	macro drop _all
	
	cd "C:\Users\imahd\Dropbox\TeIAS-Thesis\STATA-Code"

	global dta "../STATA-Data"
	global tbl "../STATA-Tables"



**# Prepare Data


	* import data
	import delimited $dta/main.csv, clear

	* encode contempronous news dummies
	encode newstype, gen(nt)
	
	tab nt

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
	rename netbuyshareins ns
	rename netbuyshareind nd



**# Estimations


	**# Ins
    
	eststo clear

	* both XTPCSE and MA
	eststo: xtscc ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml
	outreg2 using $tbl/v2.tex, replace label ctitle("S-XTSCC") tex(pr) tstat
	outreg2 using $tbl/v2.doc, replace label ctitle("S-XTSCC") tex(pr) tstat
	
	eststo: xtscc ns r1 r2 r6 r28 i.wd i.mh i.ml
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTSCC-wo News") tex(pr) tstat
	

	* neither XTPCSE nor MA
	eststo: xtreg ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTREG") tex(pr) tstat

	eststo: xtreg ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml, fe
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTREG-fe") tex(pr) tstat

	eststo: xtreg ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml, vce(cluster fid)
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTREG-cluster") tex(pr) tstat

	eststo: xtreg ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml, fe vce(cluster fid)
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTREG-fe-cluster") tex(pr) tstat

	/*
	does not converge

	eststo: xtpcse ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml, pairwise
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTPCSE") tex(pr) tstat
	*/

	eststo: xtpcse ns r1 r2 r6 r28 i.nt i.wd i.mh i.ml, hetonly
	outreg2 using $tbl/v2.tex, append label ctitle("S-XTPCSE-hetonly") tex(pr) tstat
	outreg2 using $tbl/v2.doc, append label ctitle("S-XTPCSE-hetonly") tex(pr) tstat
	
	**# Ind

	* both XTPCSE and MA
	eststo: xtscc nd r1 r2 r6 r28 i.nt i.wd i.mh i.ml
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTSCC") tex(pr) tstat
	outreg2 using $tbl/v2.doc, append label ctitle("D-XTSCC") tex(pr) tstat
	
	eststo: xtscc nd r1 r2 r6 r28 i.wd i.mh i.ml
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTSCC-No News") tex(pr) tstat
	

	* neither XTPCSE nor MA
	eststo: xtreg nd r1 r2 r6 r28 i.nt i.wd i.mh i.ml
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTREG") tex(pr) tstat

	eststo: xtreg nd r1 r2 r6 r28 i.nt i.wd i.mh i.ml, fe
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTREG-fe") tex(pr) tstat

	eststo: xtreg nd r1 r2 r6 r28 i.nt i.wd i.mh i.ml, vce(cluster fid)
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTREG-cluster") tex(pr) tstat

	eststo: xtreg nd r1 r2 r6 r28 i.nt i.wd i.mh i.ml, fe vce(cluster fid)
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTREG-fe-cluster") tex(pr) tstat

	eststo: xtpcse nd r1 r2 r6 r28 i.nt i.wd i.mh i.ml, hetonly
	outreg2 using $tbl/v2.tex, append label ctitle("D-XTPCSE-hetonly") tex(pr) tstat
	outreg2 using $tbl/v2.doc, append label ctitle("D-XTPCSE-hetonly") tex(pr) tstat
	

	