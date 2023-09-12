/

ssc install estout
ssc install outreg2
ssc install ftools
ssc install xtscc

set more off
set varabbrev off

clear all

// change cwd
cd "C:\Users\imahd\Downloads\OneDrive - khatam.ac.ir\thesis"

// import data
import delimited stata_1.csv, clear

// encode dummies so they can be used in the regression
encode newstype, gen(nt)
tabulate nt

encode lag1_news, gen(l1_nt)
tabulate l1_nt

encode lag2_news, gen(l2_nt)
tabulate l2_nt

// encode month_highest and month_lowest dummies
encode month_highest , gen(mh)
encode month_lowest , gen(ml)

// encode firmtickers, for using as the panel variable
encode firmticker , gen(fid)

// make time variable
egen time = group(date), label

// set panel variable id and time
xtset fid time

// label vars
label variable r1 "R(-1)"
label variable r2 "R(-2, -5)"
label variable r6 "R(-6, -27)"
label variable r28 "R(-28, -119)"

// Estimations
eststo clear

// institutions

// buy
/* eststo: xtpcse excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, pairwise
eststo: xtpcse excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, c(psar1) hetonly
eststo: xtpcse excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, c(psar1) pairwise */

eststo: xtscc excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c0.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

outreg2 using c00.tex, replace label tex(fragment) bdec(3) sdec(2)


// sell
eststo: xtscc excesssellins  r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c0.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

outreg2 using c00.tex, append label tex(fragment) bdec(3) sdec(2)


// individuals
// buy
eststo: xtscc excessbuyind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c0.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

outreg2 using c00.tex, append label tex(fragment) bdec(3) sdec(2)

// sell
eststo: xtscc excesssellind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c0.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

outreg2 using c00.tex, append label tex(fragment) bdec(3) sdec(2)


// make big and small frames
frame copy default big
frame copy default small

cwf big
keep if firmsizetercile == 2

// make time variable
drop time
egen time = group(date)

// set panel variable id and time
xtset fid time


cwf small
keep if firmsizetercile == 0

// make time variable
drop time
egen time = group(date)

// set panel variable id and time
xtset fid time


// Clear stored ests
eststo clear

// institutions

// Large
cwf big

// Buy
eststo: xtscc excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// Sell
eststo: xtscc excesssellins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml 
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)


// Small
cwf small

// Buy
eststo: xtscc excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// Sell
eststo: xtscc excesssellins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// Individuals

// large
cwf big

// Buy

eststo: xtscc excessbuyind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// Sell
eststo: xtscc excesssellind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// Small
cwf small

// Buy
eststo: xtscc excessbuyind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// Small - Sell
eststo: xtscc excesssellind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml
outreg2 using c1.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)



// not controling for news
// back to defaul dataframe
cwf default


// Estimations
eststo clear

// institutions

// buy
eststo: xtscc excessbuyins r1 r2 r6 r28 i.mh i.ml
outreg2 using c2.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// sell
eststo: xtscc excesssellins  r1 r2 r6 r28 i.mh i.ml
outreg2 using c2.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)


// individuals
// buy
eststo: xtscc excessbuyind r1 r2 r6 r28 i.mh i.ml
outreg2 using c2.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// sell
eststo: xtscc excesssellind r1 r2 r6 r28 i.mh i.ml
outreg2 using c2.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)


// auto-correlation

// Estimations
eststo clear

// set trace on


// autocorelleation of q = 1, 2, 3

forval m=0/3 {
	
	dis `m'

	// institutions
	// buy
	eststo: xtscc excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, lag(`m')
	outreg2 using q`m'.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)


	// sell
	eststo: xtscc excesssellins  r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, lag(`m')
	outreg2 using q`m'.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

	
	// individuals
	// buy
	eststo: xtscc excessbuyind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, lag(`m')
	outreg2 using q`m'.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

	// sell
	eststo: xtscc excesssellind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, lag(`m')
	outreg2 using q`m'.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

}

// set trace off


// Estimations
eststo clear


// no spatial corrleation but having hetroskedasity across panels and auto-correlation


// ins - buy
eststo: xtpcse excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly correlation(psar1)
outreg2 using c4.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// ins - sell
eststo: xtpcse excesssellins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly correlation(psar1)
outreg2 using c4.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// ind - buy
eststo: xtpcse excessbuyind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly correlation(psar1)
outreg2 using c4.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// sell
eststo: xtpcse excesssellind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly correlation(psar1)
outreg2 using c4.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)


// Estimations
eststo clear


// no spatioal correlation but het and auto, with xtreg, fe cluster()
eststo: xtreg excessbuyins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, fe vce(cluster fid)
outreg2 using c5.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// ins - sell
eststo: xtreg excesssellins r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, fe vce(cluster fid)
outreg2 using c5.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// ind - buy
eststo: xtreg excessbuyind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, fe vce(cluster fid)
outreg2 using c5.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// sell
eststo: xtreg excesssellind r1 r2 r6 r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, fe vce(cluster fid)
outreg2 using c5.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)




















		