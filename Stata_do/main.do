/

ssc install estout
ssc install outreg2

set more off
set varabbrev off


clear all

* change cd
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
**# Bookmark #1


// encode month_highest and month_lowest dummies
encode month_highest , gen(mh)
encode month_lowest , gen(ml)


// encode firmtickers, for using as the panel variable
encode firmticker , gen(fid)


// convert date to STATA date
gen numdate = date(date, "YMD")

// format numdate to make it readable
format numdate %td


// set the panel variable and time variable
xtset fid numdate


// excess buy and sell for institutions and individuals
eststo clear


// institutions
eststo: xtpcse excessbuyins r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0 c0.tex, replace

eststo: xtpcse excessbuyins r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excessbuyins r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excessbuyins r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append



eststo: xtpcse excesssellins r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excesssellins r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excesssellins r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excesssellins r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append


// individuals
eststo: xtpcse excessbuyind r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excessbuyind r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excessbuyind r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excessbuyind r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append



eststo: xtpcse excesssellind r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excesssellind r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excesssellind r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append

eststo: xtpcse excesssellind r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c0, append



// By big and small firms
eststo clear


// make big and small frames
frame rename default orig

frame copy orig big
frame copy orig small

cwf big
keep if firmsizetercile == 2

cwf small
keep if firmsizetercile == 0


// institutions

// ins - big - buy
cwf big

eststo: xtpcse excessbuyins r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, replace

eststo: xtpcse excessbuyins r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyins r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyins r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append


// ins - big - sell
eststo: xtpcse excesssellins r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellins r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellins r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellins r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append


// ins - small - buy
cwf small

eststo: xtpcse excessbuyins r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyins r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyins r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyins r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append


// isn - small - sell
eststo: xtpcse excesssellins r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellins r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellins r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellins r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append


// individuals

// ind - big - buy
cwf big


eststo: xtpcse excessbuyind r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyind r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyind r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyind r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append


// ind - big - sell
eststo: xtpcse excesssellind r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellind r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellind r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellind r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append



// ind - small - buy

cwf small


eststo: xtpcse excessbuyind r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyind r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyind r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excessbuyind r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append


// ind - small - sell

eststo: xtpcse excesssellind r1 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellind r2 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellind r6 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append

eststo: xtpcse excesssellind r28 i.nt i.l1_nt i.l2_nt i.mh i.ml, hetonly
outreg2 using c1, append









		