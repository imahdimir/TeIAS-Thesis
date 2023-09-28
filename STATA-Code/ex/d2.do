/

ssc install estout
ssc install outreg2
ssc install ftools

set more off
set varabbrev off


clear all

* change cwd
cd "C:\Users\imahd\Downloads\OneDrive - khatam.ac.ir\thesis"

// import data
import delimited stata_2.csv, clear


// encode month_highest and month_lowest dummies
encode month_highest , gen(mh)
encode month_lowest , gen(ml)


// encode firmtickers, for using as the panel variable
encode firmticker , gen(fid)


// make time variable
egen time = group(date), label

// set panel variable id and time
xtset fid time


// label adjusted returns vars for reporting purpose
label variable r1 "R(-1)"
label variable r2 "R(-2, -5)"
label variable r6 "R(-6, -27)"
label variable r28 "R(-28, -119)"



// excess buy and sell for institutions and individuals
eststo clear


// ins - buy
eststo: xtscc excessbuyins r1 r2 r6 r28 i.mh i.ml
outreg2 using c20.tex, replace keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// ins - sell
eststo: xtscc excesssellins  r1 r2 r6 r28 i.mh i.ml
outreg2 using c20.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)


// individuals
// buy
eststo: xtscc excessbuyind r1 r2 r6 r28 i.mh i.ml
outreg2 using c20.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)

// sell
eststo: xtscc excesssellind r1 r2 r6 r28 i.mh i.ml
outreg2 using c20.tex, append keep(r1 r2 r6 r28) nocons label tex(fragment) bdec(3) sdec(2)



