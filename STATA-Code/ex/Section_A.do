set more off
set varabbrev off
clear all
macro drop _all
use ../data/training, clear
frame rename default orig

// Section  A: Data & Background

**# Task 1: How many members enrolled in a grid other than their own

frame copy orig tt
cwf tt
keep if grid != grid_enrolled 
keep mem_id hh_id cluster_id grid grid_enrolled
count

save "../proc/sectionA_task1.dta", replace
export excel using "../results/tables/completeion.xls", sheet("task 1") firstrow(variables) replace
cwf orig
frame drop tt

**# Task 2: How many grids (Grid in which household exists) are there from which at least 1 individual enrolled in the training?
frame copy orig tt
cwf tt
collapse (sum) enrolled ,by(grid)
keep if enrolled>0
save "../proc/sectionA_task2.dta", replace
export excel using "../results/tables/completeion.xls", sheet("task 2") firstrow(variables) replace
cwf orig
frame drop tt

**# Task 3:Limit the data to the grids from Task 2. Then restrict the data to the individuals who were covered, aged 15 to 40 and were NOT part of the control group
frame copy orig tt
cwf tt
egen grid_w_enrol = sum(enrolled),by(grid)
replace grid_w_enrol = grid_w_enrol>0
keep if grid_w_enrol ==1
keep if covered==1
keep if inrange(age,15,40)
drop if treatment_status == 4
export excel using "../results/tables/completeion.xls", sheet("task 3") firstrow(variables) replace
save "../proc/sectionA_task3.dta", replace

cwf orig
frame drop tt

**# Task 4: For each grid, find the completion rate in each treatment arm. Name this variable "completed_training".
replace dropout =0 if mi(dropout)
gen completed = dropout == 0 if accepted_voucher==1
egen completion_train = mean(completed) , by(treatment_status grid) 

frame copy orig tt
cwf tt
keep if !mi(completion_train)
collapse (mean) completion_train, by(grid treatment_status)
sort grid treatment_status

export excel using "../results/tables/completeion.xls", sheet("task 4") firstrow(variables) replace
save "../proc/sectionA_task4.dta", replace

cwf orig
frame drop tt

**# Task 5:  Run a household level regression to determine the impact of distance on training completion in different treatment arms, specifically OVM and VBT. In this regression specification, you should control for different treatment and grid dummies, and cluster the regression results on Cluster IDs.
eststo clear
eststo: qui reghdfe completion_train distance_to_training_center i.treatment_status, a(i.hh_id##i.grid) cluster(cluster_id)
esttab using ../results/tables/sectionA_task5.tex, p r2 noomitted la replace nobaselevels
esttab using ../results/tables/sectionA_task5.doc, p r2 noomitted la replace nobaselevels
eststo clear

**# Task 6: Prove that VBT coefficient is statistically different from the OVM coefficient and also that both these coefficients are statistically different from the T1 coefficient.
testparm i.treatment_status
eststo clear
eststo: qui reghdfe completion_train distance_to_training_center ib2.treatment_status, a(i.hh_id##i.grid) cluster(cluster_id)
esttab using ../results/tables/sectionA_task6.doc, p r2 noomitted replace nobaselevels la
eststo clear

**# Task 7: Show that the training completion rates in OVMs at the minimum distance from a training center are no different from the training completion rates in VBTs
egen minimumDist = min(distance_to_training_center) if treatment_status==2
replace minimumDist = 0 if treatment_status==3
gen tre = 1 if minimumDist>0
replace tre = 0 if minimumDist==0
eststo clear
eststo: qui reg completion_train tre, cluster(cluster_id)
esttab using ../results/tables/sectionA_task7.doc, p r2 noomitted replace nobaselevels la
eststo clear

**# Task 8
clear
use ../data/stipend, clear
{
**# Checkinh the rows uniqueness
duplicates report PSU_code hh_id 
assert r(unique_value) == r(N)
}
{
**# Data Cleaning

{
**# Change missings to "No"    

vl create cols = (send* not_enough_stipend)
foreach var of varlist $cols{
	replace `var' = 0 if `var'==.
}
replace send_if_stipend_2000 = -99 if send_if_stipend_1500 == 1
replace send_if_stipend_3000 = -99 if send_if_stipend_1500 == 1 | send_if_stipend_2000 == 1
replace not_enough_stipend = -99 if not_enough_stipend == 0

/*
label define yesno1 1 "Yes" 0 "No"
label values $cols yesno1

label values $cols yesno
label list
*/
* Checking the values of each column
levelsof send_if_stipend_1500
levelsof send_if_stipend_1500

levelsof send_if_stipend_1500
sca de v15 = r(levels)
sca list
assert send_if_stipend_1500==1 | send_if_stipend_1500==0

levelsof not_enough_stipend
assert not_enough_stipend==1 | not_enough_stipend==-99
}

foreach var in $cols{
	gen test_`var' = 1 if `var'==1
}
* Cheking that each row has exactly one "Yes"
vl create cols_test = (test_send_if_stipend_1500 test_send_if_stipend_2000 test_send_if_stipend_3000 test_not_enough_stipend)

foreach var in $cols_test{
	replace `var' = 0 if `var'==.
}
gen test = test_send_if_stipend_1500 + test_send_if_stipend_2000 + test_send_if_stipend_3000  + test_not_enough_stipend
levelsof test
assert test==1

drop $cols_test test

* Creating variable "overall"

gen overall = send_if_stipend_1500 + send_if_stipend_2000 + send_if_stipend_3000 + not_enough_stipend

* Save dataset
compress
save ../proc/sectionA_task8.dta, replace
}

exit