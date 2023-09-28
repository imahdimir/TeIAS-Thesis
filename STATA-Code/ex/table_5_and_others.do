clear

//cd Mir_Mahdi_2023

ssc install reghdfe
ssc install ftools
ssc install outreg2
ssc install estout

use master.dta

eststo: quietly reghdfe dep1 i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, replace

eststo: quietly reghdfe dep2 i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

esttab using table_5.tex, replace se title("Table 5") mtitles("Log-OLS" "LPM")

eststo clear

quietly reghdfe dep_fac i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_fac_LPM i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_stu i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_stu_LPM i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_stf i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_stf_LPM i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_alu i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append

quietly reghdfe dep_alu_LPM i.post_scanned, absorb(bib_doc_id year_location) cluster(bib_doc_id)
outreg2 using coefs, append


