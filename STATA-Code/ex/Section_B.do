set more off
set varabbrev off
clear all
macro drop _all
**# Section B
* In this do file I try to find and calculate the minimum distance between villages and facilities. After that, I am going to plot the histogram of the distance by each district.
{
**# Predict Minimum Distance
* Using "geonear" written by (Picard, 2010) to find and predict the nearest neighbors
use ../data/Village_Details, clear
* ssc install geonear
geonear village_id gps_north_village gps_east_village using "../data/Govt_Facilities", n(facility_id gps_north gps_east)

label var km_to_nid "Nearest Facility Distance (Km)"
label var nid "Facility Id Number"

// varibale "km_to_nid" is the variable of interest
* Summary statistics for the variable of interest: km_to_nid

eststo clear
estpost tabstat km_to_nid, by(dist_code) c(stat) stat(mean p50 sd min max n)
esttab using ../results/tables/sectionB_summary.tex, replace cells("mean(fmt(%6.2fc)) p50 sd(fmt(%6.2fc)) min max count(fmt(%6.0fc))") nonumber nomtitle nonote noobs label collabels("Mean" "Median" "SD" "Min" " Max" "N") title("Nearest Distance Summary Statistics by Districts")
eststo clear	
}

{
**# Histogram
* Using tw hist in order to plot the requested histograms
tw (hist km_to_nid, by(dist_code) percent), xtitle("Distribution of Facility Distance by District Code (Km)")
graph export "..\results\figures\SectionB_Bonus.png", as(png), replace
}
* Saving Final Dataset
compress
save ../proc/sectionB_NearestFacilityDist.dta, replace

exit