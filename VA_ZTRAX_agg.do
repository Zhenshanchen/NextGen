clear all
set more off
cap log close
set seed 123456789

*set your local dropbox directory
*Change it manually if it doesn't work - e.g., if you didn't install dropbox under c
global dropbox "`:environment USERPROFILE'\Dropbox\"
*global dropbox "D:\Dropbox"

global root "$dropbox\NextGen"
global GIS "$root\GISdata"

*set up your Zitrax directory here
global Zitrax "E:\Zitrax"
 *current assessment and sales data directory
global dta1 "$Zitrax\current_assess_transaction"
 *historic assessment data directory
global dta2 "$Zitrax\historic_assessment"
**** We only process the current assessment data here, historic assessment data can be processed in a similar way.

 *output directory
global temp "$root\dta\temp"
global dta0 "$root\dta"
********************************************************

**************************************
* Begin assessment data aggregation  *
**************************************
*get ImportParcelID matched pairs between hist assess and current assess
/*
use "$Zitrax\dta\historic_assess_24.dta",clear
keep PropertyFullStreetAddress PropertyCity PropertyAddressUnitNumber PropertyZip PropertyZip4 ImportParcelID
duplicates drop ImportParcelID, force

ren ImportParcelID ha_ImportParcelID
label variable ha_ImportParcelID "id from historic assessemnt file"
drop if trim(PropertyFullStreetAddress)==""
drop if trim(PropertyCity)==""

joinby PropertyFullStreetAddress PropertyCity PropertyAddressUnitNumber using "$Zitrax\dta\current_assess_24.dta"
keep ha_ImportParcelID ImportParcelID
drop if ha_ImportParcelID==.|ImportParcelID==.
duplicates report
save $dta\ha_to_curr_idKey.dta, replace


use "$Zitrax\dta\historic_assess_24.dta",clear
tab TaxYear
keep if TaxYear>=1994

keep RowID ImportParcelID FIPS State County ExtractDate AssessorParcelNumber ///
UnformattedAssessorParcelNumber PropertyFullStreetAddress PropertyCity PropertyZip /// 
PropertyZoningDescription TaxIDNumber TaxAmount TaxYear NoOfBuildings LegalTownship ///
LotSizeAcres LotSizeSquareFeet PropertyAddressLatitude PropertyAddressLongitude BatchID

ren ImportParcelID ha_ImportParcelID
joinby ha_ImportParcelID using "$dta0\ha_to_curr_idkey.dta",unmatched(master)
drop _merge
/*_merge==1 means the address is missing or doesn't show up at all in current assess file, so it's 
not associated with an ImportParcelID */
global strings "County AssessorParcelNumber UnformattedAssessorParcelNumber PropertyFullStreetAddress PropertyCity PropertyZip PropertyZoningDescription TaxIDNumber LegalTownship"
foreach v in $strings {
replace `v'=trim(`v')
}
*Now the ImportParcelID and ha_ImportParcelID gaurantee the tax record can be matched with transactions (try both at merging),
*even if the ImportParcelID is changed - at least one of them should be right

destring(PropertyZip), replace

append using "$Zitrax\dta\current_assess_24.dta"

keep RowID ImportParcelID ha_ImportParcelID FIPS State County ExtractDate AssessorParcelNumber ///
UnformattedAssessorParcelNumber PropertyFullStreetAddress PropertyCity PropertyZip /// 
PropertyZoningDescription TaxIDNumber TaxAmount TaxYear NoOfBuildings LegalTownship ///
LotSizeAcres LotSizeSquareFeet PropertyAddressLatitude PropertyAddressLongitude BatchID

duplicates drop
save "$dta0\all_assess_MD.dta",replace
*/
use "$Zitrax\dta\current_assess_51.dta",clear
keep RowID ImportParcelID FIPS State County ExtractDate AssessorParcelNumber ///
UnformattedAssessorParcelNumber PropertyFullStreetAddress PropertyCity PropertyZip /// 
PropertyZoningDescription TaxIDNumber TaxAmount TaxYear NoOfBuildings LegalTownship ///
LotSizeAcres LotSizeSquareFeet PropertyAddressLatitude PropertyAddressLongitude BatchID

duplicates drop
save "$dta0\all_assess_VA.dta",replace

**************************************
*  End assessment data aggregation   *
**************************************

*****************************************************************************************************
* Begin standard attribute processing (various attributes from zillow hist or current assess)       *
*****************************************************************************************************
*Get current sales data
use "$Zitrax\dta\current_assess_sale_51.dta",clear
drop SalesPriceAmountStndCode SellerFullName BuyerFullName DocumentDate ///
 RecordingDocumentNumber
* in this file the RowID is not unique and shows two sales for some parcels 
save "$temp\current_assess_sale_VA.dta",replace

*current assess value data
use "$Zitrax\dta\current_assess_value_51.dta",clear
keep RowID ImprovementAssessedValue LandAssessedValue TotalAssessedValue AssessmentYear
save "$temp\current_assess_value_VA.dta",replace

use "$Zitrax\dta\current_assess_building_51.dta",clear
keep RowID NoOfUnits PropertyCountyLandUseDescription PropertyCountyLandUseCode BuildingOrImprovementNumber BuildingConditionStndCode ///
YearBuilt EffectiveYearBuilt NoOfStories TotalRooms TotalBedrooms TotalCalculatedBathCount HeatingTypeorSystemStndCode AirConditioningStndCode FireplaceNumber ///
RoofStructureTypeStndCode FoundationTypeStndCode FIPS
save "$temp\current_assess_VA_building.dta",replace

/*hist assess value data
use "$Zitrax\dta\historic_assess_value_24.dta",clear
keep RowID ImprovementAssessedValue LandAssessedValue TotalAssessedValue AssessmentYear
save "$temp\historic_assess_value_MD.dta",replace

use "$Zitrax\dta\historic_assess_building_24.dta",clear
keep RowID NoOfUnits PropertyCountyLandUseDescription PropertyCountyLandUseCode BuildingOrImprovementNumber BuildingConditionStndCode ///
YearBuilt EffectiveYearBuilt NoOfStories TotalRooms TotalBedrooms TotalCalculatedBathCount HeatingTypeorSystemStndCode AirConditioningStndCode FireplaceNumber ///
RoofStructureTypeStndCode FoundationTypeStndCode FIPS
destring PropertyCountyLandUseCode,replace
save "$temp\historic_assess_MD_building.dta",replace
*/
use "$Zitrax\dta\current_assess_buildingarea_51.dta",clear
sort RowID BuildingAreaSequenceNumber
ren BuildingAreaSqFt SQFT
keep if BuildingAreaStndCode=="BSF"|BuildingAreaStndCode=="BSH"|BuildingAreaStndCode=="BSN"|BuildingAreaStndCode=="BSP"|BuildingAreaStndCode=="BSU"|BuildingAreaStndCode=="BSY"|BuildingAreaStndCode=="BAT"|BuildingAreaStndCode=="BAG"|BuildingAreaStndCode=="BAL"|BuildingAreaStndCode=="ST1"
replace BuildingAreaStndCode="BASE" if BuildingAreaStndCode=="BSF"|BuildingAreaStndCode=="BSH"|BuildingAreaStndCode=="BSN"|BuildingAreaStndCode=="BSP"|BuildingAreaStndCode=="BSU"|BuildingAreaStndCode=="BSY"
drop BuildingOrImprovementNumber BuildingAreaSequenceNumber FIPS BatchID

duplicates drop
duplicates tag RowID BuildingAreaStndCode, g(dup1)
egen SQFT1=sum(SQFT) if dup1==1,by(RowID BuildingAreaStndCode)
replace SQFT=SQFT1 if SQFT1~=.
drop dup1 SQFT1
duplicates drop
duplicates report RowID BuildingAreaStndCode
sort *
duplicates drop RowID BuildingAreaStndCode,force
reshape wide SQFT, i(RowID) j(BuildingAreaStndCode) string
duplicates report RowID
drop SQFTBAT
save "$temp\current_assess_VA_buildingarea.dta",replace
/*
use "$Zitrax\dta\historic_assess_buildingarea_24.dta",clear
sort RowID BuildingAreaSequenceNumber
ren BuildingAreaSqFt SQFT
keep if BuildingAreaStndCode=="BSF"|BuildingAreaStndCode=="BSH"|BuildingAreaStndCode=="BSN"|BuildingAreaStndCode=="BSP"|BuildingAreaStndCode=="BSU"|BuildingAreaStndCode=="BSY"|BuildingAreaStndCode=="BAT"|BuildingAreaStndCode=="BAG"|BuildingAreaStndCode=="BAL"|BuildingAreaStndCode=="ST1"
replace BuildingAreaStndCode="BASE" if BuildingAreaStndCode=="BSF"|BuildingAreaStndCode=="BSH"|BuildingAreaStndCode=="BSN"|BuildingAreaStndCode=="BSP"|BuildingAreaStndCode=="BSU"|BuildingAreaStndCode=="BSY"
drop BuildingOrImprovementNumber BuildingAreaSequenceNumber FIPS BatchID

duplicates drop
duplicates tag RowID BuildingAreaStndCode, g(dup1)
egen SQFT1=sum(SQFT) if dup1==1,by(RowID BuildingAreaStndCode)
replace SQFT=SQFT1 if SQFT1~=.
drop dup1 SQFT1
duplicates drop
reshape wide SQFT, i(RowID) j(BuildingAreaStndCode) string
duplicates report RowID
drop SQFTST1 SQFTBAT
save "$temp\historic_assess_MD_buildingarea.dta",replace
*/
use "$Zitrax\dta\current_assess_lotappeal_51.dta",clear
keep RowID LotSiteAppealStndCode FIPS BatchID
gen Waterfront=(LotSiteAppealStndCode=="WFS")
duplicates drop
duplicates report RowID
save "$temp\current_assess_VA_waterfront.dta",replace

use "$Zitrax\dta\current_assess_pool_51.dta",clear
keep RowID PoolStndCode FIPS BatchID
gen Pool=1
duplicates drop
drop PoolStndCode 
duplicates report RowID
duplicates drop
save "$temp\current_assess_VA_pool.dta",replace

use "$Zitrax\dta\current_assess_garage_51.dta",clear
keep RowID GarageNoOfCars GarageStndCode FIPS BatchID

duplicates drop
duplicates report RowID
duplicates drop RowID FIPS,force
drop GarageStndCode
save "$temp\current_assess_VA_garage.dta",replace
/*
use "$Zitrax\dta\historic_assess_garage_24.dta",clear
keep RowID GarageNoOfCars GarageStndCode FIPS BatchID
duplicates drop
duplicates report RowID
save "$temp\historic_assess_MD_garage.dta",replace
*/
***************************************
* End property attributes processing  *
***************************************


******************************************************************************************************
*  Limit Sample to Fairfox, Merge standard attributes from ZTRAX  *
******************************************************************************************************
use "$dta0\all_assess_VA.dta",clear

replace LegalTownship = strtrim(LegalTownship)
keep if County=="ALEXANDRIA CITY"|County=="ARLINGTON"|County=="FAIRFAX"|County=="FAIRFAX CITY"
tab FIPS
merge m:1 RowID using"$temp\current_assess_value_VA.dta"
drop if _merge==2
capture drop _merge
merge m:1 RowID using"$temp\current_assess_VA_building.dta"
drop if _merge==2
capture drop _merge
merge m:1 RowID using"$temp\current_assess_VA_buildingarea.dta",keepusing(SQFTBAG SQFTBAL SQFTBASE)
drop if _merge==2
capture drop _merge
merge m:1 RowID using"$temp\current_assess_VA_waterfront.dta", keepusing(Waterfront)
drop if _merge==2
capture drop _merge
replace Waterfront=0 if Waterfront==.
merge m:1 RowID using"$temp\current_assess_VA_pool.dta",keepusing(Pool)
drop if _merge==2
capture drop _merge
replace Pool=0 if Pool==.
merge m:1 RowID using"$temp\current_assess_VA_garage.dta",keepusing(GarageNoOfCars)
drop if _merge==2
capture drop _merge

/*
merge m:1 RowID using"$dta0\historic_assess_value_ct.dta",update
drop if _merge==2
drop _merge

merge m:1 RowID using"$dta0\historic_assess_ct_building.dta",update keepusing(NoOfUnits PropertyCountyLandUseDescription PropertyCountyLandUseCode BuildingOrImprovementNumber BuildingConditionStndCode YearBuilt NoOfStories TotalRooms TotalBedrooms TotalCalculatedBathCount HeatingTypeorSystemStndCode AirConditioningStndCode FireplaceNumber *Stnd*)
drop if _merge==2
drop _merge

count if TaxYear==.
count if AssessmentYear==.
replace TaxYear=AssessmentYear if TaxYear==.
drop if TaxYear==.


duplicates report RowID
merge m:1 RowID using"$dta0\historic_assess_ct_buildingarea.dta",update keepusing(SQFTBAG SQFTBAL SQFTBASE)
drop if _merge==2
drop _merge
merge m:1 RowID using"$dta0\historic_assess_ct_garage.dta",update keepusing(GarageStndCode GarageNoOfCars)
drop if _merge==2
drop _merge

*Stnds with very bad quality are dropped during processing above
global code "BuildingConditionStndCode RoofStructureTypeStndCode HeatingTypeorSystemStndCode AirConditioningStndCode FoundationTypeStndCode PoolStndCode GarageStndCode"

global vars "GarageNoOfCars Pool FireplaceNumber TotalBedrooms TotalRooms NoOfStories EffectiveYearBuilt YearBuilt BuildingOrImprovementNumber PropertyCountyLandUseCode NoOfUnits AssessmentYear TotalAssessedValue ImprovementAssessedValue LandAssessedValue "

duplicates drop ImportParcelID TaxYear $code $vars, force

replace PropertyCountyLandUseDescription = trim(PropertyCountyLandUseDescription)
keep if PropertyCountyLandUseDescription=="1-FAMILY RESIDENCE" | ///
		 PropertyCountyLandUseDescription =="SINGLE FAMILY RESIDENCE" | ///	
		 PropertyCountyLandUseDescription =="SINGLE FAMILY RESIDENTIAL" | ///
		 PropertyCountyLandUseDescription =="1-FAM RES" 
*Impute missing values from other periods if this variable is constant over time
foreach v of varlist $vars { 

	display " working on `v' now"
	egen mins = min(`v'), by(ImportParcelID)
	egen maxs = max(`v'), by(ImportParcelID)
	egen means = mean(`v'), by(ImportParcelID)
	
	gen e_`v'=`v'
	replace e_`v' = means if maxs==means & mins == means & `v'==.
	drop mins maxs means
}
foreach v of varlist $code { 

	display " working on `v' now"
	
	gen e_`v'=`v'
}


*Fill in missing years
gen e_Year = TaxYear
replace ImportParcelID=ha_ImportParcelID if ImportParcelID==.
sort ImportParcelID TaxYear
* mark missing year from the bottom
gen markForAdd = 1 if ImportParcelID==ImportParcelID[_n+1] & e_Year < e_Year[_n+1]-1  
mvencode markForAdd, mv(0) override
gen numToAdd =  e_Year[_n+1]-e_Year if ImportParcelID==ImportParcelID[_n+1] & markForAdd==1
expand numToAdd if markForAdd ==1
sort ImportParcelID TaxYear
*revise year generation process so we don't need to generate temp files
tab numToAdd
gen RN1=rnormal()
egen Rank1=rank(RN1),by(ImportParcelID TaxYear markForAdd numToAdd)
replace e_Year=e_Year+Rank1-1
sort ImportParcelID e_Year
capture drop RN1 Rank1

* populate consecutive years where data are missing
foreach yr of numlist 2017/1995{
foreach v of varlist $vars{
	display " working on `v' for `yr' now"
	replace e_`v' = e_`v'[_n-1] if ImportParcelID==ImportParcelID[_n-1] & e_`v'==. & TaxYear ==`yr'
	
}

** do the same with the Codes
foreach v of varlist $code {
	display " working on `v' for `yr' now"
	replace e_`v' = e_`v'[_n-1] if ImportParcelID==ImportParcelID[_n-1] & e_`v'=="" & TaxYear ==`yr'
	
}	
}
*then go backward 
foreach yr of numlist 2016/1994{
	foreach v of varlist $vars{
		display " working on `v' for `yr' now"
		replace e_`v' = e_`v'[_n+1] if ImportParcelID==ImportParcelID[_n+1] & e_`v'==. & TaxYear ==`yr'

	}

	** do the same with the Codes
	foreach v of varlist $code {
		display " working on `v' for `yr' now"
		replace e_`v' = e_`v'[_n+1] if ImportParcelID==ImportParcelID[_n+1] & e_`v'=="" & TaxYear ==`yr'

	}
	
}
drop if e_Year==2018
*/
 * fix gis - one and only time
gen LatFixed =PropertyAddressLatitude+0.00008
gen LongFixed=PropertyAddressLongitude+0.000428

save "$dta0\Allassess_withAttr_VA.dta",replace
*********************************************************************************************************
*               Limit Sample to Fairfox, Merge standard attributes from ZTRAX               *
*********************************************************************************************************

**************************************************
*  Pulling out property points for GIS analysis  *
**************************************************
use "$dta0\Allassess_withAttr_VA.dta",clear
set seed 1234567
tab NoOfUnits
count if NoOfUnits==.
gen ID=_n
sort ImportParcelID PropertyFullStreetAddress PropertyCity TaxYear ID
gen neg_Year=-(TaxYear)
sort ImportParcelID neg_Year ID

duplicates drop PropertyFullStreetAddress PropertyCity,force
duplicates drop ImportParcelID,force

tab TaxYear
drop neg_Year
tab NoOfUnits 
count if NoOfUnits==.
drop if NoOfUnits>=2&NoOfUnits!=.
count if NoOfUnits==1|(NoOfUnits==0)
tab LegalTownship 
tab LegalTownship if NoOfUnits==1|(NoOfUnits==0)

order NoOfUnits PropertyFullStreetAddress PropertyCity
gen connected_address_code=ustrpos(PropertyFullStreetAddress,"-")
order connected_address

tab NoOfUnits if connected_address>=1
*drop if the address is a connected address and does not have NoOfUnits as 1.
drop if connected_address>=1&NoOfUnits!=1
/*restriction 33 dropped*/
*drop if NoOfUnits>1
drop if NoOfUnits>1&NoOfUnits!=.
/*restriction 0 dropped*/
drop connected_add*
ren ID id
save "$dta0\propOneunit_VA.dta",replace

use "$dta0\propOneunit_VA.dta",clear
*Note the coordinate fix is done by shifting a certain vector (the hammer fix)
count if LatFixed==.
*704 no coordinates out of 380,067 (~2%)
export delimited using "$dta0\propOneunitVA.csv", replace
