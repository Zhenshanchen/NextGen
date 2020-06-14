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
global results "$root\results"
********************************************************
use "$dta0\all_assess_MD.dta",clear
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
use "$Zitrax\dta\current_assess_24.dta",clear
keep RowID ImportParcelID FIPS State County ExtractDate AssessorParcelNumber ///
UnformattedAssessorParcelNumber PropertyFullStreetAddress PropertyCity PropertyZip /// 
PropertyZoningDescription TaxIDNumber TaxAmount TaxYear NoOfBuildings LegalTownship ///
LotSizeAcres LotSizeSquareFeet PropertyAddressLatitude PropertyAddressLongitude BatchID

duplicates drop
save "$dta0\all_assess_MD.dta",replace

**************************************
*  End assessment data aggregation   *
**************************************

*****************************************************************************************************
* Begin standard attribute processing (various attributes from zillow hist or current assess)       *
*****************************************************************************************************
*Get current sales data
use "$Zitrax\dta\current_assess_sale_24.dta",clear
drop SalesPriceAmountStndCode SellerFullName BuyerFullName DocumentDate ///
 RecordingDocumentNumber
* in this file the RowID is not unique and shows two sales for some parcels 
save "$temp\current_assess_sale_MD.dta",replace

*current assess value data
use "$Zitrax\dta\current_assess_value_24.dta",clear
keep RowID ImprovementAssessedValue LandAssessedValue TotalAssessedValue AssessmentYear
save "$temp\current_assess_value_MD.dta",replace

use "$Zitrax\dta\current_assess_building_24.dta",clear
keep RowID NoOfUnits PropertyCountyLandUseDescription PropertyCountyLandUseCode BuildingOrImprovementNumber BuildingConditionStndCode ///
YearBuilt EffectiveYearBuilt NoOfStories TotalRooms TotalBedrooms TotalCalculatedBathCount HeatingTypeorSystemStndCode AirConditioningStndCode FireplaceNumber ///
RoofStructureTypeStndCode FoundationTypeStndCode FIPS
save "$temp\current_assess_MD_building.dta",replace

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
use "$Zitrax\dta\current_assess_buildingarea_24.dta",clear
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
save "$temp\current_assess_MD_buildingarea.dta",replace
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
use "$Zitrax\dta\current_assess_lotappeal_24.dta",clear
keep RowID LotSiteAppealStndCode FIPS BatchID
gen Waterfront=(LotSiteAppealStndCode=="WFS")
duplicates drop
duplicates report RowID
save "$temp\current_assess_MD_waterfront.dta",replace

use "$Zitrax\dta\current_assess_pool_24.dta",clear
keep RowID PoolStndCode FIPS BatchID
gen Pool=1
duplicates drop
drop PoolStndCode 
duplicates report RowID
duplicates drop
save "$temp\current_assess_MD_pool.dta",replace

use "$Zitrax\dta\current_assess_garage_24.dta",clear
keep RowID GarageNoOfCars GarageStndCode FIPS BatchID

duplicates drop
duplicates report RowID
duplicates drop RowID FIPS,force
drop GarageStndCode
save "$temp\current_assess_MD_garage.dta",replace
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
*  Limit Sample to Montgomery and Prince George, Merge standard attributes from ZTRAX  *
******************************************************************************************************
use "$dta0\all_assess_MD.dta",clear

replace LegalTownship = strtrim(LegalTownship)
keep if County=="MONTGOMERY"|County=="PRINCE GEORGES"
tab FIPS
merge m:1 RowID using"$temp\current_assess_value_MD.dta"
drop if _merge==2
capture drop _merge
merge m:1 RowID using"$temp\current_assess_MD_building.dta"
drop if _merge==2
capture drop _merge
merge m:1 RowID using"$temp\current_assess_MD_buildingarea.dta",keepusing(SQFTBAG SQFTBAL SQFTBASE)
drop if _merge==2
capture drop _merge
merge m:1 RowID using"$temp\current_assess_MD_waterfront.dta", keepusing(Waterfront)
drop if _merge==2
capture drop _merge
replace Waterfront=0 if Waterfront==.
merge m:1 RowID using"$temp\current_assess_MD_pool.dta",keepusing(Pool)
drop if _merge==2
capture drop _merge
replace Pool=0 if Pool==.
merge m:1 RowID using"$temp\current_assess_MD_garage.dta",keepusing(GarageNoOfCars)
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

save "$dta0\Curassess_withAttr_MD.dta",replace
*********************************************************************************************************
* Limit Sample to Montgomery and Prince George, Merge standard attributes from ZTRAX  *
*********************************************************************************************************

**************************************************
*  Pulling out property points for GIS analysis  *
**************************************************
use "$dta0\Curassess_withAttr_MD.dta",clear
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
save "$dta0\propOneunit_MD.dta",replace

use "$dta0\propOneunit_MD.dta",clear
*Note the coordinate fix is done by shifting a certain vector (the hammer fix)
count if LatFixed==.
*11,773 no coordinates out of 537,330 (~2%)
export delimited using "$dta0\propOneunitMD.csv", replace

********************************************
*   Fix coordinates with Parcel Polygons   *
********************************************
*points_polycent is from MD parcel polygon data - parcel centroids
set more off
use "$dta0\points_polycent.dta",clear
gen PropertyFullStreetAddress= premise_ad+" "+premise__2+" "+premise__3
replace PropertyFullStreetAddress=owners_add if premise_ad=="0"
gen PropertyCity=premise__4
replace PropertyCity=owners_cit if premise__4==""
gen State=owners_sta
gen PropertyZip=owners_zip
tab PropertyCity

keep PropertyFullStreetAddress PropertyCity PropertyZip lat_polyce long_polyc land_assmt improv_ass
ren lat_polyce Lat_polycent
ren long_polyc Long_polycent
ren land_assmt Land_AssV_poly
ren improv_ass Improv_Ass_poly

duplicates report PropertyFullStreetAddress PropertyCity
drop if PropertyFullStreetAddress==""|PropertyCity==""
duplicates drop
duplicates drop PropertyFullStreetAddress PropertyCity,force
replace PropertyCity="NORTH POTOMAC" if PropertyCity=="N POTOMAC"
set seed 1234567
sort PropertyFullStreetAddress PropertyCity
gen ID=_n
save "$dta0\points_cent_formatch.dta",replace

*Checking the ZTRAX points against the Parcel polygon (and centroids) from towns, we find most of the points fall in the right polygons in the urban areas.
*The points are almost perfect for treatment (on the flight path).

*****************************************
*           Data with Treatment         *
*****************************************
*The flight path is intersected with the ZTRAX points in ArcGIS
set more off
use "$dta0\ztrax_treat2mile.dta",clear
set seed 1234567

merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat1mile.dta"
gen Buffer_1mile=(_merge==3)
drop _merge
merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat.dta"
gen Treatment=(_merge==3)
drop _merge

gen Control_1mile=1 if Treatment==0&Buffer_1mile==1
replace Control_1mile=0 if Control_1mile==.
gen Control_2mile=1 if Treatment==0&Buffer_1mile==0
replace Control_2mile=0 if Control_2mile==.
tab Treat
tab Control_1mile
tab Control_2mile

ren propertyfu PropertyFullStreetAddress
ren propertyci PropertyCity
ren importparc ImportParcelID
ren latfixed LatFixed
ren longfixed LongFixed
ren fips FIPS
ren state State
ren county County
ren legaltowns LegalTownship
save "$dta0\ZTRAX_oneunit_withtreat.dta",replace

***********************************************
*           Processing Transaction            *
***********************************************
use "$dta0\ZTRAX_oneunit_withtreat.dta",clear
ren totalasses TotalAssessedValue
ren assessment AssessmentYear
keep ImportParcelID PropertyFullStreetAddress PropertyCity TotalAssessedValue AssessmentYear
sort *
*duplicates report PropertyFullStreetAddress PropertyCity e_Year
save "$dta0\ZTRAX_oneunitValue_withtreat.dta",replace


clear all
set more off
set max_memory 16g
set matsize 11000
use "$Zitrax\dta\transaction_24.dta",clear
set seed 1234567
merge 1:m TransId using"$Zitrax\dta\transaction_property_24.dta",keepusing(FIPS AssessorParcelNumber ImportParcelID PropertyFullStreetAddress PropertyCity)
drop if _merge!=3
*All matched with ImportParcelID ()
drop if FIPS!=24031

count if ImportParcelID==.
*drop obs for which we cannot get attributes and geographics
drop if ImportParcelID==.&AssessorParcelNumber==""&(trim(PropertyFullStreetAddress)=="0"|trim(PropertyFullStreetAddress)=="")
drop if (trim(PropertyFullStreetAddress)=="0"|trim(PropertyFullStreetAddress)=="")
/*restriction 1515+5327 dropped*/
*Later, try merge with address or AssessorParcelNumber if ImportParcelID is missing


*Deleting non-armslength here 
tab IntraFamilyTransferFlag
*Not populated at all
tab TransferTaxExemptFlag
*Not populated at all
tab PropertyUseStndCode
drop if PropertyUseStndCode=="CM" /*restriction commercial 56986 dropped*/
drop if PropertyUseStndCode=="IN" /*restriction industrial 1338 dropped*/
drop if PropertyUseStndCode=="EX" /*restriction exempt 1916 dropped*/


tab DataClassStndCode
tab DataClassStndCode, sum(SalesPriceAmount)

*keep only real transactions (D-trans without mortgage,H-deed with concurrent mortgage)
*Foreclosures are temporally kept to identify nonarmslength
keep if DataClassStndCode=="D"|DataClassStndCode=="H"|DataClassStndCode=="F"
/*restriction 561,140 dropped*/
tab DataClassStndCode, sum(LoanAmount)

*537,627 deed records without mortgage, 40,564 finance records (deed with cocurrent mortgage)
tab DocumentTypeStndCode
/*Major categories might not be full market value:   DELU (in lieu of foreclosure documents), 
       EXDE (executor's deed), 
       FCDE (foreclosure deed), 
       FDDE (fiduciary deed),
       QCDE (quitclaim deed),
	   TXDE (tax deed),
	   TRFC (foreclosure sale transfer).
*/
gen pFCs = (DocumentTypeStndCode=="TRFC" | DocumentTypeStndCode=="NTSL")
drop if DocumentTypeStndCode=="DELU"|DocumentTypeStndCode=="EXDE"|DocumentTypeStndCode=="FCDE"|DocumentTypeStndCode=="FDDE"|DocumentTypeStndCode=="QCDE"|DocumentTypeStndCode=="TXDE"|DocumentTypeStndCode=="TRFC"|DocumentTypeStndCode=="NTSL"
/*restriction 28,973 deed records dropped*/

* foreclosure deeds transactions and substitute deeds 
* sometimes DocumentTypeStndCode=="OTHR" are FC sales like TransId 187733346
/*ZC: TRFC catches too few - gen pFCs = (DocumentTypeStndCode=="TRFC" | DocumentTypeStndCode=="NTSL")
*/
replace pFCs = 1 if (DataClassStndCode=="F"& RecordingBookNumber!="" )
tab pFCs

gen n=1
egen countall=sum(n), by(ImportParcelID PropertyFullStreetAddress PropertyCity)
egen _countFs =sum(n) if pFCs==1, by(ImportParcelID PropertyFullStreetAddress PropertyCity)
egen countFs =mean(_countFs) , by(ImportParcelID PropertyFullStreetAddress PropertyCity)

gen possibleFC = ( countFs<countall & countFs >0 & countFs!=.)

gen recYear = substr(RecordingDate, 1,4)  /* for merge later on*/
gen recMonth = substr(RecordingDate, 6,2)  /* for merge later on*/
gen recDay = substr(RecordingDate, 9,2)  /* for merge later on*/
destring(recYear), replace
destring(recMonth), replace
destring(recDay), replace
gen dayOfRec = mdy(recMonth,recDay,recYear)
sort ImportParcelID PropertyFullStreetAddress PropertyCity dayOfRec TransId

* running total of observations
gen Ftotal = 0 
egen minIPI = min(_n), by(ImportParcelID PropertyFullStreetAddress PropertyCity dayOfRec)
replace Ftotal = 1 if minIPI ==_n &	 pFCs==1
replace Ftotal = Ftotal[_n-1]+1 if minIPI <_n &  pFCs==1

* running transactions total
gen Ttotal = 0 
replace Ttotal = 1 if minIPI ==_n 
replace Ttotal = Ttotal[_n-1]+1 if minIPI <_n 

*** mark the first transaction after a substitute deed
gen _firstPostPossibleFC = (Ftotal==1 & Ftotal[_n+1]-1!=Ftotal[_n] ) | /// 
						  (Ftotal>1 & Ftotal[_n-1]+1==Ftotal[_n] & ///	 
						   Ftotal[_n+1]-1!=Ftotal[_n])
						   
*** check the years difference as well if it is really long maybe don't mark it
gen _TransFC = Ttotal if _firstPostPossibleFC==1
gen TransFC = 1 if _TransFC==. & _TransFC[_n-1]>0 & _TransFC[_n-1]!=. 

gen TransFC_daysSince = dayOfRec[_n] - dayOfRec[_n-1] if _TransFC==. & _TransFC[_n-1]>0 & _TransFC[_n-1]!=. 
gen TransFC_1y = TransFC if TransFC_daysSince<=365
gen TransFC_2y = TransFC if TransFC_daysSince<=730 & TransFC_daysSince>365
gen TransFC_gt2y = TransFC  if TransFC_daysSinc>730

rename TransFC_1y nonARMS_sat1
rename TransFC_2y nonARMS_sat2
rename TransFC_gt2y nonARMS_sat3
mvencode nonARMS_*, mv(0) override

label variable nonARMS_sat1 "Likely non Arms sale due to first sale (in 1yr) after default and appt of substitute trustee"
label variable nonARMS_sat2 "Likely non Arms sale due to first sale (in 2yr) after default and appt of substitute trustee"
label variable nonARMS_sat3 "Likely non Arms sale due to first sale (in gt2yr) after default and appt of substitute trustee"

tab nonARMS_sat1
tab nonARMS_sat2
tab nonARMS_sat3 

*Drop foreclosures 
drop if DataClassStndCode=="F"
/*restriction  4,812 dropped*/

*Drop possible foreclosure sales (within in 1yr or 2yrs after default)
drop if nonARMS_sat1==1
/*restriction  2,341 dropped*/
drop if nonARMS_sat2==1
/*restriction  1,087 dropped*/

gen withloan=(LoanAmount>0&LoanAmount!=.)
keep TransId AssessorParcelNumber ImportParcelID PropertyFullStreetAddress PropertyCity SalesPriceAmount SalesPriceAmountStndCode LoanAmount LoanRateTypeStndCode /// 
LoanDueDate DataClassStndCode DocumentTypeStndCode IntraFamilyTransferFlag LoanTypeStndCode /// 
PropertyUseStndCode RecordingDate withloan LenderName LenderTypeStndCode LenderIDStndCode

duplicates tag TransId,gen(dup1)
foreach v in ImportParcelID{
drop if dup1==1&`v'==.
}
/*restriction 349 dropped*/

foreach v in AssessorParcelNumber PropertyFullStreetAddress PropertyCity{
drop if dup1==1&trim(`v')==""
}
duplicates report TransId
gen N=_n
sort TransId N 
duplicates drop TransId,force
/*restriction 8,605 dropped*/
drop N
save "$dta0\sales_nonarmsprocessed_dropforeclosure.dta",replace

use "$dta0\sales_nonarmsprocessed_dropforeclosure.dta",clear
merge 1:m TransId using"$Zitrax\dta\transaction_buyer_24.dta",keepusing(BuyerLastName BuyerIndividualFullName BuyerNonIndividualName)
drop if _merge==2
drop _merge
set seed 123445565
sort *
duplicates drop TransId BuyerLastName BuyerIndividualFullName,force
duplicates report TransId
duplicates tag TransId,gen(dup_buyer)
sort *
egen rank1=rank(_n),by(TransId)
drop if rank1>2
drop rank1 dup_buyer

*SellerLastName SellerIndividualFullName SellerNonIndividualName
joinby TransId using"$Zitrax\dta\transaction_seller_24.dta",unmatched(master)
drop v7 v8 v9 v10 
drop SellerFirstMiddleName SellerNameSequenceNumber
drop _merge
duplicates drop
duplicates report TransId
duplicates tag TransId, gen(dup_seller)
drop if dup_seller>=5
drop dup_seller
sort *
egen TransId_rank=rank(_n),by(TransId)

*drop if " "
reshape wide BuyerLastName BuyerIndividualFullName BuyerNonIndividualName SellerLastName SellerIndividualFullName SellerNonIndividualName, i(TransId) j(TransId_rank)
duplicates report TransId

ren SalesPriceAmount SalesPrice
*identify multiple parcel sale
*Same buyer name, and record date
/*
duplicates report RecordingDate SalesPrice DataClassStndCode ///
BuyerLastName1 BuyerIndividualFullName1 BuyerNonIndividualName1 ///
BuyerLastName2 BuyerIndividualFullName2 BuyerNonIndividualName2
*Add seller name 
duplicates report RecordingDate SalesPrice DataClassStndCode /// 
BuyerLastName1 BuyerIndividualFullName1 BuyerNonIndividualName1 ///
BuyerLastName2 BuyerIndividualFullName2 BuyerNonIndividualName2 ///
SellerLastName1 SellerIndividualFullName1 SellerNonIndividualName1 ///
SellerLastName2 SellerIndividualFullName2 SellerNonIndividualName2
*/

duplicates drop PropertyFullStreetAddress RecordingDate SalesPrice DataClassStndCode /// 
BuyerLastName1 BuyerIndividualFullName1 BuyerNonIndividualName1 ///
BuyerLastName2 BuyerIndividualFullName2 BuyerNonIndividualName2 ///
SellerLastName1 SellerIndividualFullName1 SellerNonIndividualName1 ///
SellerLastName2 SellerIndividualFullName2 SellerNonIndividualName2,force

duplicates tag RecordingDate SalesPrice DataClassStndCode /// 
BuyerLastName1 BuyerIndividualFullName1 BuyerNonIndividualName1 ///
BuyerLastName2 BuyerIndividualFullName2 BuyerNonIndividualName2 ///
SellerLastName1 SellerIndividualFullName1 SellerNonIndividualName1 ///
SellerLastName2 SellerIndividualFullName2 SellerNonIndividualName2, gen(Mul_sale)

sort RecordingDate SalesPrice DataClassStndCode /// 
BuyerLastName1 BuyerIndividualFullName1 BuyerNonIndividualName1 ///
BuyerLastName2 BuyerIndividualFullName2 BuyerNonIndividualName2 ///
SellerLastName1 SellerIndividualFullName1 SellerNonIndividualName1 ///
SellerLastName2 SellerIndividualFullName2 SellerNonIndividualName2
*drop all identified multiple sales
browse if Mul_sale==1
drop if Mul_sale==1
/*restriction 2988 dropped */
drop Mul_sale
save "$dta0\sales_nonarmsprocessed_dropmultiplesale.dta",replace

*Generate indicator showing buyer seller having the same last name
gen BS_SameLast=1 if BuyerLastName1==SellerLastName1|BuyerLastName1==SellerLastName2|BuyerLastName2==SellerLastName1|BuyerLastName2==SellerLastName2
replace BS_SameLast=. if BuyerLastName1==""&SellerLastName1==""|BuyerLastName1==""&SellerLastName2==""|BuyerLastName2==""&SellerLastName1==""|BuyerLastName2==""&SellerLastName2==""
tab BS_SameLast
ren BS_SameLast BS_relation

*Generate indicator showing buyer and lender are the same group
gen LB_Same=1 if LenderName==BuyerNonIndividualName1&BuyerNonIndividualName1!=""|LenderName==BuyerNonIndividualName2&BuyerNonIndividualName2!=""
ren LB_Same LB_relation
duplicates drop TransId SalesPrice,force
/*restriction 0 dropped*/

tab LB_relation
tab BS_relation

* now let's loop over common nonARMS terms in buyer and seller
gen nonARMS_termmark=.
foreach v of varlist BuyerLastName1 BuyerIndividualFullName1 BuyerNonIndividualName1 BuyerLastName2 BuyerIndividualFullName2 BuyerNonIndividualName2 SellerLastName1 SellerIndividualFullName1 SellerNonIndividualName1 SellerLastName2 SellerIndividualFullName2 SellerNonIndividualName2{

	replace nonARMS_termmark = 1 if strpos(`v', "SEC OF HOUSING & URBAN")>0 /* s bad*/
	replace nonARMS_termmark = 1 if strpos(`v', "BANK OF AMERICA")>0 /* s bad*/
	replace nonARMS_termmark = 1 if strpos(`v', "ET AL")>0  /*b - grabs alot of legit sales*/
	replace nonARMS_termmark = 1  if strpos(`v', "VETERANS AFFAIRS")>0
	replace nonARMS_termmark = 1  if strpos(`v', "SECRETARY")>0
	replace nonARMS_termmark = 1 if strpos(`v', "SEC OF")>0
	replace nonARMS_termmark = 1 if strpos(`v', "HOUSING")>0
	replace nonARMS_termmark = 1 if strpos(`v', "NATIONAL")>0
	replace nonARMS_termmark = 1 if strpos(`v', "FEDERAL")>0
	replace nonARMS_termmark = 1 if strpos(`v', "MORTGAGE")>0
	replace nonARMS_termmark = 1 if strpos(`v', "LOAN")>0
	replace nonARMS_termmark = 1 if strpos(`v', "CAPITAL")>0 
	replace nonARMS_termmark = 1 if strpos(`v', "CAPITOL")>0
	replace nonARMS_termmark = 1 if strpos(`v', "FINANCE")>0 
	*gen nonARMS`f'_15 = 1 if strpos(`v', "UNITED")>0 /*b - almost all legit sales*/
	*gen nonARMS`f'_16 = 1 if strpos(`v', " INC")>0 /*b - grabs alot of legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', " LLC")>0 /*b - almost all legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', " CORP")>0 /*b - grabs alot of legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', " CORPORATION")>0 /*b - grabs alot of legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', " ASSOCIATION")>0
	replace nonARMS_termmark = 1 if strpos(`v', " COMPANY")>0
	*gen nonARMS`f'_23 = 1 if strpos(`v', "DEVELOPMENT")>0 /*b - grabs alot of legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', " F S B")>0
	replace nonARMS_termmark = 1 if strpos(`v', "REGIONAL OFFICE DIRECTOR")>0
	replace nonARMS_termmark = 1 if strpos(`v', " USA")>0
	replace nonARMS_termmark = 1 if strpos(`v', "HSBC")>0
	replace nonARMS_termmark = 1 if strpos(`v', "GUARANTY")>0
	*gen nonARMS`f'_29 = 1 if strpos(`v', "UNITED")>0 /*b - almost all legit sales*/
	*gen nonARMS`f'_30 = 1 if strpos(`v', "TRUST")>0 /*b - grabs alot of legit sales*/
	*gen nonARMS`f'_31 = 1 if strpos(`v', "TRUSTEE")>0  /*b - grabs alot of legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', "BANK OF")>0
	replace nonARMS_termmark = 1 if strpos(`v', "ST OF")>0
	replace nonARMS_termmark = 1 if strpos(`v', "STATE OF")>0
	replace nonARMS_termmark = 1 if strpos(`v', "MUTUAL")>0
	*gen nonARMS`f'_36 = 1 if strpos(`v', "C/O")>0 /*b - almost all legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', "(TR)")>0
	*gen nonARMS`f'_38 = 1 if strpos(`v', "LTD")>0 /*b - almost all legit sales*/
	replace nonARMS_termmark = 1 if strpos(`v', "CHEVY CHASE BANK")>0
	replace nonARMS_termmark = 1 if strpos(`v', " BANK")>0

}
mvencode nonARMS_termmark, mv(0) override


*Getting historical ass values
joinby ImportParcelID using"$dta0\ZTRAX_oneunitValue_withtreat.dta",unmatched(master)
gen matched_PID=(_merge==3)
drop _merge
sort *

drop if matched_PID==0
drop matched_PID
/*restriction 371,452 dropped-dropping those will be not matched with property data (outside the study region - 2 miles from the flight path)*/


gen TransactionYear=substr(RecordingDate,1,4)
destring TransactionYear,replace
foreach n of numlist 0/12{
	gen _ratio`n' = SalesPrice/TotalAssessedValue if TransactionYear-6+`n'== AssessmentYear 
	egen ratio`n' = mean(_ratio`n'), by(TransId)
}
*_ratio0-_ratio8 meaning the price ratio over AV 6 years before to 6 years after

* ratio clean up 
*Revise the price ratio based on the whole assessment value history to avoid major measurement error
gen ratioToUse = 99
gen ratioDist = 99

foreach n of numlist 0/12{
	replace ratioToUse = ratio`n' if (ratioToUse==99 | ratioToUse <.3 | ratioToUse >2)&ratio`n'!=. 
	replace ratioDist = `n' if ratioToUse==ratio`n' 
}

tab TransactionYear
ren TransactionYear e_Year
*Transfer Prices in 2017 dollar
*Inflation Rate is based on Bureau of Labor Statistics CPI
*The first sale is in 1994
replace SalesPrice=SalesPrice*1.80 if e_Year==1991
replace SalesPrice=SalesPrice*1.74 if e_Year==1992
replace SalesPrice=SalesPrice*1.69 if e_Year==1993
replace SalesPrice=SalesPrice*1.65 if e_Year==1994
replace SalesPrice=SalesPrice*1.61 if e_Year==1995
replace SalesPrice=SalesPrice*1.56 if e_Year==1996
replace SalesPrice=SalesPrice*1.53 if e_Year==1997
replace SalesPrice=SalesPrice*1.50 if e_Year==1998
replace SalesPrice=SalesPrice*1.47 if e_Year==1999
replace SalesPrice=SalesPrice*1.42 if e_Year==2000
replace SalesPrice=SalesPrice*1.38 if e_Year==2001
replace SalesPrice=SalesPrice*1.36 if e_Year==2002
replace SalesPrice=SalesPrice*1.33 if e_Year==2003
replace SalesPrice=SalesPrice*1.30 if e_Year==2004
replace SalesPrice=SalesPrice*1.25 if e_Year==2005
replace SalesPrice=SalesPrice*1.21 if e_Year==2006
replace SalesPrice=SalesPrice*1.18 if e_Year==2007
replace SalesPrice=SalesPrice*1.14 if e_Year==2008
replace SalesPrice=SalesPrice*1.14 if e_Year==2009
replace SalesPrice=SalesPrice*1.12 if e_Year==2010
replace SalesPrice=SalesPrice*1.09 if e_Year==2011
replace SalesPrice=SalesPrice*1.07 if e_Year==2012
replace SalesPrice=SalesPrice*1.05 if e_Year==2013
replace SalesPrice=SalesPrice*1.03 if e_Year==2014
replace SalesPrice=SalesPrice*1.03 if e_Year==2015
replace SalesPrice=SalesPrice*1.02 if e_Year==2016

replace ratioToUse = SalesPrice/TotalAssessedValue if ratioToUse==99

drop _ratio* ratio1-ratio12
duplicates drop
duplicates report TransId

*** use the price ratio and suspect obs to mark nonARMS (burned in with actual MD data)
*drop transactions with buyer seller or buyer lender relations
count if BS_relation==1
count if BS_relation==1&(ratioToUse<.8 | ratioToUse>1.2 | ratioToUse==99)

drop if BS_relation==1&(ratioToUse<.8 | ratioToUse>1.2 | ratioToUse==99)
/*restriction 6202 dropped*/
drop if LB_relation==1&(ratioToUse<.8 | ratioToUse>1.2 | ratioToUse==99)
/*restriction 0 dropped*/

count if ratioToUse<.8
count if ratioToUse==99
*drop transactions with participants with nonARMS terms 
drop if nonARMS_termmark==1&(ratioToUse<.8 | ratioToUse>1.2 | ratioToUse==99)
/*restriction 26,987 dropped*/
save "$dta0\sales_nonarmsprocessed_dropbuysellrelation.dta",replace


use "$dta0\sales_nonarmsprocessed_dropbuysellrelation.dta",clear
*drop transactions with prices that are too low
drop if SalesPrice<1000
/*restriction 17,885 dropped*/

*drop price outlier
sum SalesPrice,detail
*p1=35970  p99=3835000
drop if SalesPrice<=r(p1)|SalesPrice>=r(p99)
/*restriction 1,414 dropped */

*drop Price ratio outlier
sum ratioToUse if ratioToUse!=99,d
drop if (ratioToUse<r(p1)|ratioToUse>r(p99))&ratioToUse!=99
/*restriction 1,386 dropped- those ratiotouse==99 will not present in the final dataset, because of the lack of corresponding assessment info*/
drop ratioToUse

count if e_Year>=2016
count if e_Year>=2005
count if e_Year>=1994

tab SalesPriceAmountStndCode
keep if SalesPriceAmountStndCode=="RD"|SalesPriceAmountStndCode=="CF"|SalesPriceAmountStndCode==""  /*RD-documented, CF-fullconsideration,backed up from sales tax*/
/*restriction 72 dropped*/
tab e_Year if ImportParcelID==.
save "$dta0\sales_nonarmsprocessed.dta",replace

***********************************************
*       Merge Assess with Transaction         *
***********************************************
use "$dta0\sales_nonarmsprocessed.dta",clear
merge m:1 ImportParcelID using "$dta0\ZTRAX_oneunit_withtreat.dta"
drop if _merge==2
*37126 out of 80943 properties find no transaction
drop _merge
tab Treatment 
tab Control_1mile 
tab Control_2mile

*Timing of next gen switch - 2015 summer throughout the year
gen SalesYear=substr(RecordingDate,1,4)
gen SalesMonth=substr(RecordingDate,6,2)
gen SalesDay=substr(RecordingDate,9,2)
destring SalesYear,replace
destring SalesMonth,replace
destring SalesDay,replace

gen post=(SalesYear>2015)
replace post=1 if SalesYear==2015&SalesMonth>=9
tab post Treatment
tab post Treatment if SalesYear>=2000

gen Ln_Price=ln(SalesPrice)
egen PID=group(PropertyFullStreetAddress PropertyCity)
*All data
eststo DID_linear:reg SalesPrice i.Treatment##i.post i.SalesMonth i.SalesYear, cluster(PID) 
eststo DID_ln:reg Ln_Price i.Treatment##i.post i.SalesMonth i.SalesYear, cluster(PID) 
esttab DID_linear DID_ln using"$results\DID_results.csv", keep(1.Treatment 1.post 1.Treatment#1.post) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
esttab DID_linear DID_ln using"$results\DID_results.html", keep(1.Treatment 1.post 1.Treatment#1.post) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))

*Only with sales after 2000
eststo DID_linear_post2000:reg SalesPrice i.Treatment##i.post i.SalesMonth i.SalesYear if SalesYear>=2000, cluster(PID) 
eststo DID_ln_post2000:reg Ln_Price i.Treatment##i.post i.SalesMonth i.SalesYear if SalesYear>=2000, cluster(PID) 

