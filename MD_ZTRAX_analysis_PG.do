**************************************************************************
*   Do file- Process of sales, and analysis of PRINCE GEORGES County     *
**************************************************************************
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


**************************************************
*           Data with Treatment-PG               *
**************************************************
*The flight path is intersected with the ZTRAX points in ArcGIS
set more off
use "$dta0\ztrax_treat6km_pg.dta",clear
set seed 1234567

merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat5km_pg.dta"
gen Buffer_5km=(_merge==3)
drop _merge
merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat4km_pg.dta"
gen Buffer_4km=(_merge==3)
drop _merge
merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat3km_pg.dta"
gen Buffer_3km=(_merge==3)
drop _merge
merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat2km_pg.dta"
gen Buffer_2km=(_merge==3)
drop _merge
merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat1km_pg.dta"
gen Buffer_1km=(_merge==3)
drop _merge
merge 1:1 propertyfu propertyci importparc using"$dta0\ztrax_treat_pg.dta"
gen Treatment=(_merge==3)
drop _merge

gen Control_6km=1 if Treatment==0
replace Control_6km=0 if Control_6km==.
gen Control_5km=1 if Treatment==0&Buffer_5km==1
replace Control_5km=0 if Control_5km==.
gen Control_4km=1 if Treatment==0&Buffer_4km==1
replace Control_4km=0 if Control_4km==.
gen Control_3km=1 if Treatment==0&Buffer_3km==1
replace Control_3km=0 if Control_3km==.
gen Control_2km=1 if Treatment==0&Buffer_2km==1
replace Control_2km=0 if Control_2km==.
gen Control_1km=1 if Treatment==0&Buffer_1km==1
replace Control_1km=0 if Control_1km==.

tab Treatment
*The following line drop properties within 2km-3km
*drop if Control_3km==1&Control_2km==0

ren propertyfu PropertyFullStreetAddress
ren propertyci PropertyCity
ren importparc ImportParcelID
ren latfixed LatFixed
ren longfixed LongFixed
ren fips FIPS
ren state State
ren county County
ren legaltowns LegalTownship
save "$dta0\ZTRAX_oneunit_withtreat_PG.dta",replace

***********************************************
*           Processing Transaction            *
***********************************************
use "$dta0\ZTRAX_oneunit_withtreat_PG.dta",clear
ren totalasses TotalAssessedValue
ren assessment AssessmentYear
keep ImportParcelID PropertyFullStreetAddress PropertyCity TotalAssessedValue AssessmentYear
sort *
*duplicates report PropertyFullStreetAddress PropertyCity e_Year
save "$dta0\ZTRAX_oneunitValue_withtreat_PG.dta",replace


clear all
set more off
set max_memory 16g
set matsize 11000
use "$Zitrax\dta\transaction_24.dta",clear
set seed 1234567
drop if FIPS!=24033
merge 1:m TransId using"$Zitrax\dta\transaction_property_24.dta",keepusing(FIPS AssessorParcelNumber ImportParcelID PropertyFullStreetAddress PropertyCity)
drop if _merge!=3
*All matched with ImportParcelID ()


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
joinby ImportParcelID using"$dta0\ZTRAX_oneunitValue_withtreat_PG.dta",unmatched(master)
gen matched_PID=(_merge==3)
drop _merge
sort *

drop if matched_PID==0
drop matched_PID
/*restriction 294,460 dropped-dropping those will be not matched with property data (outside the study region - 2 miles from the flight path)*/


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
/*restriction 10,048 dropped*/
drop if LB_relation==1&(ratioToUse<.8 | ratioToUse>1.2 | ratioToUse==99)
/*restriction 0 dropped*/

count if ratioToUse<.8
count if ratioToUse==99
*drop transactions with participants with nonARMS terms 
drop if nonARMS_termmark==1&(ratioToUse<.8 | ratioToUse>1.2 | ratioToUse==99)
/*restriction 42,993 dropped*/
*drop transactions with prices that are too low
drop if SalesPrice<1000
/*restriction 29,049 dropped*/

*drop price outlier
sum SalesPrice,detail
*p1=  p99=
drop if SalesPrice<=r(p1)|SalesPrice>=r(p99)
/*restriction 2,334 dropped */

*drop Price ratio outlier
sum ratioToUse if ratioToUse!=99,d
drop if (ratioToUse<r(p1)|ratioToUse>r(p99))&ratioToUse!=99
/*restriction 2,289 dropped- those ratiotouse==99 will not present in the final dataset, because of the lack of corresponding assessment info*/
drop ratioToUse

count if e_Year>=2016
count if e_Year>=2005
count if e_Year>=1994

tab SalesPriceAmountStndCode
keep if SalesPriceAmountStndCode=="RD"|SalesPriceAmountStndCode=="CF"|SalesPriceAmountStndCode==""  /*RD-documented, CF-fullconsideration,backed up from sales tax*/
/*restriction 76 dropped*/
tab e_Year if ImportParcelID==.
save "$dta0\sales_nonarmsprocessed_PG.dta",replace

**********************************************************
*       Merge Assess with Transaction & Analysis         *
**********************************************************
/*one-time rename dta file transformed from ArcGIS
use "$dta0\ztrax_lowelevsec_pg.dta",clear
ren propertyfu PropertyFullStreetAddress
ren propertyci PropertyCity
ren importparc ImportParcelID
ren latfixed LatFixed
ren longfixed LongFixed
ren fips FIPS
ren state State
ren county County
ren legaltowns LegalTownship
save "$dta0\ztrax_lowelevsec_pg.dta",replace
*/
clear all
set more off
set max_memory 16g
set matsize 11000
use "$dta0\sales_nonarmsprocessed_PG.dta",clear
merge m:1 ImportParcelID using "$dta0\ZTRAX_oneunit_withtreat_PG.dta"
drop if _merge==2
* out of  properties find no transaction
drop _merge

*Merge with school zones

tab Treatment 
tab Control_6km
tab Control_5km
tab Control_4km
tab Control_3km
tab Control_2km

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
gen post_treat=post*Treatment
tab post_treat

gen Ln_Price=ln(SalesPrice)
egen PID=group(PropertyFullStreetAddress PropertyCity)

*set study period
drop if SalesYear<=1993
gen period=12*(SalesYear-1994)+SalesMonth
duplicates report PID

*using city in the place of school zone
egen City=group(PropertyCity)
*drop possible house flipping events - sales within a month
duplicates tag PID SalesYear SalesMonth, gen(flipping)
drop if flipping>=1
drop flipping
duplicates report PID SalesYear SalesMonth SalesDay
xtset PID period
*Check if the treatment effect extends to 1km buffer
gen Treatment1km=(Control_1km==1|Treatment==1)
*Check if the treatment effect extends to 2km buffer
gen Treatment2km=(Control_2km==1|Treatment==1)
*Check if the treatment effect extends to 3km buffer
gen Treatment3km=(Control_3km==1|Treatment==1)

gen Ring_5km=(Control_5km==1&Control_4km==0)
gen Ring_4km=(Control_4km==1&Control_3km==0)
gen Ring_3km=(Control_3km==1&Control_2km==0)
gen Ring_2km=(Control_2km==1&Control_1km==0)
gen Ring_1km=(Control_1km==1)
foreach n in 5 4 3 2 1 {
	gen Ring_`n'kmpost=Ring_`n'km*post
}
foreach n in 3 2 1 {
gen post_treat`n'km=Treatment`n'km*post
}

drop if SalesYear<2010
*drop 2015, because: 1. the treatment take place steadily over the year, 2. the market takes time to reach an equilibrium after the change
drop if SalesYear==2015
*Only with sales in or after 2010 - treatment: in the next gen flight path, control within 3km 
eststo DID_linear:reg SalesPrice i.Treatment##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln:reg Ln_Price i.Treatment##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_linear1km:reg SalesPrice i.Treatment1km##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln1km:reg Ln_Price i.Treatment1km##i.post  i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_linear2km:reg SalesPrice i.Treatment2km##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln2km:reg Ln_Price i.Treatment2km##i.post  i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_linear3km:reg SalesPrice i.Treatment3km##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln3km:reg Ln_Price i.Treatment3km##i.post  i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 

esttab DID_linear DID_ln DID_linear1km DID_ln1km DID_linear2km DID_ln2km DID_linear3km DID_ln3km using"$results\DID_results_PG.csv", keep(1.Treatment 1.post 1.Treatment#1.post 1.Treatment1km 1.Treatment1km#1.post 1.Treatment2km 1.Treatment2km#1.post 1.Treatment3km 1.Treatment3km#1.post) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
esttab DID_linear DID_ln DID_linear1km DID_ln1km DID_linear2km DID_ln2km DID_linear3km DID_ln3km using"$results\DID_results_PG.html", keep(1.Treatment 1.post 1.Treatment#1.post 1.Treatment1km 1.Treatment1km#1.post 1.Treatment2km 1.Treatment2km#1.post 1.Treatment3km 1.Treatment3km#1.post) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
 
*Only with sales after 2010 - with buffer specification
eststo Xt_linear_consist:xtreg SalesPrice post_treat Ring_1kmpost Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_ln_consist:xtreg Ln_Price post_treat Ring_1kmpost Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_linear1km:xtreg SalesPrice post_treat1km Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_ln1km:xtreg Ln_Price post_treat1km Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_linear2km:xtreg SalesPrice post_treat2km Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_ln2km:xtreg Ln_Price post_treat2km Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 

esttab Xt_linear_consist Xt_ln_consist Xt_linear1km Xt_ln1km Xt_linear2km Xt_ln2km using"$results\Xt_results_PG.csv", keep(post_treat Ring_*kmpost post_treat*km) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
esttab Xt_linear_consist Xt_ln_consist Xt_linear1km Xt_ln1km Xt_linear2km Xt_ln2km using"$results\Xt_results_PG.html", keep(post_treat Ring_*kmpost post_treat*km) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))

tab post_treat
duplicates tag PID if Treatment==1,gen(dup_sale_treat)
count if post_treat==1&dup_sale_treat>=1
*for xt regs, 6 treated have duplicated sales that are utilized in the identification for post_treat
*Comparison: DID utilizes  

foreach n in 1 2 3 4 5 {
tab Ring_`n'kmpost
duplicates tag PID if Ring_`n'kmpost==1,gen(dup_Ring_`n'kmpost)
count if Ring_`n'kmpost==1&dup_Ring_`n'kmpost>=1
}
drop dup_sale_treat
drop dup_Ring_*
*For xt regressions:
*22 properties have duplicated sales that are utilized in the identification for 1km buffer
*35 for 2km buffer
*30 for 3km buffer
*50 for 4km buffer
*26 for 5km buffer

*drop if Control_3km==1&Control_2km==0
*Only pick these influenced or parallel to the low elevation flight path (below 10k ft)
merge m:1 ImportParcelID using "$dta0\ztrax_lowelevsec_pg.dta"
drop if _merge!=3
*Estimation with only low-elevation-section
*treatment: in the next gen flight path, control within 3km 
eststo DID_linear_lowelev:reg SalesPrice i.Treatment##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln_lowelev:reg Ln_Price i.Treatment##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_linear1km_lowelev:reg SalesPrice i.Treatment1km##i.post  i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln1km_lowelev:reg Ln_Price i.Treatment1km##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_linear2km_lowelev:reg SalesPrice i.Treatment2km##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln2km_lowelev:reg Ln_Price i.Treatment2km##i.post  i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_linear3km_lowelev:reg SalesPrice i.Treatment3km##i.post i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 
eststo DID_ln3km_lowelev:reg Ln_Price i.Treatment3km##i.post  i.period i.SalesYear#i.City if Treatment==1|Ring_1km==1|Ring_2km==1|Ring_3km==1|Ring_4km==1|Ring_5km==1, cluster(PID) 

esttab DID_linear_lowelev DID_ln_lowelev DID_linear1km_lowelev DID_ln1km_lowelev DID_linear2km_lowelev DID_ln2km_lowelev DID_linear3km_lowelev DID_ln3km_lowelev using"$results\DID_results_midelev_pg.csv", keep(1.Treatment 1.post 1.Treatment#1.post 1.Treatment1km 1.Treatment1km#1.post 1.Treatment2km 1.Treatment2km#1.post 1.Treatment3km 1.Treatment3km#1.post) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
esttab DID_linear_lowelev DID_ln_lowelev DID_linear1km_lowelev DID_ln1km_lowelev DID_linear2km_lowelev DID_ln2km_lowelev DID_linear3km_lowelev DID_ln3km_lowelev using"$results\DID_results_midelev_pg.html", keep(1.Treatment 1.post 1.Treatment#1.post 1.Treatment1km 1.Treatment1km#1.post 1.Treatment2km 1.Treatment2km#1.post 1.Treatment3km 1.Treatment3km#1.post) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
 
*Buffer
eststo Xt_linear_consist_lowelev:xtreg SalesPrice post_treat Ring_1kmpost Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_ln_consist_lowelev:xtreg Ln_Price post_treat Ring_1kmpost Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_linear1km_lowelev:xtreg SalesPrice post_treat1km Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_ln1km_lowelev:xtreg Ln_Price post_treat1km Ring_2kmpost Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_linear2km_lowelev:xtreg SalesPrice post_treat2km Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 
eststo Xt_ln2km_lowelev:xtreg Ln_Price post_treat2km Ring_3kmpost Ring_4kmpost Ring_5kmpost i.period i.SalesYear#i.City,fe cluster(PID) 

esttab Xt_linear_consist_lowelev Xt_ln_consist_lowelev Xt_linear1km_lowelev Xt_ln1km_lowelev Xt_linear2km_lowelev Xt_ln2km_lowelev using"$results\Xt_results_midelev_pg.csv", keep(post_treat Ring_*kmpost post_treat*km) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))
esttab Xt_linear_consist_lowelev Xt_ln_consist_lowelev Xt_linear1km_lowelev Xt_ln1km_lowelev Xt_linear2km_lowelev Xt_ln2km_lowelev using"$results\Xt_results_midelev_pg.html", keep(post_treat Ring_*kmpost post_treat*km) replace b(a3) se r2(3) star(+ .1 * .05 ** .01 *** .001) stats (N N_g r2, fmt(0 3))

tab post_treat
duplicates tag PID if Treatment==1,gen(dup_sale_treat)
count if post_treat==1&dup_sale_treat>=1
*6 treated have duplicated sales that are utilized in the identification for post_treat
*Comparison: DID utilizes 

foreach n in 1 2 3 4 5 {
	tab Ring_`n'kmpost
	duplicates tag PID if Ring_`n'kmpost==1,gen(dup_Ring_`n'kmpost)
	count if Ring_`n'kmpost==1&dup_Ring_`n'kmpost>=1
}
*18 properties have duplicated sales that are utilized in the identification for 1km buffer
*33 for 2km buffer
*16 for 3km buffer
*30 for 4km buffer
*18 for 5km buffer
*Note: the flights are lower in the southern path



