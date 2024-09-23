Data Sources
############

The data sources used in the population model are primarily from two sources; Census
(both England and Wales, and Scotland) and the Office of National Statistics.
The sections below describe each input dataset with the:

- unit of the data,
- geographical level the data are provided in,
- the characteristics (or segmentations) that the data include,
- the source location,
- the file location stored on TfN's local drive, and
- the access requirements for the data.

Base Year Model Data
********************

2021 England and Wales Census Data
==================================

ONS Table 1
-----------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Accomodation type
   * - Source
     - `Office for National Statistics <mailto:Census.CustomerServices@ons.gov.uk>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS custom/ct210212census2021.xlsx
   * - Access
     - Purchased, not open access

ONS Table 2
-----------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households (excluding caravans and other temporary structures)
   * - Geography
     - MSOA 2021
   * - Segmentation
     - Accomodation type, number of adults in the household, number of children in the household, car availability
   * - Source
     - `Office for National Statistics <mailto:Census.CustomerServices@ons.gov.uk>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS custom/ct210213census2021.xlsx
   * - Access
     - Purchased, not open access

ONS Table 3
-----------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents age 16 and over)
   * - Geography
     - MSOA 2021
   * - Segmentation
     - Economic status, SOC group, accomodation type, NS-SeC of Household Reference Person
   * - Source
     - `Office for National Statistics <mailto:Census.CustomerServices@ons.gov.uk>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS custom/ct210214census2021.xlsx
   * - Access
     - Purchased, not open access

ONS Table 4
-----------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Household Reference Persons (excluding caravans and other temporary structures)
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Accomodation type, NS-SeC of Household Reference Person
   * - Source
     - `Office for National Statistics <mailto:Census.CustomerServices@ons.gov.uk>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS custom/ct210215census2021.xlsx
   * - Access
     - Purchased, not open access

Occupied Households
-------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households with occupants
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Accomodation type
   * - Source
     - https://www.nomisweb.co.uk/datasets/c2021rm002
   * - File Location
     - I:/NorMITs Land Use/2023/import/RM002 accom type by household size/2672385425907310 all.csv
   * - Access
     - Freely available to download

Unoccupied Households
---------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households with zero occupants
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Accomodation type
   * - Source
     - https://www.nomisweb.co.uk/datasets/c2021rm002
   * - File Location
     - I:/NorMITs Land Use/2023/import/RM002 accom type by household size/2072764328175065 zero.csv
   * - Access
     - Freely available to download

Age and Gender
--------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - MSOA 2021
   * - Segmentation
     - Age, gender
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS/population_age11_gender_MSOA.csv
   * - Access
     - Freely available to download

Communal Establishments (CE)
----------------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents + living in CE)
   * - Geography
     - LSOA 2021
   * - Segmentation
     - None
   * - Source
     - https://www.nomisweb.co.uk/datasets/c2021ts001
   * - File Location
     - I:/NorMITs Land Use/2023/import/TS001 pop_hh_ce/1226171533660024.csv
   * - Access
     - Freely available to download

CE Types
--------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (living in CE)
   * - Geography
     - MSOA 2021
   * - Segmentation
     - CE type
   * - Source
     - https://www.nomisweb.co.uk/datasets/c2021ts048
   * - File Location
     - I:/NorMITs Land Use/2023/import/TS048  CERs by type/2741727163807526.csv
   * - Access
     - Freely available to download

CE SOC
------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (living in CE)
   * - Geography
     - GOR 2021
   * - Segmentation
     - Age, gender, SOC group
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS/ONS 2021 CERs/CERs_GOR_age11_gender_occupation.csv
   * - Access
     - Freely available to download

CE Economic Status
------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (living in CE)
   * - Geography
     - GOR 2021
   * - Segmentation
     - Age, gender, economic status
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS/ONS 2021 CERs/CERs_GOR_age11_gender_economicstatus.csv
   * - Access
     - Freely available to download

2022 Scotland Census Data
=========================

Scotland Population
-------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - Data Zone 2021
   * - Segmentation
     - Gender, Scotland age groups
   * - Source
     - `Scottish Census Data <https://www.scotlandscensus.gov.uk/search-the-census#/location/topics/list?search=population>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/Census Scotland/Population_age6_gender_DZ2011.csv
   * - Access
     - Freely available to download

Office for National Statistics
==============================

2022 MYPE
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Age, gender
   * - Source
     - `ONS Mid-Year LSOA Population Estimates <https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/MYPE/sapelsoasyoatablefinal.xlsx
   * - Access
     - Freely available to download

2023 MYPE
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population
   * - Geography
     - LAD 2023
   * - Segmentation
     - Age, gender
   * - Source
     - `ONS Mid-Year Population Estimates for England and Wales <https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/MYPE/myebtablesenglandwales20112023.xlsx
   * - Access
     - Freely available to download

AddressBase
===========

2021 AddressBase Dwellings
--------------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Dwellings
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Accommodation type
   * - Source
     - https://www.ordnancesurvey.co.uk/products/addressbase-premium#get
   * - File Location
     - I:/NorMITs Land Use/2023/import/ABP/ABP2021/output_results_all_2021(no red).xlsx
   * - Access
     - Licensed through TfN, not open access

2023 AddressBase Dwellings
--------------------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Dwellings
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Accommodation type
   * - Source
     - https://www.ordnancesurvey.co.uk/products/addressbase-premium#get
   * - File Location
     - I:/NorMITs Land Use/2023/import/ABP/ABP2023/output_results_all_2023(no red).xlsx
   * - Access
     - Licensed through TfN, not open access

Annual Population Survey
========================

T01
---

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (working age only)
   * - Geography
     - GOR 2021
   * - Segmentation
     - Gender, APS economic status
   * - Source
     - `Nomis Statistical Queries <https://www.nomisweb.co.uk/datasets/apsnew>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/APS 2023 for IPF/Regional-based-targets/APS-24-regional-based-targets_revamp.xlsx
   * - Access
     - Freely available to download

T08
---

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (working age only)
   * - Geography
     - GOR 2021
   * - Segmentation
     - Gender, employment status
   * - Source
     - `Nomis Statistical Queries <https://www.nomisweb.co.uk/datasets/apsnew>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/APS 2023 for IPF/Regional-based-targets/APS-24-regional-based-targets_revamp.xlsx
   * - Access
     - Freely available to download

Validation Data
***************

Household Data
==============

Dataset 1
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Car availability
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/households_cars_lsoa_3.csv
   * - Access
     - Freely available to download

Dataset 2
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Number of adults in the household, number of children in the household
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/households_adults_children_lsoa.csv
   * - Access
     - Freely available to download

Dataset 3
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households
   * - Geography
     - LAD 2021
   * - Segmentation
     - Number of adults in the household, number of children in the household, NS-SeC of Household Reference Person
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/households_nssec_adults_children_lad.csv
   * - Access
     - Freely available to download

Dataset 4
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households
   * - Geography
     - LAD 2021
   * - Segmentation
     - Car availability, NS-SeC of Household Reference Person
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/households_nssec_car_lad.csv
   * - Access
     - Freely available to download

Dataset 5
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households
   * - Geography
     - LAD 2021
   * - Segmentation
     - Car availability, NS-SeC of Household Reference Person
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/households_nssec_car_lad.csv
   * - Access
     - Freely available to download

Dataset 6
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Households
   * - Geography
     - LSOA 2021
   * - Segmentation
     - NS-SeC of Household Reference Person
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/households_nssec_lsoa.csv
   * - Access
     - Freely available to download

Population Data
===============

Dataset 1
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - LSOA 2021
   * - Segmentation
     - SOC group
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_soc9_lsoa.csv
   * - Access
     - Freely available to download

Dataset 2
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - LSOA 2021
   * - Segmentation
     - Economic status
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_status_lsoa.csv
   * - Access
     - Freely available to download

Dataset 3
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - LAD 2021
   * - Segmentation
     - Age, gender, SOC group
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_age11_gender_occupation_lad.csv
   * - Access
     - Freely available to download

Dataset 4
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - GOR 2021
   * - Segmentation
     - Age, gender, economic status
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_age11_gender_economicstatus_region.csv
   * - Access
     - Freely available to download

Dataset 5
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - GOR 2021
   * - Segmentation
     - Number of adults in the household, number of children in the household, economic status
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_adults_children_status_region.csv
   * - Access
     - Freely available to download

Dataset 6
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - LAD 2021
   * - Segmentation
     - Number of adults in the household, number of children in the household, SOC group
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_adults_children_occupation_lad.csv
   * - Access
     - Freely available to download

Dataset 7
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - MSOA 2021
   * - Segmentation
     - Number of adults in the household, number of children in the household, age, gender
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_adults_children_age_gender_MSOA.csv
   * - Access
     - Freely available to download

Dataset 8
---------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Population (all usual residents)
   * - Geography
     - MSOA 2021
   * - Segmentation
     - Age, gender, car availability
   * - Source
     - https://www.ons.gov.uk/datasets/create
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS-validation/population_age_gender_car_msoa.csv
   * - Access
     - Freely available to download