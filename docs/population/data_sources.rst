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

2021 England and Wales Census Data
=============================

``ONS Table 1``
---------------

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

``ONS Table 2``
---------------

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

``ONS Table 3``
---------------

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

``ONS Table 4``
---------------

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

``Occupied Households``
-----------------------

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

``Unoccupied Households``
-----------------------

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

``Age and Gender``
------------------

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

``Communal Establishments (CE)``
--------------------------------

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

``CE Types``
------------

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

``CE SOC``
----------

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

``CE Economic Status``
----------------------

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

Office for National Statistics
==============================
``2022 MYPE``
-------------

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
     - `ONS Mid-Year Population Estimate Downloads <https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/MYPE/sapelsoasyoatablefinal.xlsx
   * - Access
     - Freely available to download

AddressBase
===========
``2021 AddressBase Dwellings``
------------------------------

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

``2023 AddressBase Dwellings``
------------------------------

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