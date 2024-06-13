Data Sources
############

The data sources used in the population model are primarily from two sources; Census
(both England and Wales, and Scotland) and the Office of National Statistics.
The sections below describe each input dataset with the:

- unit of the data,
- geographical level the data are provided in,
- the characteristics (or segmentations) that the data include,
- the source location, and
- the file location stored on TfN's local drive.

England and Wales Census Data
=============================

Purchased Datasets
------------------

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - ONS Table 1
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - ONS Table 2
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - ONS Table 3
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - ONS Table 4
     - Data
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

Open Access Datasets
--------------------

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - Occupied Households
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - Age and Gender
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - Communal Establishments
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - Communal Establishment Types
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - Communal Establishment SOC
     - Data
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

.. list-table::
   :header-rows: 1
   :widths: 1 2

   * - Communal Establishment Economic Status
     - Data
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

