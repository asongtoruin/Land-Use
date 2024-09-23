Data Sources
############

The data sources used in the employment model are primarily from three sources; BRES 2022 Employment, the Office of National Statistics and the Workforce Jobs Survey 203..
The sections below describe each input dataset with the:

- unit of the data,
- geographical level the data are provided in,
- the characteristics (or segmentations) that the data include,
- the source location,
- the file location stored on TfN's local drive, and
- the access requirements for the data.


BRES 2022
==============================
``BRES 2022 Employment LAD`` 
---------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Jobs
   * - Geography
     - LAD 2021
   * - Segmentation
     - SIC Class (4 digit)
   * - Source
     - https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=189
   * - File Location
     - I:/NorMITs Land Use/2023/import/BRES2022/Employment/bres_employment22_lad_4digit_sic.csv
   * - Access
     - Freely available to download

``BRES 2022 Employment LSOA``  !!!!!!!!!!!!!!!!!!!!!- update/check
---------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Jobs
   * - Geography
     - LSOA 2011
   * - Segmentation
     - SIC Section (1 digit)
   * - Source
     - https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=189
   * - File Location
     - I:/NorMITs Land Use/2023/import/BRES2022/Employment/bres_employment22_lsoa2011_1digit_sic.csv
   * - Access
     - Freely available to download

``BRES 2022 Employment MSOA``  !!!!!!!!!!!!!!!!!!!!!- update/check
---------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Jobs
   * - Geography
     - MSOA 2011
   * - Segmentation
     - SIC Division (2 digit)
   * - Source
     - https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=189
   * - File Location
     - I:/NorMITs Land Use/2023/import/BRES2022/Employment/bres_employment22_msoa2011_2digit_sic_jobs.csv
   * - Access
     - Freely available to download

Office for National Statistics
==============================
``ONS Industry (SIC) to Occupation (SOC)``  !!!!!!!!!!!!!!!!!!!!!- update/check
---------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Jobs !!!!!!!!!!!!!!!!!!!!!- update/check
   * - Geography
     - GOR 2021
   * - Segmentation
     - SIC Section (1 digit)
     - SOC group
   * - Source
     - `Office for National Statistics <mailto:Census.CustomerServices@ons.gov.uk>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/ONS/industry_occupation/population_region_1sic_soc.csv
   * - Access
     - Freely available to download

WFJ
==============================
``WFJ 2023``
---------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Jobs (Total workforce jobs)
   * - Geography
     - GOR
   * - Segmentation
     - Total
   * - Source
     - `Office for National Statistics <mailto:Census.CustomerServices@ons.gov.uk>`_
   * - File Location
     - I:/NorMITs Land Use/2023/import/BRES2022/Employment/Employment Investigation/WFJ.csv
   * - Access
     - Freely available to download

``SOC 4 factors``
---------------

.. list-table::
   :header-rows: 0
   :widths: 1 2
   :stub-columns: 1

   * - Unit
     - Percentages (of all residents that are unemployed)
   * - Geography
     - GOR
   * - Segmentation
     - Total
   * - Source
     - TfN internal analysis based on other sources
   * - File Location
     - I:/NorMITs Land Use/2023/import/SOC/Table 8 WFJ-adjusted Land Use SOC4.csv
   * - Access
     - TfN internal analysis