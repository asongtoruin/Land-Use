Segmentation
############

Source
======
Generated from ``scenario_configurations\iteration_5\base_employment_config.yml``

Segmentation
############

Source
======
Generated from ``scenario_configurations\iteration_5\base_population_config.yml``

Standard Segments
=================
``sic_1_digit``
-----------

.. list-table::
   :header-rows: 1

   * - Value
     - Description
   * - 1 
     - A : Agriculture, forestry and fishing
   * - 2
     - B : Mining and quarrying
   * - 3
     - C : Manufacturing
   * - 4
     - D : Electricity, gas, steam and air conditioning supply
   * - 5
     - E : Water supply; sewerage, waste management and remediation activities
   * - 6
     - F : Construction
   * - 7
     - G : Wholesale and retail trade; repair of motor vehicles and motorcycles
   * - 8
     - H : Transportation and storage
   * - 9
     - I : Accommodation and food service activities
   * - 10
     - J : Information and communication
   * - 11
     - K : Financial and insurance activities
   * - 12
     - L : Real estate activities
   * - 13
     - M : Professional, scientific and technical activities
   * - 14
     - N : Administrative and support service activities
   * - 15
     - O : Public administration and defence; compulsory social security
   * - 16
     - P : Education
   * - 17
     - Q : Human health and social work activities
   * - 18
     - R : Arts, entertainment and recreation
   * - 19
     - S : Other service activities
   * - 20
     - T : Activities of households as employers; undifferentiated goods-and services-producing activities of households for own use
   * - 21
     - U : Activities of extraterritorial organisations and bodies
   * - -1
     - n/a


``sic_2_digit``
-----------
There is a very large number of possible entries (99) for this as sic 4 digit (Classes) is highly specified.
Notably -1 is used for n/a, which represnts unemployed people.
Segmentation value 4 is not used, as this allows a clearer correspondence between the value and the SIC label.

The full definition is stored in:
https://github.com/Transport-for-the-North/caf.core/blob/IPF/src/caf/core/segments/sic_2_digit.yml

.. list-table::
   :header-rows: 1

   * - Value
     - Description
   * - 1
     - 01 : Crop and animal production, hunting and related service activities
   * - ...
     - ...
   * - 99
     - 99 : Activities of extraterritorial organisations and bodies
   * - -1
     - n/a

``sic_4_digit``
----------
There is a very large number of possible entries (617) for this as sic 4 digit (Classes) is highly specified.
Notably -1 is used for n/a, which represents unemployed people.

Shows below are some sample rows
The full definition is stored in:
https://github.com/Transport-for-the-North/caf.core/blob/IPF/src/caf/core/segments/sic_4_digit.yml

.. list-table::
   :header-rows: 1

   * - Value
     - Description
   * - 1
     - 0100 : DEFRA/Scottish Executive Agricultural Data
   * - ...
     - ...
   * - 616
     - 9900 : Activities of extraterritorial organisations and bodies
   * - -1
     - n/a


``soc``
-------

.. list-table::
   :header-rows: 1

   * - Value
     - Description
   * - 1
     - SOC1
   * - 2
     - SOC2
   * - 3
     - SOC3
   * - 4
     - SOC4


Custom Segments
===============

``total``
---------

.. list-table::
   :header-rows: 1

   * - Value
     - Description
   * - 1
     - all
