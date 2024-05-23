Base Model Structure
====================

Population
----------

**Work in progress**

.. graphviz::

   digraph G { 
       node [shape=rectangle, color=blue]
           table_4 [label="ONS Table 4\nProportion of households by\ndwelling type, NS-SeC\n@ LSOA"];
           addressbase [label="AddressBase\nNumber of dwellings\n@ LSOA"];
           table_2 [label="ONS Table 2\nProportion of households\nby dwelling type, #adults,\n#children, #cars\n@ MSOA"];
           
       node [style=rounded, color=black]
           output_a [label="Output A\nHouseholds by dwelling type, NS-Sec\n@LSOA"];
           output_b [label="Output B\nHouseholds by dwelling type, NS-SeC\n#adults, #children, #cars\n@ LSOA"];
           
           
           
       table_4 -> output_a;
       addressbase -> output_a;
       output_a -> output_b;
       table_2 -> output_b
   }


Employment
----------

*To be added*