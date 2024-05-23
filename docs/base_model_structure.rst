Base Model Structure
====================

Population
----------

**Work in progress**

.. graphviz::

    digraph G { 
        rankdir="LR"
        nodesep=0.5
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"
                addressbase [label="AddressBase|Number of dwellings|LSOA"];
                
                census_dwellings [label="Census|Total properties by dwelling type\n(occupied and unoccupied)|LSOA"]
                age_gender [label="Census|Proportion of population by\ndwelling type, age, gender|MSOA"]
                
                table_1 [label="ONS Table 1|Population by dwelling type|LSOA"];
                table_2 [label="ONS Table 2|Proportion of households by\ndwelling type, #adults, #children, #cars|MSOA"];
                table_3 [label="ONS Table 3|Proportion of population by\neconomic status/employment status/SOC,\ndwelling type, NS-SeC|MSOA"];
                table_4 [label="ONS Table 4|Proportion of households by\ndwelling type, NS-SeC|LSOA"];
            }
            
        node [style=rounded, color=black]
            output_a [label="Output A|Households by dwelling type, NS-Sec|LSOA"];
            output_b [label="Output B|Households by dwelling type, NS-SeC\n#adults, #children, #cars|LSOA"];
            output_c [label="Output C|Average occupancy by dwelling type|LSOA"];
            output_d [label="Output D|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars|LSOA"];
            output_e [label="Output E|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender|LSOA"];
            output_f [label="Output F|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender, economic status,\nemployment status, SOC|LSOA"];
            
            {rank="same" output_a output_c}
            {rank="same" output_b output_e}
            {rank="same" output_d output_f}
            
        table_4 -> output_a;
        addressbase -> output_a;
        output_a -> output_b;
        table_2 -> output_b
        
        census_dwellings -> output_c;   
        table_1 -> output_c;
        
        output_b -> output_d;
        output_c -> output_d;
        
        age_gender -> output_e;
        output_d -> output_e;
        
        table_3 -> output_f;
        output_e -> output_f;
    }


Employment
----------

*To be added*