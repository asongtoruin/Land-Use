Base Model Structure
####################

Introduction
============
This page can be used as a reference for the general flow of data within each of the base models.

Population
==========

**Work in progress**

.. graphviz::

    digraph G {
        rankdir="LR"
        nodesep=0.5
        node [shape=record, color=blue width=3.4]
            subgraph ce_inputs {
                peripheries=0
                rank="same"
                communal_establishments [label="Census|Total population in \nCommunal Establishments|LSOA"]
                ce_type [label="Census|Total population in Communal \nEstablishments by CE type|MSOA"]
                ce_pop_soc [label="Census|Total population in Communal \nEstablishments by age, gender, SOC|GOR"]
                ce_pop_econ [label="Census|Total population in Communal \nEstablishments by age, gender, \neconomic status|GOR"]

            }
            subgraph cluster_inputs{
                peripheries=0
                rank="same"

                addressbase [label="AddressBase|Number of dwellings|LSOA"];
                age_gender [label="Census|Proportion of population by\ndwelling type, age, gender|MSOA"]

                occupied [label="Census|Number of occupied households|LSOA"]
                unoccupied [label="Census|Number of unoccupied households|LSOA"]

                table_1 [label="ONS Table 1|Population by dwelling type|LSOA"];
                table_2 [label="ONS Table 2|Proportion of households by\ndwelling type, #adults, #children, #cars|MSOA"];
                table_3 [label="ONS Table 3|Proportion of population by\neconomic status/employment status/SOC,\ndwelling type, NS-SeC|MSOA"];
                table_4 [label="ONS Table 4|Proportion of households by\ndwelling type, NS-SeC|LSOA"];

                ce_output [label="Communal Establishments|Population by CE type, age, gender, \neconomic status, SOC|LSOA"];
            }

        node [style=rounded, color=black]

            output_p11 [label="Output P1.1|Occupied Households|LSOA"];
            output_p12 [label="Output P1.2|Unoccupied Households|LSOA"];
            output_p13 [label="Output P1.3|Average Household Occupancy|LSOA"];
            output_p2 [label="Output P2|Adjusted Number of Dwellings|LSOA"];
            output_p3 [label="Output P3|Households by dwelling type, NS-Sec|LSOA"];
            output_p4 [label="Output P4|Households by dwelling type, NS-SeC\n#adults, #children, #cars|LSOA"];
            output_p5 [label="Output P5|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars|LSOA"];
            output_p6 [label="Output P6|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender|LSOA"];
            output_p7 [label="Output P7|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender, economic status,\nemployment status, SOC|LSOA"];
            output_p8 [label="Output P8|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender, economic status,\nemployment status, SOC|LSOA"];

            {rank="same" output_p11 output_p12 output_p13}
            {rank="same" output_p2}
            {rank="same" output_p3}
            {rank="same" output_p4}
            {rank="same" output_p5}
            {rank="same" output_p6}
            {rank="same" output_p7}
            {rank="same" output_p8}

        occupied -> output_p11;
        unoccupied -> output_p12;

        occupied -> output_p13;
        unoccupied -> output_p13;
        table_1 -> output_p13;

        output_p11 -> output_p2;
        output_p12 -> output_p2;
        addressbase -> output_p2

        table_4 -> output_p3;
        output_p2 -> output_p3;
        output_p3 -> output_p4;
        table_2 -> output_p4

        output_p4 -> output_p5;
        output_p13 -> output_p5

        age_gender -> output_p6;
        output_p5 -> output_p6

        table_3 -> output_p7;
        output_p6 -> output_p7

        communal_establishments -> ce_output
        ce_type -> ce_output
        ce_pop_soc -> ce_output
        ce_pop_econ -> ce_output
        ce_output -> output_p8;
        output_p7 -> output_p8
    }


Employment
==========

**Work in progress**

Where the year for geographical area is not stated then it is 2021.

.. graphviz::

    digraph G {
        rankdir="LR"
        nodesep=0.5
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"

                table_1 [label="BRES 2022 Employment LAD|Jobs by LAD, SIC Class (4 digit)|LAD"];
                table_2 [label="BRES 2022 Employment MSOA|Jobs by MSOA, SIC Division (2 digit)|MSOA 2011"];
                table_3 [label="BRES 2022 Employment LSOA|Jobs by LSOA, SIC Section (1 digit)|LSOA 2011"];
                table_4 [label="ONS Industry to Occupation|Number of jobs by\nIndustry (A-U), SOC group (1-3)|GOR"];
                table_5 [label="ONS Industry to SIC Section|Correspondece between\nIndustry and SIC Section"];
            }
            
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"

                table_6 [label="Occupation Splits by Industry|% splits by Occupation, Industry, Region|GOR"];
                table_8 [label="SIC Division by SIC Section|% splits by SIC Division (2 digit)\nby SIC Section (1 digit)|MSOA 2011"];
            
            }
        
        node [style=rounded, color=black]
            output_e1 [label="Output E1|Jobs by LAD, SIC Class (4 digit)|LAD"];
            output_e2 [label="Output E2|Jobs by MSOA, SIC Division (2 digit)|MSOA"];
            output_e3 [label="Output E3|Jobs by LSOA, SIC Section (1 digit)|LSOA"];
        
        node [shape=record, color=blue width=3.4]
            table_7 [label="Jobs by LSOA with SOC group|Jobs by LSOA, SOC group (1-3)|LSOA"];
            
        node [style=rounded, color=black]
            output_e4 [label="Output E4|Jobs by LSOA, SIC Division (2 digit),\nSOC group (1-3)|LSOA"];
        
        {rank="same" output_e3 table_6 table_8}    

        table_1 -> output_e1;
        table_2 -> output_e2;
        table_3 -> output_e3;
        table_4 -> table_6;
        table_5 -> table_6;
        output_e3 -> table_7;
        table_6 -> table_7;
        table_8 -> output_e4
        table_7 -> output_e4
        table_2 -> table_8

    }

