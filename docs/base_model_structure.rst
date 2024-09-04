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
            communal_establishments [label="Census|Total population in \nCommunal Establishments|LSOA"]
            ce_type [label="Census|Total population in Communal \nEstablishments by CE type|MSOA"]
            ce_pop_soc [label="Census|Total population in Communal \nEstablishments by age, gender, SOC|GOR"]
            ce_pop_econ [label="Census|Total population in Communal \nEstablishments by age, gender, \neconomic status|GOR"]
            
            addressbase [label="AddressBase|Number of dwellings|LSOA"];
            age_gender [label="Census|Proportion of population by\ndwelling type, age, gender|MSOA"]

            occupied [label="Census|Number of occupied households|LSOA"]
            unoccupied [label="Census|Number of unoccupied households|LSOA"]

            table_1 [label="ONS Table 1|Population by dwelling type|LSOA"];
            table_2 [label="ONS Table 2|Proportion of households by\ndwelling type, #adults, #children, #cars|MSOA"];
            table_3 [label="ONS Table 3|Proportion of population by\neconomic status/employment status/SOC,\ndwelling type, NS-SeC|MSOA"];
            table_4 [label="ONS Table 4|Proportion of households by\ndwelling type, NS-SeC|LSOA"];

            ce_output [label="Communal Establishments|Population by CE type, age, gender, \neconomic status, SOC|LSOA"];

            mype_2022 [label="MYPE 2022|Population by age, gender|LSOA"]
            mype_2023 [label="MYPE 2023|Population by age, gender|LAD"]

        node [style=rounded, color=black]

            output_p1_1 [label="Output P1.1|Occupied Households|LSOA"];
            output_p1_2 [label="Output P1.2|Unoccupied Households|LSOA"];
            output_p1_3 [label="Output P1.3|Average Household Occupancy|LSOA"];
            output_p2 [label="Output P2|Adjusted Number of Dwellings|LSOA"];
            output_p3 [label="Output P3|Households by dwelling type, NS-Sec|LSOA"];
            output_p4_1 [label="Output P4.1|Households by dwelling type, NS-SeC\n#adults, #children, #cars|LSOA"];
            output_p4_2 [label="Output P4.2|Households rebalanced with input datasets|LSOA"];
            output_p4_3 [label="Output P4.3|Households rebalanced with indpendent datasets|LSOA"];
            output_p5 [label="Output P5|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars|LSOA"];
            output_p6 [label="Output P6|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender|LSOA"];
            output_p7 [label="Output P7|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender, economic status,\nemployment status, SOC|LSOA"];
            output_p8 [label="Output P8|Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender, economic status,\nemployment status, SOC|LSOA"];
            output_p9 [label="Output P9|Population rebalanced with input datasets|LSOA"];
            output_p10 [label="Output P10|Population rebalanced with indpendent datasets|LSOA"];
            output_p11 [label="Output P11|Population uplifted to 2023 totals|LSOA"];


        occupied -> output_p1_1;
        unoccupied -> output_p1_2;

        occupied -> output_p1_3;
        unoccupied -> output_p1_3;
        table_1 -> output_p1_3;

        output_p1_1 -> output_p2;
        output_p1_2 -> output_p2;
        addressbase -> output_p2

        table_4 -> output_p3;
        output_p2 -> output_p3;
        output_p3 -> output_p4_1;
        table_2 -> output_p4_1

        output_p4_1 -> output_p4_2;
        table_2 -> output_p4_2;
        output_p4_2 -> output_p4_3

        output_p4_3 -> output_p5;
        output_p1_3 -> output_p5

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

        output_p8 -> output_p9
        age_gender -> output_p9
        table_3 -> output_p9

        output_p9 -> output_p10

        mype_2022 -> output_p11
        mype_2023 -> output_p11
        output_p10 -> output_p11
    }


Employment
==========

**Work in progress**

Where the year for geographical area is not stated then it is 2021. Geographies cover England, Scotland and Wales.

.. graphviz::

    digraph G {
        rankdir="LR"
        nodesep=0.5
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"
                table_2 [label="BRES 2022 Employment MSOA|Jobs by MSOA, SIC Division (2 digit)|MSOA 2011"];
                table_1 [label="BRES 2022 Employment LAD|Jobs by LAD, SIC Class (4 digit)|LAD"];
                table_3 [label="BRES 2022 Employment LSOA|Jobs by LSOA, SIC Section (1 digit)|LSOA 2011"];
            }
        
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"
                table_2a [label="Balanced BRES 2022 Employment MSOA|Jobs by MSOA, SIC Division (2 digit)|MSOA 2011"];
                table_3a [label="Balanced BRES 2022 Employment LSOA|Jobs by LSOA, SIC Section (1 digit)|LSOA 2011"];
            }
        
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                rank="same"
                peripheries=0
                table_4 [label="ONS Industry to Occupation|Number of jobs by\nIndustry (A-U), SOC group (1-4)|GOR"];
                table_5 [label="ONS Industry to SIC Section|Correspondence between\nIndustry and SIC Section (1 digit)"];
            }
            
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"

                table_6 [label="Occupation Splits by Industry|% splits by Occupation, Industry, Region|GOR"];
                table_8 [label="SIC Division by SIC Section|% splits by SIC Division (2 digit)\nby SIC Section (1 digit)|MSOA 2011"];
            
            }
        
        node [style=rounded, color=black]
                subgraph cluster_inputs{
                peripheries=0
                rank="same"
                output_e1 [label="Output E1|Jobs by LAD, SIC Class (4 digit)|LAD"];
                output_e2 [label="Output E2|Jobs by MSOA, SIC Division (2 digit)|MSOA"];
                output_e3 [label="Output E3|Jobs by LSOA, SIC Section (1 digit)|LSOA"];
            }
                
        node [shape=record, color=blue width=3.4]
            table_7 [label="Jobs by LSOA with SOC group|Jobs by LSOA, SOC group (1-4)|LSOA"];
        
        node [shape=record, color=blue width=3.4]
            wfj_2023 [label="WFJ 2023|Total workforce jobs by region|GOR"];
            
        node [style=rounded, color=black]
            output_e4 [label="Output E4|Jobs by LSOA, SIC (1 and 2 digit),\nSOC group (1-4)|LSOA"];
            output_e4_2 [label="Output E4.2|Jobs by LSOA, SIC Division (2 digit),\nSOC group (1-4)\nweighted to WFJ|LSOA"];
            output_e5 [label="Output E5|Jobs by LSOA, SIC Division (2 digit),\nSIC Division (4 digit), SOC group (1-4)|LSOA"];
        
        {rank="same" output_e3 table_6 table_8}    

        table_1 -> output_e1;
        output_e1 -> output_e5
        table_1 -> table_2a;
        table_1 -> table_3a;
        table_2 -> table_2a;
        table_2a -> output_e2;
        table_3 -> table_3a;
        table_3a -> output_e3;
        table_4 -> table_6;
        table_5 -> table_6;
        output_e3 -> table_7;
        table_6 -> table_7;
        table_8 -> output_e4
        table_7 -> output_e4
        table_2 -> table_8
        wfj_2023 -> output_e4_2
        output_e4 -> output_e4_2
        output_e4 -> output_e5
    }


