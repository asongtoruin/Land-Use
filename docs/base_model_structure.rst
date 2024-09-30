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
            
            addressbase [label="AddressBase 2021|Number of dwellings|LSOA"];
            age_gender [label="Census|Proportion of population by\ndwelling type, age, gender|MSOA"]

            occupied [label="Census|Number of occupied households|LSOA"]
            unoccupied [label="Census|Number of unoccupied households|LSOA"]

            table_1 [label="ONS Table 1|Population by dwelling type|LSOA"];
            table_2 [label="ONS Table 2|Proportion of households by\ndwelling type, #adults, #children, #cars|MSOA"];
            table_3 [label="ONS Table 3|Proportion of population by\neconomic status/employment status/SOC,\ndwelling type, NS-SeC|MSOA"];
            table_4 [label="ONS Table 4|Proportion of households by\ndwelling type, NS-SeC|LSOA"];

            ce_output [label="Communal Establishments|Population by CE type, age, gender, \neconomic status, SOC|LSOA"];

            addressbase_2023 [label="AddressBase 2023|Number of dwellings|LSOA"];

            mype_2022 [label="MYPE 2022|Population by age, gender|LSOA"]
            mype_2023 [label="MYPE 2023|Population by age, gender|LAD"]
            aps_20241 [label="APS 2024|Population by economic status, gender|GOR"]
            aps_20242 [label="APS 2024|Population by employment status, gender|GOR"]

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
            output_p10 [label="Output P10|Population rebalanced with independent datasets|LSOA"];
            output_p11 [label="Output P11|2023 Households by dwelling type, NS-SeC\n#adults, #children, #cars|LSOA"];
            output_p12_1 [label="Output P12.1|2023 Population by dwelling type, NS-SeC\n#adults, #children, #cars|LSOA"];
            output_p12_2 [label="Output P12.2|2023 Population by dwelling type, NS-SeC\nhh#adults, hh#children, hh#cars,\nage, gender, economic status,\nemployment status, SOC|LSOA"];
            output_p13 [label="Output P13|Population IPF to 2023|LSOA"];
            output_p14 [label="Output P14|Population IPF to 2023|LSOA"];


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

        output_p9 -> output_p10

        output_p2 -> output_p11
        output_p4_3 -> output_p11
        addressbase_2023 -> output_p11

        output_p1_3 -> output_p12_1
        output_p11 -> output_p12_1

        output_p12_1 -> output_p12_2
        output_p10 -> output_p12_2

        output_p12_2 -> output_p13
        mype_2022 -> output_p13
        mype_2023 -> output_p13

        output_p13 -> output_p14
        aps_20241 -> output_p14
        aps_20242 -> output_p14
    }


Employment
==========

**Work in progress**

Where the year for geographical area is not stated then it is 2021. Geographies cover England, Scotland and Wales.
SIC Section is the first level (at 1 digit)
SIC Division is the second level (at 2 digit)
SIC Groups is the third level (at 3 digits) which is not used in this process
SIC Class is the forth level (at 4 digit)

SOC has 4 categories, but some of the datasets do not include the full range as SOC=4 represents unemployed people.
For datasets with a SOC Segmentation but where the range is not stated then it will be the full range (1-4).

.. graphviz::

    digraph G {
        rankdir="LR"
        nodesep=0.5
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"
                table_1 [label="BRES 2022 Employment LAD|Jobs by LAD, SIC Class|LAD"];
                table_2 [label="BRES 2022 Employment MSOA|Jobs by MSOA, SIC Division|MSOA 2011"];
                table_2a [label="BRES 2022 Employment MSOA SIC splits|SIC Section and SIC Division splits|MSOA 2011"];
                table_3 [label="BRES 2022 Employment LSOA|Jobs by LSOA, SIC Section|LSOA 2011"];
            }
        
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"
                table_2a [label="Balanced BRES 2022 Employment MSOA|SIC Division|MSOA 2011"];
                table_3a [label="Balanced BRES 2022 Employment LSOA|SIC Section|LSOA 2011"];
            }
            
        node [shape=record, color=blue width=3.4]
            subgraph cluster_inputs{
                peripheries=0
                rank="same"

                table_6 [label="ONS Jobs by SIC and SOC|SIC Section and SOC (1-3)|GOR"];
                table_8 [label="BRES SIC Section by SIC Division|Jobs by SIC Division and Section|MSOA 2011"];
            
            }
        
        node [style=rounded, color=black]
                subgraph cluster_inputs{
                peripheries=0
                rank="same"
                output_e1 [label="Output E1|Jobs by LAD, SIC Class|LAD"];
                output_e2 [label="Output E2|Jobs by MSOA, SIC Division|MSOA"];
                output_e3 [label="Output E3|Jobs by LSOA, SIC Section|LSOA"];
            }
                
        node [shape=record, color=blue width=3.4]
            table_7 [label="Jobs by LSOA|SIC Section and SOC (1-3)|LSOA"];
            table_7a [label="Jobs by LSOA|SIC (Section and Division) and SOC (1-3)|LSOA"];
            table_11 [label="Jobs by LSOA|SIC (Section and Division) and SOC|LSOA"];
            table_10 [label="SOC 4 Factors|SOC 4 proportions by region|GOR"];
        
        node [shape=record, color=blue width=3.4]
            table_4 [label="WFJ 2023|Total workforce jobs by region|GOR"];
            
        node [style=rounded, color=black]
            output_e4 [label="Output E4|Jobs by LSOA, \nSIC (Section and Division), SOC|LSOA"];
            output_e4_2 [label="Output E4.2|Jobs by LSOA, SIC Division,\nSOC weighted to WFJ|LSOA"];
            output_e5 [label="Output E5|Jobs by LSOA, SOC\n,SIC (Class, Section, Division)|LSOA"];
        

        table_1 -> output_e1;
        output_e1 -> output_e5
        table_1 -> table_2a;
        table_1 -> table_3a;
        table_2 -> table_2a;
        table_2a -> output_e2;
        table_3 -> table_3a;
        table_3a -> output_e3;
        output_e3 -> table_7;
        table_6 -> table_7;
        table_8 -> table_7a
        table_11 -> output_e4
        table_7 -> table_7a
        table_7a -> table_11
        table_10 -> table_11
        table_4 -> output_e4_2
        output_e4 -> output_e4_2
        output_e4 -> output_e5
    }
