from caf.core.segments import SegmentsSuper

from land_use.constants import segments

# TODO: This is very similar to "h". Could we standardise?
ONS_DWELLINGS = {
    1: "Unshared dwelling: Detached",
    2: "Unshared dwelling: Semi-detached",
    3: "Unshared dwelling: Terraced",
    4: "Unshared dwelling: Flat, maisonette or apartment",
    5: "Unshared dwelling: A caravan or other mobile or temporary structure"
}

ONS_NSSEC = {
    1: "NS-SeC of HRP: 1. Higher managerial, administrative and professional occupations; 2. Lower managerial, administrative and professional occupations",
    2: "NS-SeC of HRP: 3. Intermediate occupations; 4. Small employers and own account workers; 5. Lower supervisory and technical occupations",
    3: "NS-SeC of HRP: 6. Semi-routine occupations; 7. Routine occupations",
    4: "NS-SeC of HRP: 8. Never worked or long-term unemployed*",
    5: "NS-SeC of HRP: L15: Full-time student"
}

# TODO: the only difference between this and ONS_NSSEC is the and instead of or in category 4, this is annoying!
ONS_NSSEC_ANNOYING = {
    1: "NS-SeC of HRP: 1. Higher managerial, administrative and professional occupations; 2. Lower managerial, administrative and professional occupations",
    2: "NS-SeC of HRP: 3. Intermediate occupations; 4. Small employers and own account workers; 5. Lower supervisory and technical occupations",
    3: "NS-SeC of HRP: 6. Semi-routine occupations; 7. Routine occupations",
    4: "NS-SeC of HRP: 8. Never worked and long-term unemployed*",
    5: "NS-SeC of HRP: L15: Full-time student"
}

# NOTE: the keys of the {'pop_econ': 1, 'pop_emp': 2, 'soc': 1} dictionaries need
#  to be consistent with the names of the segmentations being used
ONS_ECON_EMP_SOC_COMBO = {
    'Economically active (excluding full-time students): In employment: part-time: Occupation: 1. Managers, directors and senior officials; 2. Professional occupations; 3. Associate professional and technical occupations':
        {'pop_econ': 1, 'pop_emp': 2, 'soc': 1},
    'Economically active (excluding full-time students): In employment: part-time: Occupation: 4. Administrative and secretarial occupations; 5. Skilled trades occupations; 6. Caring, leisure and other service occupations; 7. Sales and customer service occupations':
        {'pop_econ': 1, 'pop_emp': 2, 'soc': 2},
    'Economically active (excluding full-time students): In employment: part-time: Occupation: 8. Process, plant and machine operatives; 9. Elementary occupations':
        {'pop_econ': 1, 'pop_emp': 2, 'soc': 3},
    'Economically active (excluding full-time students): In employment: full-time: Occupation: 1. Managers, directors and senior officials; 2. Professional occupations; 3. Associate professional and technical occupations':
        {'pop_econ': 1, 'pop_emp': 1, 'soc': 1},
    'Economically active (excluding full-time students): In employment: full-time: Occupation: 4. Administrative and secretarial occupations; 5. Skilled trades occupations; 6. Caring, leisure and other service occupations; 7. Sales and customer service occupations':
        {'pop_econ': 1, 'pop_emp': 1, 'soc': 2},
    'Economically active (excluding full-time students): In employment: full-time: Occupation: 8. Process, plant and machine operatives; 9. Elementary occupations':
        {'pop_econ': 1, 'pop_emp': 1, 'soc': 3},
    'Economically active (excluding full-time students): Unemployed':
        {'pop_econ': 2, 'pop_emp': 3, 'soc': 4},
    'Economically inactive: Retired':
        {'pop_econ': 3, 'pop_emp': 5, 'soc': 4},
    'Full-time students':
        {'pop_econ': 4, 'pop_emp': 4, 'soc': 4},
    'Economically inactive: Other':
        {'pop_econ': 3, 'pop_emp': 3, 'soc': 4}
}

ONS_DWELLING_AGE_SEX_MAPPINGS = {
    'Age (11 categories)': [
        'age_9',
        SegmentsSuper.get_segment(SegmentsSuper.AGE).values
    ],
    'Sex (2 categories)': [
        'g', SegmentsSuper.get_segment(SegmentsSuper.GENDER).values
    ],
    'Accommodation type (5 categories)': [
        'accom_h',
        SegmentsSuper.get_segment(SegmentsSuper.ACCOMODATION_TYPE_H).values
    ]
}

AGE_11_TO_9_AGGREGATIONS = {
    'Aged 4 years and under': '0 to 4 years',
    'Aged 5 to 9 years': '5 to 9 years',
    'Aged 10 to 15 years': '10 to 15 years',
    'Aged 16 to 19 years': '16 to 19 years',
    'Aged 20 to 24 years': '20 to 34 years',
    'Aged 25 to 34 years': '20 to 34 years',
    'Aged 35 to 49 years': '35 to 49 years',
    'Aged 50 to 64 years': '50 to 64 years',
    'Aged 65 to 74 years': '65 to 74 years',
    'Aged 75 to 84 years': '75+ years',
    'Aged 85 years and over': '75+ years'
}

CE_POP_BY_TYPE = {
    "Medical and care establishment": 1,
    "Other establishment: Defence": 2,
    "Other establishment: Prison service": 3,
    "Other establishment: Approved premises (probation or bail hostel)": 3,
    "Other establishment: Detention centres and other detention": 3,
    "Other establishment: Education": 4,
    "Other establishment: Hotel, guest house, B&B or youth hostel": 5,
    "Other establishment: Hostel or temporary shelter for the homeless": 5,
    "Other establishment: Holiday accommodation": 5,
    "Other establishment: Other travel or temporary accommodation": 5,
    "Other establishment: Religious": 6,
    "Other establishment: Staff or worker accommodation or Other": 7
}

ONS_ECON_AGE_SEX_MAPPINGS = {
    'Age (11 categories)': [
        'age_9',
        SegmentsSuper.get_segment(SegmentsSuper.AGE).values
    ],
    'Sex (2 categories)': [
        'g', SegmentsSuper.get_segment(SegmentsSuper.GENDER).values
    ],
    'Economic activity status (7 categories)': [
        'pop_econ',
        SegmentsSuper.get_segment(SegmentsSuper.POP_ECON).values
    ]
}

ECON_6_TO_4_AGGREGATIONS = {
    'Economically active (excluding full-time students): In employment': 'Economically active employees',
    'Economically active (excluding full-time students): Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks': 'Economically active unemployed',
    'Economically active and a full-time student: In employment': 'Students',
    'Economically active and a full-time student: Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks': 'Students',
    'Economically inactive (excluding full-time students)': 'Economically inactive',
    'Economically inactive and a full-time student': 'Students',
    'Does not apply': 'Economically inactive'
}

ONS_OCC_AGE_SEX_MAPPINGS = {
    'Age (11 categories)': [
        'age_9',
        SegmentsSuper.get_segment(SegmentsSuper.AGE).values
    ],
    'Sex (2 categories)': [
        'g', SegmentsSuper.get_segment(SegmentsSuper.GENDER).values
    ],
    'Occupation (current) (10 categories)': [
        'soc',
        SegmentsSuper.get_segment(SegmentsSuper.SOC).values
    ]
}

SOC_10_TO_4_AGGREGATIONS = {
    '1. Managers, directors and senior officials': 'SOC1',
    '2. Professional occupations': 'SOC1',
    '3. Associate professional and technical occupations': 'SOC1',
    '4. Administrative and secretarial occupations': 'SOC2',
    '5. Skilled trades occupations': 'SOC2',
    '6. Caring, leisure and other service occupations': 'SOC2',
    '7. Sales and customer service occupations': 'SOC3',
    '8. Process, plant and machine operatives': 'SOC3',
    '9. Elementary occupations': 'SOC3',
    'Does not apply': 'SOC4'
}

# Based on https://onsdigital.github.io/dp-classification-tools/standard-industrial-classification/ONS_SIC_hierarchy_view.html
# Note for most BIG groups the section (A,B,...U) is enough except for G which is split into 3 BIG groups (5,6, and 7)
# based on the type of trade.
SIC_2_DIGIT_TO_BIG_AGGREGATIONS = {
    1: 1,
    2: 1,
    3: 1,
    5: 2,
    6: 2,
    7: 2,
    8: 2,
    9: 2,
    10: 3,
    11: 3,
    12: 3,
    13: 3,
    14: 3,
    15: 3,
    16: 3,
    17: 3,
    18: 3,
    19: 3,
    20: 3,
    21: 3,
    22: 3,
    23: 3,
    24: 3,
    25: 3,
    26: 3,
    27: 3,
    28: 3,
    29: 3,
    30: 3,
    31: 3,
    32: 3,
    33: 3,
    35: 2,
    36: 2,
    37: 2,
    38: 2,
    39: 2,
    41: 4,
    42: 4,
    43: 4,
    45: 5,
    46: 6,
    47: 7,
    49: 8,
    50: 8,
    51: 8,
    52: 8,
    53: 8,
    55: 9,
    56: 9,
    58: 10,
    59: 10,
    60: 10,
    61: 10,
    62: 10,
    63: 10,
    64: 11,
    65: 11,
    66: 11,
    68: 12,
    69: 13,
    70: 13,
    71: 13,
    72: 13,
    73: 13,
    74: 13,
    75: 13,
    77: 14,
    78: 14,
    79: 14,
    80: 14,
    81: 14,
    82: 14,
    84: 15,
    85: 16,
    86: 17,
    87: 17,
    88: 17,
    90: 18,
    91: 18,
    92: 18,
    93: 18,
    94: 18,
    95: 18,
    96: 18,
    97: 18,
    98: 18,
    99: 18,
}


# Based on https://onsdigital.github.io/dp-classification-tools/standard-industrial-classification/ONS_SIC_hierarchy_view.html
SIC_2_DIGIT_TO_SIC_1_DIGIT_AGGREGATIONS = {
    1: 1,
    2: 1,
    3: 1,
    5: 2,
    6: 2,
    7: 2,
    8: 2,
    9: 2,
    10: 3,
    11: 3,
    12: 3,
    13: 3,
    14: 3,
    15: 3,
    16: 3,
    17: 3,
    18: 3,
    19: 3,
    20: 3,
    21: 3,
    22: 3,
    23: 3,
    24: 3,
    25: 3,
    26: 3,
    27: 3,
    28: 3,
    29: 3,
    30: 3,
    31: 3,
    32: 3,
    33: 3,
    35: 4,
    36: 5,
    37: 5,
    38: 5,
    39: 5,
    41: 6,
    42: 6,
    43: 6,
    45: 7,
    46: 7,
    47: 7,
    49: 8,
    50: 8,
    51: 8,
    52: 8,
    53: 8,
    55: 9,
    56: 9,
    58: 10,
    59: 10,
    60: 10,
    61: 10,
    62: 10,
    63: 10,
    64: 11,
    65: 11,
    66: 11,
    68: 12,
    69: 13,
    70: 13,
    71: 13,
    72: 13,
    73: 13,
    74: 13,
    75: 13,
    77: 14,
    78: 14,
    79: 14,
    80: 14,
    81: 14,
    82: 14,
    84: 15,
    85: 16,
    86: 17,
    87: 17,
    88: 17,
    90: 18,
    91: 18,
    92: 18,
    93: 18,
    94: 19,
    95: 19,
    96: 19,
    97: 20,
    98: 20,
    99: 21,
}