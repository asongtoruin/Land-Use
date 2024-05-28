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