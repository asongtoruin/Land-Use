from typing import Dict, List

from caf.core.segments import SegmentsSuper, Segment, Exclusion


def is_standard_segment(identifier: str) -> bool:
    """Confirm whether a provided string corresponds to a standard Segment

    Parameters
    ----------
    identifier : str
        String to check

    Returns
    -------
    bool
        True if belonging to a standard segment, else False.
    """
    try:
        SegmentsSuper(identifier)
        return True
    except ValueError:
        return False


def split_input_segments(input_segments: List[str]) -> Dict[bool, List[str]]:
    """Splits a list of input segment strings into standard segments and custom

    Parameters
    ----------
    input_segments : List[str]
        Segment strings to check

    Returns
    -------
    Dict[bool, List[str]]
        Dictionary with two keys - True and False - with a list for each. True
        corresponds to those segments which are "standard", False corresponds
        to those which are likely to be custom.
    """
    # Could do this with a defaultdict but it's simple enough to not care, and
    # this way we *definitely* get both keys every time
    output_dict : dict[bool, list] = {
        True: [], False: []
    }

    # Update respective lists
    for seg_str in input_segments:
        output_dict[is_standard_segment(seg_str)].append(seg_str)

    return output_dict


_CUSTOM_SEGMENT_CATEGORIES = {
    "total": {
        1: "all"
    },
    "agg_age": {
        1: "aged 15 years and under",
        2: "aged 16 to 24 years",
        3: "aged 25 to 34 years",
        4: "aged 35 to 49 years",
        5: "aged 50 years and over"
    },
    "scot_age": {
        1: "0 - 15",
        2: "16 - 24",
        3: "25 - 34",
        4: "35 - 49",
        5: "50 - 64",
        6: "65 and over"
    },
    "big": {
        1: "1 : Agriculture, forestry & fishing (A)",
        2: "2 : Mining, quarrying & utilities (B,D and E)",
        3: "3 : Manufacturing (C)",
        4: "4 : Construction (F)",
        5: "5 : Motor trades (Part G)",
        6: "6 : Wholesale (Part G)",
        7: "7 : Retail (Part G)",
        8: "8 : Transport & storage (inc postal) (H)",
        9: "9 : Accommodation & food services (I)",
        10: "10 : Information & communication (J)",
        11: "11 : Financial & insurance (K)",
        12: "12 : Property (L)",
        13: "13 : Professional, scientific & technical (M)",
        14: "14 : Business administration & support services (N)",
        15: "15 : Public administration & defence (O)",
        16: "16 : Education (P)",
        17: "17 : Health (Q)",
        18: "18 : Arts, entertainment, recreation & other services (R,S,T and U)",
    },
    'ce': {
        1: 'Medical and care',
        2: 'Defence',
        3: 'Prison, approved premises, and detention',
        4: 'Education',
        5: 'Hotels, hostels, holiday accommodation, and travel',
        6: 'Religion',
        7: 'Staff'
    },
    'soc_9':
    {
        # Standard Occupational Classfications (SOC 2020) only includes those working
        1: "1. Managers, directors and senior officials",
        2: "2. Professional occupations",
        3: "3. Associate professional and technical occupations",
        4: "4. Administrative and secretarial occupations",
        5: "5. Skilled trades occupations",
        6: "6. Caring, leisure and other service occupations",
        7: "7. Sales and customer service occupations",
        8: "8. Process, plant and machine operatives",
        9: "9. Elementary occupations",
    },
    'soc_3':
    {
        # Standard Occupational Classfications (SOC 2020) in groups only includes those working (excludes SOC4)
        # SOC group 1 consists of soc_9 1-3
        # SOC group 2 consists of soc_9 4-7
        # SOC group 3 consists of soc_9 8-9
        1: "SOC group 1",
        2: "SOC group 2",
        3: "SOC group 3",
    },
}

_CUSTOM_EXCLUSIONS = {
    'scot_age':
        {
            'age_9': {
                1: {4, 5, 6, 7, 8, 9},
                2: {1, 2, 3, 6, 7, 8, 9},
                3: {1, 2, 3, 4, 6, 7, 8, 9},
                4: {1, 2, 3, 4, 5, 7, 8, 9},
                5: {1, 2, 3, 4, 5, 6, 8, 9},
                6: {1, 2, 3, 4, 5, 6, 7},
            }
        }
}

CUSTOM_SEGMENTS = dict()
for key, values in _CUSTOM_SEGMENT_CATEGORIES.items():
    exclusions = []
    # check for exclusions
    if key in _CUSTOM_EXCLUSIONS.keys():
        for other_category, exclusion_definitions in _CUSTOM_EXCLUSIONS[key].items():
            for definition in exclusion_definitions:
                exclusions.append(Exclusion(other_name=other_category, exclusions=exclusion_definitions))

    CUSTOM_SEGMENTS[key] = Segment(name=key, values=values, exclusions=exclusions)

if __name__ == '__main__':
    example = ['p', 'tp', 'TfN', 'm', 'Land-Use', 'g']

    print(split_input_segments(example))
