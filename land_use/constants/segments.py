from typing import Dict, List

from caf.core.segments import SegmentsSuper, Segment


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
    output_dict = {
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
    "h": {
        1: "Whole house or bungalow: Detached",
        2: "Whole house or bungalow: Semi-detached",
        3: "Whole house or bungalow: Terraced",
        4: "Flat, maisonette or apartment",
        5: "A caravan or other mobile or temporary structure",
    },
    "hr": {
        1: "Whole house or bungalow: Detached",
        2: "Whole house or bungalow: Semi-detached",
        3: "Whole house or bungalow: Terraced",
        4: "Flat, maisonette or apartment",
    },
    "ha": {
        1: "No adults or 1 adult in household",
        2: "2 adults in household",
        3: "3 or more adults in household",
    },
    "hc": {
        1: "Household with no children or all children non-dependent",
        2: "Household with one or more dependent children",
    },
    "car": {
        1: "No cars or vans in household",
        2: "1 car or van in household",
        3: "2 or more cars or vans in household",
    },
    "age": {
        1: "0 to 4 years",
        2: "5 to 9 years",
        3: "10 to 15 years",
        4: "16 to 19 years",
        5: "20 to 34 years",
        6: "35 to 49 years",
        7: "50 to 64 years",
        8: "65 to 74 years",
        9: "75+ years",
    },
    "agg_age": {
        1: "aged 15 years and under",
        2: "aged 16 to 24 years",
        3: "aged 25 to 34 years",
        4: "aged 35 to 49 years",
        5: "aged 50 years and over"
    },
    "gender": {
        1: "male",
        2: "female"
    },
    "ns_sec": {
        1: "HRP managerial / professional",
        2: "HRP managerial / professional",
        3: "HRP semi-routine / routine",
        4: "HRP never worked / long-term unemployed",
        5: "HRP full-time student"
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
    "pop_soc": {
        1: "SOC1 - Part Time",
        2: "SOC2 - Part Time",
        3: "SOC3 - Part Time",
        4: "SOC1 - Full Time",
        5: "SOC2 - Full Time",
        6: "SOC3 - Full Time",
        7: "Unemployed",
        8: "Retired",
        9: "Full-Time Students",
        10: "Economically inactive: Other"
    }
}

CUSTOM_SEGMENTS = {
    key: Segment(name=key, values=values) for key, values in _CUSTOM_SEGMENT_CATEGORIES.items()
}


if __name__ == '__main__':
    example = ['p', 'tp', 'TfN', 'm', 'Land-Use', 'g']

    print(split_input_segments(example))
