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
    "scot_age": {
        1: "0 - 15",
        2: "16 - 24",
        3: "25 - 34",
        4: "35 - 49",
        5: "50 - 64",
        6: "65 and over"
    },
    "gender": {
        1: "male",
        2: "female"
    },
    "ns_sec": {
        1: "HRP managerial / professional",
        2: "HRP intermediate / technical",
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
        1: "SOC1",
        2: "SOC2",
        3: "SOC3",
        4: "SOC4"
    },
    'pop_emp': {
        1: 'full_time',
        2: 'part_time',
        3: 'unemployed',
        4: 'students',
        5: 'non-working_age'
    },
    'pop_econ': {
        1: 'Economically active employees',
        2: 'Economically active unemployed',
        3: 'Economically inactive',
        4: 'Students'
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
    'sic_2_digit': {
        1: "01 : Crop and animal production, hunting and related service activities",
        2: "02 : Forestry and logging",
        3: "03 : Fishing and aquaculture",
        5: "05 : Mining of coal and lignite",
        6: "06 : Extraction of crude petroleum and natural gas",
        7: "07 : Mining of metal ores",
        8: "08 : Other mining and quarrying",
        9: "09 : Mining support service activities",
        10: "10 : Manufacture of food products",
        11: "11 : Manufacture of beverages",
        12: "12 : Manufacture of tobacco products",
        13: "13 : Manufacture of textiles",
        14: "14 : Manufacture of wearing apparel",
        15: "15 : Manufacture of leather and related products",
        16: "16 : Manufacture of wood and of products of wood and cork, except furniture;manufacture of articles of straw and plaiting materials",
        17: "17 : Manufacture of paper and paper products",
        18: "18 : Printing and reproduction of recorded media",
        19: "19 : Manufacture of coke and refined petroleum products",
        20: "20 : Manufacture of chemicals and chemical products",
        21: "21 : Manufacture of basic pharmaceutical products and pharmaceutical preparations",
        22: "22 : Manufacture of rubber and plastic products",
        23: "23 : Manufacture of other non-metallic mineral products",
        24: "24 : Manufacture of basic metals",
        25: "25 : Manufacture of fabricated metal products, except machinery and equipment",
        26: "26 : Manufacture of computer, electronic and optical products",
        27: "27 : Manufacture of electrical equipment",
        28: "28 : Manufacture of machinery and equipment n.e.c.",
        29: "29 : Manufacture of motor vehicles, trailers and semi-trailers",
        30: "30 : Manufacture of other transport equipment",
        31: "31 : Manufacture of furniture",
        32: "32 : Other manufacturing",
        33: "33 : Repair and installation of machinery and equipment",
        35: "35 : Electricity, gas, steam and air conditioning supply",
        36: "36 : Water collection, treatment and supply",
        37: "37 : Sewerage",
        38: "38 : Waste collection, treatment and disposal activities; materials recovery",
        39: "39 : Remediation activities and other waste management services. This division includes the provision of remediation services, i.e. the cleanup of contaminated buildings and sites, soil, surface or ground water.",
        41: "41 : Construction of buildings",
        42: "42 : Civil engineering",
        43: "43 : Specialised construction activities",
        45: "45 : Wholesale and retail trade and repair of motor vehicles and motorcycles",
        46: "46 : Wholesale trade, except of motor vehicles and motorcycles",
        47: "47 : Retail trade, except of motor vehicles and motorcycles",
        49: "49 : Land transport and transport via pipelines",
        50: "50 : Water transport",
        51: "51 : Air transport",
        52: "52 : Warehousing and support activities for transportation",
        53: "53 : Postal and courier activities",
        55: "55 : Accommodation",
        56: "56 : Food and beverage service activities",
        58: "58 : Publishing activities",
        59: "59 : Motion picture, video and television programme production, sound recording and music publishing activities",
        60: "60 : Programming and broadcasting activities",
        61: "61 : Telecommunications",
        62: "62 : Computer programming, consultancy and related activities",
        63: "63 : Information service activities",
        64: "64 : Financial service activities, except insurance and pension funding",
        65: "65 : Insurance, reinsurance and pension funding, except compulsory social security",
        66: "66 : Activities auxiliary to financial services and insurance activities",
        68: "68 : Real estate activities",
        69: "69 : Legal and accounting activities",
        70: "70 : Activities of head offices; management consultancy activities",
        71: "71 : Architectural and engineering activities; technical testing and analysis",
        72: "72 : Scientific research and development",
        73: "73 : Advertising and market research",
        74: "74 : Other professional, scientific and technical activities",
        75: "75 : Veterinary activities",
        77: "77 : Rental and leasing activities",
        78: "78 : Employment activities",
        79: "79 : Travel agency, tour operator and other reservation service and related activities",
        80: "80 : Security and investigation activities",
        81: "81 : Services to buildings and landscape activities",
        82: "82 : Office administrative, office support and other business support activities",
        84: "84 : Public administration and defence; compulsory social security",
        85: "85 : Education",
        86: "86 : Human health activities",
        87: "87 : Residential care activities",
        88: "88 : Social work activities without accommodation",
        90: "90 : Creative, arts and entertainment activities",
        91: "91 : Libraries, archives, museums and other cultural activities",
        92: "92 : Gambling and betting activities",
        93: "93 : Sports activities and amusement and recreation activities",
        94: "94 : Activities of membership organisations",
        95: "95 : Repair of computers and personal and household goods",
        96: "96 : Other personal service activities",
        97: "97 : Activities of households as employers of domestic personnel",
        98: "98 : Undifferentiated goods- and services-producing activities of private households for own use",
        99: "99 : Activities of extraterritorial organisations and bodies",
    }
}

_CUSTOM_EXCLUSIONS = {
    'hc': {
        'age': [{'own_val': 1, 'other_vals': {1, 2, 3}}],
        'agg_age': [{'own_val': 1, 'other_vals': {1}}]
    },
    # TODO check if children are students or non-working age in pop_emp
    'pop_emp': {
        'age': [{'own_val': i, 'other_vals': {1, 2, 3}} for i in range(1, 4)]
    }
}

CUSTOM_SEGMENTS = dict()
for key, values in _CUSTOM_SEGMENT_CATEGORIES.items():
    exclusions = []
    # check for exclusions
    if key in _CUSTOM_EXCLUSIONS.keys():
        for other_category, exclusion_definitions in _CUSTOM_EXCLUSIONS[key].items():
            for definition in exclusion_definitions:
                exclusions.append(Exclusion(seg_name=other_category, **definition))

    CUSTOM_SEGMENTS[key] = Segment(name=key, values=values, exclusions=exclusions)

if __name__ == '__main__':
    example = ['p', 'tp', 'TfN', 'm', 'Land-Use', 'g']

    print(split_input_segments(example))
