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
    'h': {1: "Whole house or bungalow: Detached",
          2: "Whole house or bungalow: Semi-detached",
          3: "Whole house or bungalow: Terraced",
          4: "Flat, maisonette or apartment",
          5: "A caravan or other mobile or temporary structure"
          }
}

CUSTOM_SEGMENTS = {
    key: Segment(name=key, values=values) for key, values in _CUSTOM_SEGMENT_CATEGORIES.items()
}


if __name__ ==  '__main__':
    example = ['p', 'tp', 'TfN', 'm', 'Land-Use', 'g']

    print(split_input_segments(example))
