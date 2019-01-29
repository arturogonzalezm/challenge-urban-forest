import re

from shapely import wkt

from polygon_utils import merge_multi_polygons, multi_polygon_area, to_shape, intersection_area


def calculate_multipolygon_area(multipolygons):

    merged = merge_multi_polygons(*multipolygons)
    return multi_polygon_area(merged)


def calculate_multipolygon_bounds(multipolygons):
    bounds = []
    for m in multipolygons:
        bounds.append(to_shape(m).bounds)
    return bounds

# regex to convert geometry str to proper format
match = re.compile('(\s[^\s]*)\s')

def convert(value):
    """
    Convert geometry str to the format with ','
    """
    geometry_type, coordinates = value.split(' ', 1)
    geometry_type = geometry_type.strip()
    standardized_coordinates = match.sub(r'\1,', coordinates.strip())
    return f'{geometry_type} {standardized_coordinates}'

def calculate_forest_bound(polygon):
    """
    To return polygon bounds
    """
    return wkt.loads(polygon).bounds

def join_condition(suburb_bound, forest_bound):
    """
    Condition that suburb has intersection with the forest polygon
    """
    def intersect(box_a, box_b):
        a_min_x, a_min_y, a_max_x, a_max_y = box_a
        b_min_x, b_min_y, b_max_x, b_max_y = box_b
        return a_min_y <= b_max_y and \
               a_max_x >= b_min_x and \
               a_max_y >= b_min_y and \
               a_min_x <= b_max_x

    for sub in suburb_bound:
        if intersect(sub, forest_bound):
            return True

    return False

def calculate_forest_rate(multipolygons, forest_multipolygon, suburb_area):
    """
    This function is to calculate the forest percentage of the suburb
    """
    merged_suburb = merge_multi_polygons(*multipolygons)
    merge_forest = merge_multi_polygons(*[wkt.loads(m) for m in forest_multipolygon])
    return round(intersection_area(merged_suburb, merge_forest) / suburb_area, 3)