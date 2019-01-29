from itertools import chain

from shapely.geometry import asShape, Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


# polygon is like:
#
# {
#     'type': 'Polygon',
#     'coordinates': [[(144.963286, -37.814212), (144.964498, -37.813854), (144.964962, -37.814806)]]
# }
#
# multi-polygon is like:
#
# {
#     'type': 'MultiPolygon',
#     'coordinates': [[[(144.963286, -37.814212), (144.964498, -37.813854), (144.964962, -37.814806)]]]
# }

# --- private ----------------------------------------------------------------------------------------------------------

def to_shape(multi_polygon):
    def to_multi_polygon_json(multi_polygon_json):
        ps = []
        for p in multi_polygon_json:
            ls = []
            for l in p:
                cs = []
                for c in l:
                    ds = []
                    for d in c:
                        ds.append(d)
                    cs.append((ds[0], ds[1]))
                ls.append(cs)
            ps.append(ls)
        return {'type': 'MultiPolygon', 'coordinates': ps}

    return asShape(to_multi_polygon_json(multi_polygon))


def to_polygon(polygon_shape):
    def coords(loop):
        cs = []
        for c in loop.coords:
            cs.append(c)
        return cs

    e = coords(polygon_shape.exterior)

    is_ = []
    for i in polygon_shape.interiors:
        is_.append(coords(i))

    is_.append(e)
    is_.reverse()
    return is_


def to_multi_polygon(multi_polygon_shape):
    ps = []
    for p in multi_polygon_shape.geoms:
        ps.append(to_polygon(p))
    return ps


# --- public interface -------------------------------------------------------------------------------------------------

def merge_multi_polygons(*multi_polygons):
    mps = []
    for mp in multi_polygons:
        if not isinstance(mp, BaseGeometry):
            mp = to_shape(mp)
        mps.append(mp)
    union = unary_union(mps)
    if union.geom_type == 'MultiPolygon':
        res = to_multi_polygon(union)
    else:
        res = [to_polygon(union)]
    return res


def may_intersect(multi_polygon_a, multi_polygon_b):

    multi_polygon_a = to_shape(multi_polygon_a)
    multi_polygon_b = to_shape(multi_polygon_b)
    a_min_x, a_min_y, a_max_x, a_max_y = multi_polygon_a.bounds
    b_min_x, b_min_y, b_max_x, b_max_y = multi_polygon_b.bounds

    return a_min_y <= b_max_y and \
           a_max_x >= b_min_x and \
           a_max_y >= b_min_y and \
           a_min_x <= b_max_x


def may_intersect_modified(multi_polygons, multi_polygon_b):
    b_min_x, b_min_y, b_max_x, b_max_y = multi_polygon_b.bounds
    for multi_polygon in multi_polygons:
        for polygon in multi_polygon:
            a_min_x, a_min_y, a_max_x, a_max_y = Polygon(list(chain(*polygon))).bounds
            if a_min_y <= b_max_y and \
               a_max_x >= b_min_x and \
               a_max_y >= b_min_y and \
               a_min_x <= b_max_x:
                return True

    return False


def multi_polygon_area(multi_polygon):
    return to_shape(multi_polygon).area


def intersection_area(multi_polygon_a, multi_polygon_b):
    a = to_shape(multi_polygon_a)
    b = to_shape(multi_polygon_b)
    return a.intersection(b).area
