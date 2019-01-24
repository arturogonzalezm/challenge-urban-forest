# you could do with some help from the given types and functions ;)
# below are some usage examples
from polygon_utils import *

# Carlton Gardens polygon
carltonGardensPerimeterLoop = [
    (144.970087, -37.800805), (144.974108, -37.801204), (144.973073, -37.807611), (144.968972, -37.807191)
]
carltonGardensLakeLoop = [
    (144.969768, -37.805891), (144.970074, -37.806206), (144.969954, -37.806332), (144.969583, -37.806153),
    (144.969689, -37.805985)
]
carltonGardensPolygon = [carltonGardensPerimeterLoop, carltonGardensLakeLoop]

# Royal Exhibition Building polygon
exhibitionBuildingPolygon = [
    [
        (144.972502, -37.804601), (144.972396, -37.805126), (144.970485, -37.804884), (144.970565, -37.804350),
        (144.971369, -37.804443), (144.971420, -37.804191), (144.971804, -37.804228), (144.971760, -37.804487)
    ]
]

# WeWork block polygon
weWorkBlockPolygon = [
    [
        (144.963286, -37.814212), (144.964498, -37.813854), (144.964962, -37.814806), (144.963711, -37.815168)
    ]
]

carltonGardensArea = multi_polygon_area([carltonGardensPolygon])
print(carltonGardensArea)
exhibitionBuildingArea = multi_polygon_area([exhibitionBuildingPolygon])
print(exhibitionBuildingArea)
weWorkBlockArea = multi_polygon_area([weWorkBlockPolygon])
print(weWorkBlockArea)

merge = merge_multi_polygons([exhibitionBuildingPolygon], [weWorkBlockPolygon])


def approximately(a, b):
    return abs(a - b) < 0.000000001


assert approximately((exhibitionBuildingArea + weWorkBlockArea), multi_polygon_area(merge))

assert may_intersect([carltonGardensPolygon], [exhibitionBuildingPolygon])
assert intersection_area([carltonGardensPolygon], [exhibitionBuildingPolygon]) > 0.0

assert not may_intersect([carltonGardensPolygon], [weWorkBlockPolygon])
assert intersection_area([carltonGardensPolygon], [weWorkBlockPolygon]) == 0.0
