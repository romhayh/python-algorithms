import math
import numpy as np

def findBottomLeft(points:list):
    bottomLeft = points[0]
    for point in points:
        if point.y < bottomLeft.y or (point.y == bottomLeft.y and point.x < bottomLeft.x):
            bottomLeft = point
    return bottomLeft

def sortCCW(points:list):
    firstPoint = findBottomLeft(points)
    points.remove(firstPoint)
    points.sort(key=lambda point: math.atan2(point.y - firstPoint.y, point.x - firstPoint.x))
    points.insert(0, firstPoint)

def isLeftTurn(p1, p2, p3):
    return (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x) > 0

def grahamScan(points:list):
    sortCCW(points)
    hull = [points[0], points[1]]
    for point in points[2:]:
        while len(hull) > 1 and not isLeftTurn(hull[-2], hull[-1], point):
            hull.pop()
        hull.append(point)
    onePoint = hull[0]
    hull.append(onePoint)
    return hull