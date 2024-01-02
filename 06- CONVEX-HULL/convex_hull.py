import math

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

def findBottomLeft(points: list):
    return min(points, key=lambda p: (p.y, p.x))

def sortCCW(points: list):
    firstPoint = findBottomLeft(points)
    points.remove(firstPoint)
    points.sort(key=lambda p: math.atan2(p.y - firstPoint.y, p.x - firstPoint.x))
    points.insert(0, firstPoint)

def isLeftTurn(p1, p2, p3):
    return (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x) > 0

def grahamScan(points: list):
    sortCCW(points)
    hull = [points[0], points[1]]

    for point in points[2:]:
        while len(hull) > 1 and not isLeftTurn(hull[-2], hull[-1], point):
            hull.pop()
        hull.append(point)

    hull.append(hull[0])
    return hull