import random
import math

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

def fitness(points):
    return sum(dist(points[i - 1], points[i]) for i in range(1, len(points)))

def dist(p1, p2):
    return math.hypot(p1.x - p2.x, p1.y - p2.y)

def solve(points):
    population = init_population(points, 100)
    for _ in range(20):
        population = evolve(population)
    return best_route(population)

def init_population(points, population_size):
    return [random.sample(points, len(points)) for _ in range(population_size)]

def evolve(population):
    population.sort(key=fitness)
    population = population[:len(population) // 2]
    while len(population) < 100:
        parents = random.sample(population, 2)
        child = crossover(*parents)
        population.append(child)
    return population

def crossover(parent1, parent2):
    n = len(parent1)
    c1, c2 = sorted(random.sample(range(n), 2))
    return parent1[:c1] + parent2[c1:c2] + parent1[c2:]

def best_route(population):
    return min(population, key=fitness)
