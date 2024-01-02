import random
import math


def fitness(points):
    sum = 0
    for i in range(1, len(points)):
        sum += dist(points[i-1], points[i])
    return sum


def dist(p1, p2):
    return float(math.sqrt((p1.x-p2.x)**2 + (p1.y-p2.y)**2))


def solve(points):
    population = init_population(points, 100)
    for i in range(20):
        population = evolve(population)
    return best_route(population)


def init_population(points, population_size):
    population = []
    for i in range(population_size):
        population.append(random.sample(points, len(points)))
    return population


def evolve(population):
    population.sort(key=lambda x: fitness(x))
    population = population[:50]
    while len(population) < 100:
        parent1 = random.choice(population)
        parent2 = random.choice(population)
        child = crossover(parent1, parent2)
        population.append(child)
    return population


def crossover(parent1, parent2):
    n = len(parent1)
    c1, c2 = sorted(random.sample(range(n), 2))
    child = parent1[:c1] + parent2[c1:c2] + parent1[c2:]
    return child


def best_route(population):
    population.sort(key=lambda x: fitness(x))
    return population[0]