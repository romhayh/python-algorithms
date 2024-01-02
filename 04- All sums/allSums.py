def allSumsDP(values):
    max_possible_sum = sum(values)
    possible_sums = set([0])

    for value in values:
        new_sums = set()
        for existing_sum in possible_sums:
            new_sum = existing_sum + value
            if new_sum <= max_possible_sum:
                new_sums.add(new_sum)
        possible_sums.update(new_sums)

    return possible_sums