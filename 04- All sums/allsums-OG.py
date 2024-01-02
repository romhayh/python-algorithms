def allSumsDP(values):
    num_values = len(values)
    total_sum = sum(values)
    can_sum = [False] * (total_sum + 1)
    can_sum[0] = True

    for i in range(num_values):
        for j in range(total_sum, values[i] - 1, -1):
            can_sum[j] |= can_sum[j - values[i]]

    all_sums = set()
    for i in range(total_sum + 1):
        if can_sum[i]:
            all_sums.add(i)

    return all_sums