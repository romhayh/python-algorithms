def dnc(base_function, combine_function):
    def calculate_function(arr):
        if len(arr) == 1:
            return base_function(arr)[0]
        else:
            half = len(arr) // 2
            left_half = arr[:half]
            right_half = arr[half:]
            return combine_function(calculate_function(left_half), calculate_function(right_half))
    
    return lambda arr: calculate_function(arr)



def maxAreaHist(hist):
    def calculate_area(stack, index):
        stack_top = stack.pop()
        width = index if not stack else index - stack[-1] - 1
        return hist[stack_top] * width

    stack, max_area = [], 0

    for index, height in enumerate(hist):
        while stack and hist[stack[-1]] > height:
            max_area = max(max_area, calculate_area(stack, index))
        stack.append(index)

    while stack:
        max_area = max(max_area, calculate_area(stack, len(hist)))

    return max_area
