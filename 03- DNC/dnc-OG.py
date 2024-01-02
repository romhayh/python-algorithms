
def calculateFunction(arr, baseFunction, combineFunction):
    half = len(arr)//2
    leftHalf = arr[half:]
    rightHalf = arr[:half]
    if len(arr) == 1:
        return baseFunction(arr)[0]
    elif len(arr) > 1:
        return combineFunction(calculateFunction(leftHalf, baseFunction,combineFunction), calculateFunction(rightHalf,baseFunction, combineFunction))


def dnc(baseFunction, combineFunction):
    return lambda arr: calculateFunction(arr, baseFunction, combineFunction)



def maxAreaHist(hist):
    stack = []
    maxArea = 0
    index = 0
    while index < len(hist):
        if (not stack) or (hist[stack[-1]] <= hist[index]):
            stack.append(index)
            index += 1
        else:
            stackTop = stack.pop()
            area = (hist[stackTop] *
                    ((index - stack[-1] - 1)
                     if stack else index))
            maxArea = max(maxArea, area)
    while stack:
        stackTop = stack.pop()
        area = (hist[stackTop] *
                ((index - stack[-1] - 1)
                 if stack else index))
        maxArea = max(maxArea, area)
    return maxArea