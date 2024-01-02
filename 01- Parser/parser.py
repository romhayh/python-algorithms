from numpy import double
from abc import ABC, abstractmethod
import queue


class Expression(ABC):
    @abstractmethod
    def calc(self) -> double:
        pass


# implement the classes here
class Num(Expression):
    def __init__(self, number):
        self.number = number

    def calc(self) -> double:
        return self.number


class BinExp(Expression):
    def __init__(self, left: Num, right: Num):
        self.left = left
        self.right = right

    def calc(self) -> double:
        pass


class Plus(BinExp):
    def __init__(self, left, right):
        super().__init__(left, right)

    def calc(self) -> float:
        return self.left.calc() + self.right.calc()


class Minus(BinExp):
    def __init__(self, left, right):
        super().__init__(left, right)

    def calc(self) -> float:
        return self.left.calc() - self.right.calc()


class Mul(BinExp):
    def __init__(self, left, right):
        super().__init__(left, right)

    def calc(self) -> float:
        return self.left.calc() * self.right.calc()


class Div(BinExp):
    def __init__(self, left, right):
        super().__init__(left, right)

    def calc(self) -> float:
        if self.right.calc() == 0:
            raise ZeroDivisionError

        return self.left.calc() / self.right.calc()


def is_operator(character):
    if character == '/' or character == '*' or character == '+' or character == '-':
        return True


def calc(operator, left, right):
    operations = {
        '*': Mul(Num(left), Num(right)).calc,
        '/': Div(Num(left), Num(right)).calc,
        '+': Plus(Num(left), Num(right)).calc,
        '-': Minus(Num(left), Num(right)).calc
    }
    return operations[operator]()


def has_higher_precedence(current_operator, stack_operator):
    precedence = {'*': 2, '/': 2, '+': 1, '-': 1}
    return precedence[current_operator] > precedence[stack_operator]


def calc_expresion(expression):
    stack = []

    for i in expression:
        if i.isnumeric() or (i[0] == '-' and len(i) > 1):
            stack.append(i)
        else:
            right_number = stack.pop()
            left_number = stack.pop()
            stack.append(calc(i, int(left_number), int(right_number)))

    return stack.pop()


def get_curr_number(expression, index):
    number = ''
    while index < len(expression) and expression[index].isnumeric():
        number += expression[index]
        index += 1
    return number

def put_operator_in_queue(character, stack, char_queue):
    while stack and is_operator(stack[-1]):
        if not has_higher_precedence(character, stack[-1]):
            char_queue.put(stack.pop())
        else:
            break


# implement the parser function here
def parser(expression) -> double:
    stack = []
    char_queue = queue.Queue()
    curr_index = 0
    need_to_set_as_minus = False
    for index, character in enumerate(expression):

        # Number
        if character.isnumeric() and curr_index <= index:
            number = get_curr_number(expression, index)
            if need_to_set_as_minus:
                char_queue.put(str(0 - int(number)))
                need_to_set_as_minus = False
            else:
                char_queue.put(number)
            curr_index = index + len(number)

        # Char
        elif is_operator(character):
            if character == '-' and not expression[index - 1].isnumeric() and not expression[index - 1] == ')':
                if expression[index + 1].isnumeric():
                    need_to_set_as_minus = True
                else:
                    put_operator_in_queue(character, stack, char_queue)
                    stack.append(character)
            else:
                put_operator_in_queue(character, stack, char_queue)
                stack.append(character)

        # Parenthesis
        elif character == '(':
            stack.append(character)
        elif character == ')':
            while stack[-1] != '(':
                char_queue.put(stack.pop())
            stack.pop()

    while stack and is_operator(stack[-1]):
        char_queue.put(stack.pop())
        
    return calc_expresion(char_queue.queue)