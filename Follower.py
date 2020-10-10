class Follower:

    def __init__(self, name, input_path, output_path, function):
        self.name = name
        self.input_path = input_path
        self.output_path = output_path
        self.function = function

    def run(self):
        print(self.name)

if __name__ == "__main__":
    mapper = Follower('name', 'ip', 'op', 'func')
    mapper.run()



# class Shape(metaclass=abc.ABCMeta):
#     @abc.abstractmethod
#     def area(self):
#         pass


# class Rectangle(Shape):
#     def __init__(self, x, y):
#         self.l = x
#         self.b = y

#     def area(self):
#         return self.l*self.b


# r = Rectangle(10, 20)
# print('area: ', r.area())
