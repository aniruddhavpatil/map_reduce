from filesystem.Client import Client as FS_client
from utils import hash_function

class Worker:

    def __init__(self, task_id, task_type, n_tasks, files, output_location, function):
        self.task_id = task_id
        self.task_type = task_type
        self.n_tasks = n_tasks
        self.files = files
        self.function = function
        self.output_location = output_location
        self.fs_client = FS_client()
        self.fs_client.connect()

    def run(self):
        # print(self.task_id, self.task_type)
        # print(self.files)
        # print(self.function('lambda ' + str(self.task_id)))
        if(self.task_type == 'map'):
            self.map()
        elif(self.task_type == 'reduce'):
            self.reduce()

    def emit_intermediate(self, key, value):
        hash_value = hash_function(key, self.n_tasks)
        store_key = 'intermediate_' + str(hash_value)
        store_value = str(key) + ':' + str(value)
        self.fs_client.append(store_key, store_value)

    def emit(self, key, value):
        store_key = self.output_location
        store_value = str(key) + ':' + str(value)
        self.fs_client.append(store_key, store_value)

    def map(self):
        for f in self.files:
            data = self.fs_client.get(f)
            processed_data = self.function(f, data)
            for k,v in processed_data:
                self.emit_intermediate(k,v)

    def reduce(self):
        data = self.fs_client.get('intermediate_' + str(self.task_id))
        data = data.split('\n')
        tuple_data = []
        for t in data:
            # print(t)
            try:
                k,v = t.split(':')
                tuple_data.append((k,v))
            except ValueError:
                pass
        print('Processing data')
        processed_data = self.function(tuple_data)
        print('Emitting data')
        for k, v in processed_data:
            self.emit(k, v)

if __name__ == "__main__":
    mapper = Worker('id', 'ip', 'op', 'func')
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
