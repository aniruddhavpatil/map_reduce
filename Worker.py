from filesystem.Client import Client as FS_client
from utils import hash_function
from rpc.Client import Client

class Worker:

    def __init__(self, networkConfig,task_id, task_type, n_tasks, files, output_location, function):
        self.networkConfig = networkConfig
        self.task_id = task_id
        self.task_type = task_type
        self.n_tasks = n_tasks
        self.files = files
        self.function = function
        self.output_location = output_location
        self.fs_client = FS_client()
        self.fs_client.connect()
        self.rpc = Client(networkConfig)

    def run(self):
        if(self.task_type == 'map'):
            self.map()
        elif(self.task_type == 'reduce'):
            self.reduce()
        return

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
        self.rpc.run('start_mapper')
        for f in self.files:
            data = self.fs_client.get(f)
            processed_data = self.function(f, data)
            for i, (k,v) in enumerate(processed_data):
                print(self.task_type + str(self.task_id) + ':' + 'Emitting intermediate data', i+1, 'of', len(processed_data))

                self.emit_intermediate(k,v)
        self.rpc.run('stop_mapper', self.task_id)
        

    def reduce(self):
        self.rpc.run('start_reducer')
        data = self.fs_client.get('intermediate_' + str(self.task_id))
        data = data.split('\n')
        tuple_data = []
        for t in data:
            try:
                k,v = t.split(':')
                tuple_data.append((k,v))
            except ValueError:
                pass
        processed_data = self.function(tuple_data)
        for i, (k, v) in enumerate(processed_data):
            self.emit(k, v)
            print(self.task_type + str(self.task_id) + ':' + 'Emitting data', i+1, 'of', len(processed_data))
        self.rpc.run('stop_reducer', self.task_id)

if __name__ == "__main__":
    mapper = Worker('id', 'ip', 'op', 'func')
    mapper.run()