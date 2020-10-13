# from rpc import Server.Server, Client.Client    

from rpc.Server import Server
import multiprocessing as mp
import time
import os
# import rpc.Client.Client as Client
from filesystem.Client import Client as FS_client
from Worker import Worker
import importlib


class Master:
    def __init__(self, networkConfig, methods, n_mappers, n_reducers, map_fn, reduce_fn, input_data, output_data):
        self.server = Server(networkConfig, [self.start_mapper, self.stop_mapper, self.stop_reducer, self.start_reducer, *methods])
        self.mapper_count = 0
        self.reducer_count = 0
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers

        self.input_data = input_data
        self.output_data = output_data

        self.map_fn = map_fn
        self.reduce_fn = reduce_fn
        self.networkConfig = networkConfig
        self.base_dir = os.getcwd()
        self.fs_client = FS_client()
        self.fs_client.connect()
        self.file_dict = {}
        self.mappers = []
        self.reducers = []


    def input_partition(self, files, n):
        for f in files:
            with open(os.path.join(self.base_dir, f), 'r') as curr_file:
                data=curr_file.read()
                total_size = len(data)
                split_size = total_size // n
                chunked_size = 0
                chunk = 0
                while chunk < n:
                    key = chunk
                    chunk_name = 'split_' + str(key) + '_' + f
                    if chunk < n - 1:
                        chunked_size = (chunk + 1) * split_size
                        # print(chunk_name)
                        self.fs_client.set(chunk_name,data[chunk * split_size :  chunked_size])
                    else:
                        # print(chunk_name)
                        self.fs_client.set(chunk_name, data[chunk * split_size: ])
                    chunk+=1
                    if not key in self.file_dict:
                        self.file_dict[key] = [chunk_name]
                    else:
                        self.file_list[key].append(chunk_name)
        return self.file_dict




    def start_mapper(self):
        self.mapper_count += 1

    def stop_mapper(self, id):
        self.mapper_count -= 1
        # print('Mapper:', id, 'finished job')
    
    def start_reducer(self):
        self.reducer_count += 1

    def stop_reducer(self, id):
        self.reducer_count -= 1
        # print('Reducer:', id, 'finished job')
        

    def run(self):
        # self.server.run()
        try:
            self.server_process = mp.Process(target=self.server.run)
            print('Starting Master')
            self.server_process.start()
            D = self.input_partition([self.input_data], self.n_mappers)
            print('Starting mappers')
            for i in range(self.n_mappers):
                worker = Worker(self.networkConfig, i, 'map', self.n_mappers, D[i], 'output', self.map_fn)
                self.mappers.append(mp.Process(target=worker.run))

            for i,w in enumerate(self.mappers):
                print('Mapper:', i, 'started')
                w.start()

            for i,w in enumerate(self.mappers):
                print('Mapper:', i, 'completed')
                w.join()

            print('Starting reducers')
            for i in range(self.n_reducers):
                worker = Worker(self.networkConfig, i, 'reduce',
                                self.n_reducers, None, self.output_data, self.reduce_fn)
                self.reducers.append(mp.Process(target=worker.run))

            for i, w in enumerate(self.reducers):
                print('Reducer:', i, 'started')
                w.start()

            for i,w in enumerate(self.reducers):
                print('Reducer:', i, 'completed')
                w.join()


        except KeyboardInterrupt:
            self.stop()
        self.stop()


        

    def stop(self):
        print('Stopping Master')
        
        for i,p in enumerate(self.mappers):
            try:
                p.terminate()
                p.join()
                p.close()
            except:
                pass
        
        for i,p in enumerate(self.reducers):
            try:
                p.terminate()
                p.join()
                p.close()
            except:
                pass

        try:
            self.server.kill()
            self.server_process.terminate()
            self.server_process.join()
            self.server_process.close()
        except:
            pass

def myFunc(i):
    print(i)

def main():
    module = importlib.import_module('word_count_map')
    map_fn = getattr(module, 'map_fn')
    module = importlib.import_module('word_count_reduce')
    reduce_fn = getattr(module, 'reduce_fn')
    master = Master(('localhost', 8000), [myFunc], 4, 4, map_fn, reduce_fn)
    master.run()

if __name__ == "__main__":
    main()
