# from rpc import Server.Server, Client.Client    

from rpc.Server import Server
from rpc.Client import Client
import multiprocessing as mp
import time
import os
# import rpc.Client.Client as Client
from filesystem.Client import Client as FS_client
from Worker import Worker

class Master:
    def __init__(self, networkConfig, methods):
        self.server = Server(networkConfig, [self.start_mapper, self.stop_mapper, *methods])
        self.mapper_count = 0
        self.reducer_count = 0
        self.base_dir = os.getcwd()
        self.fs_client = FS_client()
        self.fs_client.connect()
        self.file_dict = {}


    def input_partition(self, files, n):
        # HAX
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
        # total_size = 0
        # for f in files:
        #     total_size+=os.path.getsize(os.path.join(self.base_dir, f))
        
        # split_size = total_size // n
        # curr = 0
        # curr_file = 0
        # curr_file_size = os.path.getsize(os.path.join(self.base_dir, files[curr_file]))
        # while curr_file < len(files):
        return True




    def start_mapper(self):
        print('Start Mapper')
        self.mapper_count += 1

    def stop_mapper(self, id):
        print('Stopping Mapper')
        self.mapper_count -= 1

    def run(self):
        # self.server.run()
        self.server_process = mp.Process(target=self.server.run)
        print('Starting Master')
        self.server_process.start()
        # server_process.join()
        # print('Stopping Master')

    def stop(self):
        print('Stopping Master')
        self.server_process.terminate()
        self.server_process.join()
        self.server_process.close()

def myFunc(i):
    print(i)

def map_func(key, value):
    ret = []
    for w in value.split():
        ret.append((w.strip(':'), 1))
    return ret

def red_func(tuple_data):
    D = {}
    for k, v in tuple_data:
        if k not in D:
            D[k]=1
        else:
            D[k]+=1
    return tuple(list(zip(D.keys(), D.values())))

def IE_map(key, value):
    ret = []
    for w in value.split():
        ret.append((w.strip(':'), key))
    return ret

def IE_red(tuple_data):
    D = {}
    for k,v in tuple_data:
        if k not in D:
            D[k] = {}
            D[k][v] = 1
        else:
            if v not in D[k]:
                D[k][v] = 1
            else:
                D[k][v]+=1
    for k in D:
        curr_list = list(zip(D[k].keys(), D[k].values()))
        curr_list.sort(key=lambda x: x[1], reverse=True)
        D[k] = curr_list

    ret = []
    for item in list(zip(D.keys(), D.values())):
        print(item)
        value_string = ''
        for value in item[1]:
            value_string+=value[0] + ' ' + str(value[1]) + ','
        ret.append((item[0], value_string[:-1]))
    return ret


def driver_reducer():
    master = Master(('localhost', 8000), [myFunc])
    master.run()
    # mapper = Client(('localhost', 8000))
    # D = master.input_partition(['corpus_utf.txt'], 4)
    # print(D)
    workers = []
    for i in range(4):
        worker = Worker(i, 'reduce', 4, None, 'output', IE_red)
        # workers.append(worker)
        worker.run()
    master.stop()

def driver_mapper():
    master = Master(('localhost', 8000), [myFunc])
    master.run()
    mapper = Client(('localhost', 8000))
    D = master.input_partition(['corpus_utf.txt'], 4)
    print(D)
    workers = []
    for i in range(4):
        worker = Worker(i, 'map', 4, D[i], 'output', IE_map)
        # workers.append(worker)
        worker.run()

    # processes = []
    # for i in range(5):
    #     processes.append(mp.Process(target=mapper.run,
    #                                 args=('myFunc', 'mapper:' + str(i),)))
    # # print('Starting Mapper')
    # # mapperProcess.start()
    # for i, p in enumerate(processes):
    #     print('Starting process:', i)
    #     p.start()
    #     time.sleep(1)

    # time.sleep(2)
    # for i, p in enumerate(processes):
    #     print('Stopping process:', i)
    #     p.terminate()
    #     p.join()
    #     p.close()
    # print('Stopping Mapper')
    # time.sleep(1)
    master.stop()

def main():
    # driver_mapper()
    driver_reducer()
    # fs_client = FS_client()
    # fs_client.connect()
    # corpus = open('corpus_utf.txt', 'r')
    # corpus_text = corpus.read()
    # fs_client.set('corpus', corpus_text[:100])
    # value = fs_client.get('corpus')
    # print(value == corpus_text[:100])
    

if __name__ == "__main__":
    main()
