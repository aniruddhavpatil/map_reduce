# from rpc import Server.Server, Client.Client    

from rpc.Server import Server
from rpc.Client import Client
import multiprocessing as mp
import time
# import rpc.Client.Client as Client

class Leader:
    def __init__(self, networkConfig, methods):
        self.server = Server(networkConfig, [self.start_mapper, self.stop_mapper, *methods])
        self.mapper_count = 0
        self.reducer_count = 0

    def start_mapper(self):
        print('Start Mapper')
        self.mapper_count += 1

    def stop_mapper(self, id):
        print('Stopping Mapper')
        self.mapper_count -= 1

    def run(self):
        # self.server.run()
        self.server_process = mp.Process(target=self.server.run)
        print('Starting Leader')
        self.server_process.start()
        # server_process.join()
        # print('Stopping Master')

    def stop(self):
        print('Stopping Leader')
        self.server_process.terminate()
        self.server_process.join()
        self.server_process.close()

def myFunc(i):
    print(i)

def main():
    leader = Leader(('localhost', 8000), [myFunc])
    leader.run()
    mapper = Client(('localhost', 8000))

    processes = []
    for i in range(5):
        processes.append(mp.Process(target=mapper.run, args=('myFunc', 'mapper:' +str(i),)))
    # print('Starting Mapper')
    # mapperProcess.start()
    for i,p in enumerate(processes):
        print('Starting process:',i)
        p.start()
        time.sleep(1)

    time.sleep(2)
    for i,p in enumerate(processes):
        print('Stopping process:', i)
        p.terminate()
        p.join()
        p.close()
    # print('Stopping Mapper')
    # time.sleep(1)
    leader.stop()
    
if __name__ == "__main__":
    main()
