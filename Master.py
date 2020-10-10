# from rpc import Server.Server, Client.Client    

from rpc.Server import Server
from rpc.Client import Client
import multiprocessing as mp
import time
# import rpc.Client.Client as Client

class Master:
    def __init__(self, networkConfig, methods):
        self.server = Server(networkConfig, [self.startMapper, self.stopMapper, *methods])
        self.mapperCount = 0
        self.reducerCount = 0

    def startMapper(self):
        print('Start Mapper')
        self.mapperCount += 1

    def stopMapper(self, id):
        print('Stopping Mapper')
        self.mapperCount -= 1

    def run(self):
        # self.server.run()
        self.serverProcess = mp.Process(target=self.server.run)
        print('Starting Master')
        self.serverProcess.start()
        # serverProcess.join()
        # print('Stopping Master')

    def stop(self):
        print('Stopping Master')
        self.serverProcess.terminate()
        self.serverProcess.join()
        self.serverProcess.close()

def myFunc(i):
    print(i)

def main():
    master = Master(('localhost', 8000), [myFunc])
    master.run()
    mapper = Client(('localhost', 8000))

    processes = []
    for i in range(5):
        processes.append(mp.Process(target=mapper.run, args=('myFunc', i,)))
    # print('Starting Mapper')
    # mapperProcess.start()
    for i,p in enumerate(processes):
        print('Starting process:',i)
        time.sleep(1)
        p.start()

    time.sleep(5)
    for i,p in enumerate(processes):
        print('Stopping process:', i)
        p.terminate()
        p.join()
        p.close()
    # print('Stopping Mapper')
    # time.sleep(1)
    master.stop()
    
if __name__ == "__main__":
    main()
