import json
from Master import Master
import importlib


def get_network_config(config):
    address, port = config
    return (address, port)

def get_map_method(location):
    module = importlib.import_module(location)
    method = getattr(module, 'map_fn')
    return method


def get_reduce_method(location):
    module = importlib.import_module(location)
    method = getattr(module, 'reduce_fn')
    return method

if __name__ == '__main__':
    f = open('config.json', 'r')
    config = json.loads(f.read())
    mode = 'default'
    config = config[mode]
    network_config = get_network_config(config['network_config'])
    input_data = config['input_data']
    mapper_count = config['n_mappers']
    reducer_count = config['n_reducers']
    output_data = config['output_data']
    map_fn = get_map_method(config['map_fn'])
    reduce_fn = get_reduce_method(config['reduce_fn'])

    master = Master(network_config, [], mapper_count, reducer_count, map_fn, reduce_fn, input_data, output_data)
    master.run()
