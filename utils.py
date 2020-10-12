import hashlib
def hash_function(key, mod):
    return int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16) % (mod if mod > 0 else 1)