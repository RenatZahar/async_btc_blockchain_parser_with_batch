import requests
import pickle
from collections import deque
import numpy as np
from collections import OrderedDict
import os
import re
import gzip
import pprint as pp



current_dir = os.path.dirname(os.path.realpath(__file__))
cache_file_path = os.path.join(current_dir, 'tx_cache.pkl' + '.gz')

if os.path.exists(cache_file_path):
    with gzip.open(cache_file_path, 'rb') as file:
        cache = pickle.load(file)


print(cache)