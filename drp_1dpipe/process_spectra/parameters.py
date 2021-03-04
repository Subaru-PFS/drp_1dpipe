import json
import os

params_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../auxdir/parameters_sgq.json")
with open(params_path, 'r') as f:
    default_parameters = json.load(f)

