import os
import json


def add_nested_key(k, target_dict):
    kl = k.split(".")
    if kl[0] not in target_dict and len(kl) > 1:
        target_dict[kl[0]] = dict()
    if len(kl) > 2:
        add_nested_key(".".join(kl[1:]),target_dict[kl[0]])

        
def get_nested_key(k, source_dict):
    try:
        kl = k.split(".")
        if len(kl) == 1:
            return source_dict[kl[0]]
        else:
            return get_nested_key(".".join(kl[1:]),source_dict[kl[0]])
    except Exception as e:
        raise Exception(f"Failed retrieving {k} in {source_dict} : {e}")

    
def set_nested_key(k, v, target_dict):
    try:
        kl = k.split(".")
        if len(kl) == 1:
            target_dict[kl[0]] = v
        else:
            set_nested_key(".".join(kl[1:]),v, target_dict[kl[0]])
    except Exception as e:
        raise Exception(f"Failed setting {k} in {target_dict} : {e}")

            
def get_default_parameter():

    list_params_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                       "../auxdir/default_list.json")
    default_params_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                       "../auxdir/parameters_sgq.json")
    with open(list_params_path) as f:
#        print(f.read())
        list_params = json.load(f)

    with open(default_params_path) as f:
        default_params = json.load(f)

    reduced_default_params = dict()
    for k in list_params:
        add_nested_key(k,reduced_default_params)
        set_nested_key(k,
                       get_nested_key(k, default_params),
                       reduced_default_params)

    return reduced_default_params
