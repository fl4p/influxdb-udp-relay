import json

import yaml

class _YamlIncludeDummy(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = '!include'
    def __init__(self, val):
        self.val = val

    @classmethod
    def from_yaml(cls, loader, node):
        return cls(node.value)

def read_hass_configuration_yaml():
    files  = ['/config/configuration.yaml', 'configuration.yaml']
    for fn in files:
        try:
            with open(fn, "r") as fp:
                return yaml.safe_load(fp)
                #return yaml.load(fp, Loader=yaml.Loader)
        except FileNotFoundError:
            pass


def read_user_options():
    files = ['/data/options.json', 'options.json']
    for fn in files:
        try:
            with open(fn, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            pass

if __name__ == "__main__":
    conf = read_hass_configuration_yaml()
    print(conf)

