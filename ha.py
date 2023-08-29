import json

import yaml

# these custom tags secretly register with the loader (yaml.SafeLoader)
class _YamlIncludeDummy(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = '!include'
    def __init__(self, val):
        self.val = val

    @classmethod
    def from_yaml(cls, loader, node):
        return cls(node.value)

class _YamlIncludeDirMergeNamedDummy(yaml.YAMLObject):
    yaml_loader = yaml.SafeLoader
    yaml_tag = '!include_dir_merge_named'
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
        except FileNotFoundError as e:
            pass
    raise FileNotFoundError('none of these files found %s' % files)


def read_user_options():
    files = ['/data/options.json', 'options.json']
    for fn in files:
        try:
            with open(fn, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            pass
    raise FileNotFoundError('none of these files found %s' % files)

if __name__ == "__main__":
    conf = read_hass_configuration_yaml()
    print(conf)

