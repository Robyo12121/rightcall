import configparser
from pathlib import Path
import os
import logging


class Configure():
    """A base class object for managing parsing
    config files"""
    def __init__(self, header, data, debug=False):
        self.dir = Path(os.environ.get('HOME')) / '.rightcall'
        self.file = self.dir / 'config.ini'
        self.header = header
        self.data = data
        self.parser = configparser.ConfigParser()
        if debug:
            level = 'DEBUG'
        else:
            level = 'INFO'
        self.logger = logging.getLogger()
        self.logger.setLevel(level=level)

    def __str__(self):
        return f"DIR: {self.dir}, FILE: {self.file}, HEADER: {self.header}, DATA: {self.data}"

    def set(self, some_dict):
        data = {k: some_dict[k] for k in self.data}
        self.write_file(data, self.file)

    def write_file(self, data, file):
        self.parser[self.header] = data
        with open(file, 'w') as data:
            self.parser.write(data)
        return

    def get(self, file):
        self.parser.read(self.file)
        try:
            data = dict(self.parser[self.header])
        except KeyError:
            return None
        # for key in self.parser[self.header]:
        #     data[key] = self.parser[self.header][key]
        return data

    def ensure_exists(self):
        if not self.dir.exists():
            self.dir.mkdir()
        if not self.file.exists():
            self.parser[self.header] = {k: '' for k in self.data}
            with open(str(self.file), 'w') as file:
                self.parser.write(file)
        if self.file.exists():
            config = self.get(self.file)
            if config is not None:
                for item in self.data:
                    if item not in config:
                        self.parser[self.header][item] = ''
                with open(str(self.file), 'w') as file:
                    self.parser.write(file)

    def get_user_input(self):
        user_input = {}
        current = self.get(self.file)
        if current is None:
            for item in self.data:
                user_input[item] = str(input(f"{item} []: "))
            for k, v in user_input.items():
                if not v:
                    user_input[k] = None
        else:
            for item in self.data:
                user_input[item] = str(input(f"{item} [{current[item]}]: "))
            for k, v in user_input.items():
                if v is '':
                    user_input[k] = current[k]
        return user_input

    def no_change(self, some_dict):
        for k, v in some_dict.items():
            if v is not '':
                return False
        return True

    def run(self):
        self.ensure_exists()
        user_input = self.get_user_input()
        if not self.no_change(user_input):
            self.set(user_input)


if __name__ == '__main__':
    conf = Configure('aws', ('host', 'index', 'region'))
    print(conf)
    conf.run()
