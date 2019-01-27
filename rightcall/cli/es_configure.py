import configparser
from pathlib import Path
import os
import logging


class ESConfigure():
    def __init__(self):
        self.home = Path(os.environ.get('HOME'))
        self.config_file = self.home / '.rightcall'
        self.parser = configparser.ConfigParser()
        self.files = {'config': self.config_file / 'config.ini'}
        self.data = ('host', 'index', 'region')
        logging.basicConfig(level='DEBUG')
        self.logger = logging.getLogger()

    def set_info(self, some_dict):
        data = {k: some_dict[k] for k in self.data}
        self.write_file(data, self.files['config'])

    def write_file(self, data_dict, file):
        self.parser['default'] = data_dict
        with open(file, 'w') as data:
            self.parser.write(data)
        return

    def get_data(self, file):
        self.parser.read(self.config_file)
        data = {}
        try:
            for key in self.parser['default']:
                data[key] = self.parser['default'][key]
        except KeyError:
            self.logger.error("Key error")
        return data

    def settings_exist(self):
        if not self.config_file.exists():
            self.config_file.mkdir()
        if not self.files['config'].exists():
            open(str(self.files['config']), 'w').close()

    def get_user_input(self):
        user_input = {}
        current = self.get_data(self.files['config'])
        self.logger.debug(f"Current: {current}")
        for item in self.data:
            user_input[item] = str(input(f"{item} [{current[item]}]: "))
            self.logger.debug(f"KEY: {item} VALUE : {user_input[item]} TYPE: {type(user_input[item])}")
        self.logger.debug(f"User input: {user_input}")

        # If no user input set it to current setting (dont change)
        user_input = {}
        for k, v in user_input.items():
            if v is '':
                user_input[k] = current[k]
        return user_input

    def no_input(self, somedict):
        for k in somedict:
            if k is not '':
                return False
        return True

    def run(self):
        self.settings_exist()
        user_input = self.get_user_input()
        if not self.no_input(user_input.values()):
            self.set_info(user_input)


if __name__ == '__main__':
    # logging.basicConfig(level='DEBUG')
    # logger = logging.getLogger()
    conf = ESConfigure()
    print(conf.home)
    conf.run()
