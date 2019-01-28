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
        self.ensure_exists()

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
        return data

    def ensure_exists(self):
        # Ensure directory '.rightcall' exists
        if not self.dir.exists():
            self.logger.debug("Making directory")
            self.dir.mkdir()
        # Ensure file 'config.ini' exists
        if not self.file.exists():
            self.logger.debug("Making file")
            self.parser[self.header] = {k: '' for k in self.data}
            self.logger.debug(f"Writing {dict(self.parser[self.header])} to file")
            with open(str(self.file), 'w') as file:
                self.parser.write(file)
        if self.file.exists():
            self.logger.debug(f"{self.file} exists. Retrieving info for {self.header}")
            config = self.get(self.file)
            self.logger.debug(f"Found {config}")
            # Ensure header is present in file
            if config is not None:
                self.logger.debug(f"Checking if all fields present in config")
                for item in self.data:
                    if item not in config:
                        self.logger.debug(f"{item} missing from config. Writing.")
                        self.parser[self.header][item] = 'None'
                with open(str(self.file), 'w') as file:
                    self.parser.write(file)
            else:
                self.logger.debug(f"Nothing found for {self.header} in config.ini")
                self.logger.debug("Writing blank values")
                data = {k: 'None' for k in self.data}
                self.write_file(data, self.file)

    def get_user_input(self):
        user_input = {}
        current = self.get(self.file)
        print("Type 'None' to clear value value")
        if current is None:
            for item in self.data:
                user_input[item] = str(input(f"{item} []: "))
                # user_input = {k: '' if v is None else current[k] for k, v in input_dict.items()}
            for k, v in user_input.items():
                if not v:
                    user_input[k] = 'None'
        else:
            for item in self.data:
                user_input[item] = str(input(f"{item} [{current[item]}]: "))
            # result = {k: '' if v is None else current[k] for k, v in user_input.items()}
            for k, v in user_input.items():
                if v is '':
                    user_input[k] = current[k]
        return user_input

    def no_change(self, some_dict):
        for k, v in some_dict.items():
            if v is not '':
                return False
        return True

    def if_no_input(self, input_dict, current_dict):
        result = {k: '' if v is None else current_dict[k] for k, v in input_dict.items()}
        return result

    def run(self):
        self.ensure_exists()
        user_input = self.get_user_input()
        self.logger.debug(f"User Input: {user_input}")
        if not self.no_change(user_input):
            self.set(user_input)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    conf = Configure('dynamodb', ('region', 'table', 'endpoint'), debug=True)
    conf.run()
    logging.info(conf)
