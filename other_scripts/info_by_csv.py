import pandas as pd
from os import listdir
from rightcall_local import parse_csv

path = r"C:\Users\RSTAUNTO\Downloads/"


def find_csv_filenames(path, suffix='.csv'):
    filenames = listdir(path)
    return [filename for filename in filenames if filename.endswith(suffix)]


def save_proper_name(sourcepath, targetpath, file):
    if type(sourcepath) or type(targetpath) or type(file) is not str:
        raise TypeError(f"Incorrect type: {type(sourcepath)}, {type(targetpath)}, {type(file)}")
    json_data = parse_csv(path + file)
    df = pd.DataFrame.from_dict(json_data)
    df.to_csv(targetpath, sep=';', index=False)


if __name__ == '__main__':
    files = find_csv_filenames(path)
    for file in files:
        if file.startswith('odigo'):
            ref = parse_csv(path + str(file))
            print(f"{file} : {ref[0]['Name']}")
