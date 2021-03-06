import pandas as pd
import glob
from os import listdir


def list_files():
    files = []
    counter = 0
    for f in listdir("./"):
        if f.endswith('.' + "csv"):
            files.append(f)
            counter = counter + 1
    print ("CSV Files: " + str(counter))
    return files


if __name__ == '__main__':
    li = []

    all_files = list_files()

    for filename in all_files:
        filepath = "./" + filename
        df = pd.read_csv( filepath, index_col=None, header=0)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)

    frame.to_csv("coronovirus.csv")
