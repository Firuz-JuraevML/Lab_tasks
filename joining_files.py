from os import listdir
import os
import shutil
import sys


def list_files(folder):
    files = []
    counter = 0
    for f in listdir(f"./{folder}/"):
        if f.endswith('.' + "gz"):
            files.append(f)
            counter = counter + 1
    print(f"GZ Files: {str(counter)}")
    return files


def move_files(folder):
    files = list_files(folder)
    for f in files:
        shutil.move(f"./{folder}/{f}", "./")

    print ("Moving files is completed successfully!")


if __name__ == '__main__':
    for i in range(1, int(sys.argv[1]) + 1):
        print(f"Folder: {str(i)}")
        move_files(str(i))
