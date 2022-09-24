import apk2java
import os
import numpy as np
from multiprocessing import  Pool

def parallelize_dataframe(file_list, func, n_cores=15):
    decompiled_apks = []
    df_split = np.array_split(np.array(file_list), n_cores)
    pool = Pool(n_cores)
    decompiled_apks.extend(pool.map(func, df_split))
    pool.close()
    pool.join()

    with open('decompiled_files_test2.txt', 'w') as f:
        for item in decompiled_apks:
            for i in item:
                f.write("%s\n" % i)

def file_to_list(fileName):
    with open(fileName, "r") as file:
        files_list = [str(f).rstrip("\n") for f in file]
    return files_list


def decompile(apks_list):
    decompiled_files = []

    for f in apks_list:
        try:
            # command: apk2java example.apk ./decompiled_apks/
            command = "apk2java "+ "'" + f + "' " + "'./decompiled_apks/'"
            os.system(command)
            decompiled_files.append(f)
        except Exception as e:
            raise

    return decompiled_files

if __name__ == '__main__':
    apk_list = "all_apks.list"
    files = file_to_list(apk_list)
    parallelize_dataframe(files[2500:3500], decompile, 40)
