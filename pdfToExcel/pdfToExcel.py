import multiprocessing
import time
import pdfplumber
from tqdm import tqdm
import pandas as pd
import os

pdf = pdfplumber.open('NOTICE COPY - NEETPG 2022.pdf')
path = "./metadata"
pool_size = 10
tasks = range(len(pdf.pages) - 1)


def create_csv(i):
    worker = multiprocessing.current_process().name[-1]
    global_list = []
    temp_list = []
    text = pdf.pages[i].extract_table()
    for t in range(len(text)):
        for l in range(len(text[t])):
            temp_list.append(text[t][l])
            if (l+1) % 3 == 0:
                global_list.append(temp_list)
                temp_list = []
    df = pd.DataFrame(global_list, columns=['roll', 'number', 'rank'])
    df.to_csv("{0}/temp_df_{1}.csv".format(path, worker),
              mode='a', index=False)


def main():
    tic = time.time()
    if os.path.isdir(path):
        for file_name in os.listdir(path):
            file = path + "/" + file_name
            if os.path.isfile(file):
                print('Deleting file:', file)
                os.remove(file)
    else:
        os.mkdir(path)
    with multiprocessing.Pool(pool_size) as pool:
        for _ in tqdm(pool.imap_unordered(create_csv, tasks), total=len(tasks)):
            pass

    for file_name in os.listdir(path):
        file = path + "/" + file_name
        df = pd.read_csv(file, index_col=False)
        df.to_csv("{}/output.csv".format(path),
                  mode='a', index=False)
        os.remove(file)

    toc = time.time()

    print('Done in {:.4f} seconds'.format(toc-tic))


if __name__ == '__main__':
    main()
