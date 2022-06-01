import pdfplumber
from tqdm import tqdm
import pandas as pd

pdf = pdfplumber.open('NOTICE COPY - NEETPG 2022.pdf')
df = pd.DataFrame(columns=['roll', 'number', 'rank'])

for i in tqdm(range(len(pdf.pages)-1)):
    currPage = pdf.pages[i]
    text = currPage.extract_table()
    temp_list = []
    for i in range(len(text)):
        for t in range(len(text[i])):
            temp_list.append(text[i][t])
            if (t+1)%3==0:
                a_series = pd.Series(temp_list, index = df.columns)
                df = df.append(a_series, ignore_index=True)
                temp_list = []
