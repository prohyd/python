import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def CreateFile(count):
    for i in range(count):
        table = pd.DataFrame(columns=["syml", "value"])
        for j in range(100):
            table.loc[len(table)] = {
                "syml": np.random.choice(['A','B','C','D']),
                "value": np.random.uniform(0,100)
            }
        table.to_csv("data" + str(i+1)+".csv",index=False)
        del table

def Task(file):
    table = pd.read_csv(file)
    table_answer = pd.DataFrame(columns=["syml", "median","std"])
    for i in ['A','B','C','D']:
        table_syml = table[table["syml"]==i]
        table_answer.loc[len(table_answer)] = {
            "syml": i,
            "median": np.median(table_syml["value"].to_numpy()),
            "std": np.std(table_syml["value"].to_numpy())
        }
    return table_answer

def GetAnwer(arr):
    table = pd.concat([arr[0],arr[1],arr[2],arr[3],arr[4]],ignore_index=True)
    table_answer = pd.DataFrame(columns=["syml", "median","std"])
    for i in ["A","B","C","D"]:
        table_syml = table[table["syml"]==i]
        table_answer.loc[len(table_answer)] = {
            "syml": i,
            "median": np.median(table_syml["median"].to_numpy()),
            "std": np.std(table_syml["median"].to_numpy())
        }
    return table_answer


def main():
    CreateFile(5)
    files = ["data1.csv","data2.csv","data3.csv","data4.csv","data5.csv"]
    with ThreadPoolExecutor(max_workers=5) as ex:
        answer = list(ex.map(Task, files))
    table= GetAnwer(answer)
    print(table)

if __name__=='__main__':
    main()