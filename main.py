from pandas_solution import pandas_solution
from sql_solution import sql_solution
import os

from dotenv import load_dotenv

load_dotenv()

username = os.getenv("USER")
password = os.getenv("PASSWORD")

DATABASE_URL = f"postgresql://{username}:{password}@analytics.maximum-auto.ru:15432/data"


def compare_solutions(pandas_res, sql_res):
    pandas_res = pandas_res.sort_values(by=["communication_id", "sessions_date_time"]).reset_index(
        drop=True).sort_index(axis=1)
    sql_res = sql_res.sort_index(axis=1)
    if pandas_res is not None and sql_res is not None:
        if pandas_res.equals(sql_res):
            print("Dataframes are equal")
        else:
            print("Dataframes are NOT equal")
    else:
        print("Ooops... Something goes wrong")


if __name__ == "__main__":
    if username and password:
        pandas_res = pandas_solution(DATABASE_URL, print_res=True)
        sql_res = sql_solution(DATABASE_URL, print_res=True)
        compare_solutions(pandas_res, sql_res)
    else:
        print('Exiting... Config USER and PASSWORD as environment variables to continue')
