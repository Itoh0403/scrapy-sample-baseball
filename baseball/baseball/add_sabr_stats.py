import os

import pandas as pd
import mysql.connector
from tqdm import tqdm

import mySabr as ms


UPDATE_BATTER_SABR = """
    update batter set 
    update_date = now(),
    isPitcher = %s,
    wOBA = %s,
    wRAA = %s,
    wRC = %s,
    wRCP = %s,
    babip = %s 
    where name =%s and year=%s
    """

ALTER_TABLE = """
    ALTER TABLE BATTER ADD (
    isPitcher integer, 
    wOBA real, 
    wRAA real, 
    wRC real, 
    wRCP real, 
    babip real
    );
"""

if __name__ == "__main__":
    config = {"user":os.environ.get("MYSQL_USER"),
    "password":os.environ.get("MYSQL_PASSWORD"), "host":"localhost", "database":"baseball_2020"}
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    df_batter = pd.read_sql(sql="SELECT * FROM batter;", con=conn, index_col='id' )
    df_pitcher = pd.read_sql(sql="SELECT * FROM pitcher;", con=conn, index_col='id' )

    df_batter = ms.df_woba(df_batter)
    df_batter_sabr, sr_central, sr_pacific = ms.calculate_sabr(df_batter, df_pitcher, 2020)

    if "wRC" not in df_batter.columns:
        cursor.execute(ALTER_TABLE)
        print(cursor.statement)

    for index in tqdm(df_batter_sabr.dropna().index):
        m = df_batter_sabr.loc[index]
        cursor.execute(UPDATE_BATTER_SABR, (int(m["isPitcher"]), m["wOBA"], m["wRAA"], m["wRC"],
                        m["wRCP"], m["babip"], m["name"], int(m["year"])))
    conn.commit()
    conn.close()