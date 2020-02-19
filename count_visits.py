from sshtunnel import SSHTunnelForwarder
from concurrent.futures import ProcessPoolExecutor

import pymysql
import pandas as pd
import datetime
import numpy as np

from visits import Visits

def read_from_db(query):
    tunnel = SSHTunnelForwarder(
            ('192.168.2.85', 22),
            ssh_username='ubuntu',
            ssh_password='password',
            remote_bind_address=('0.0.0.0', 3306))

    tunnel.start()
    connection = pymysql.connect(host='localhost', user='molengo',
                            passwd='%molengo2019', db='molengo',
                            port=tunnel.local_bind_port)  
    df = pd.read_sql(query, connection)
    connection.close()
    tunnel.stop()
    return df

cams = read_from_db("SELECT v.id as video, c.attr \
                     FROM video_files as v JOIN cameras as c \
                     ON v.cam_id = c.id")

def analyze_df(nums):
    df = read_from_db(f"SELECT time_visit, cluster_id, file_id FROM final_clusters WHERE cluster_id IN ({ ','.join([str(i) for i in nums]) })")
    df = pd.merge(df, cams, left_on='file_id', right_on='video')
    visits = dict()
    for id_ in df.cluster_id.unique():
        clstr = df[df.cluster_id == id_].sort_values('time_visit')
        visits[id_] = []
        prev = clstr.iloc[0]
        for i in range(clstr.shape[0] - 1):
            curr = clstr.iloc[i, :]
            next_ = clstr.iloc[i+1, :]

            if next_.time_visit - curr.time_visit >= datetime.timedelta(hours=1) and next_.attr == 'entrance' and curr.attr == 'entrance':
                visits[id_].append(
                    (prev.time_visit, curr.time_visit)
                )
                prev = next_
        if prev.eq(clstr.iloc[0]).all():
            visits[id_].append(
                (clstr.iloc[0].time_visit, clstr.iloc[-1].time_visit)
            )
    return [Visits(
        cluster_id=int(d[0]),
        start=d[1][0].strftime("%y-%m-%d %H-%M-%S"),
        end=d[1][1].strftime("%y-%m-%d %H-%M-%S")
        ) 
            for d in zip(
                visits.keys(),
                *visits.values()
            )]



def count_visits():
    per_proc = 1000
    df = read_from_db("SELECT DISTINCT cluster_id FROM final_clusters").values
    clusters = np.reshape(df[:df.shape[0] // per_proc * per_proc], (-1, per_proc)).tolist() + df[df.shape[0] // 500 * 500:].tolist()
    with ProcessPoolExecutor() as executor:
        for result in executor.map(analyze_df, clusters):
            print('calculated!!!')
            Visits.add_db(result)
    return True

if __name__ == '__main__':
    res = count_visits()
    print(res)
    
    