#this script is for HARU asd

import pandas as pd
import datetime

filename = "sensor_data_hrm.csv"

df = pd.read_csv(filename)
saved_column = df['timestamp']

datetime_column = []
for r in saved_column:
    time = datetime.datetime.fromtimestamp(int(r) / 1000)
    datetime_column.append(time.strftime("%d-%m-%Y, %H:%M:%S"))

new_df = pd.DataFrame(columns=['username', 'datetime', 'value'])
new_df['username'] = df['username_id']
new_df['datetime'] = datetime_column
new_df['value'] = df['value']
new_df.reset_index()


new_df.to_csv("new3.csv")
