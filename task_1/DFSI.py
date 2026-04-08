import pandas as pd
import numpy as np
import os


Anomaly_A = pd.read_csv('ANOMALY_A_result.csv', header=None)
Anomaly_D = pd.read_csv('anomaly_results.csv')


try:
    if os.path.exists('anomaly_c_draught.csv') and os.path.getsize('anomaly_c_draught.csv') > 0:
        Anomaly_C = pd.read_csv('anomaly_c_draught.csv', header=None)
        Anomaly_C.columns = ['mmsi', 'Timestamp', 'Lat', 'Lon', 'SOG', 'Draught']

        drought = (Anomaly_C.groupby('mmsi').size() / 2).reset_index()
        drought.columns = ['mmsi', 'drought_count']
    else:

        drought = pd.DataFrame(columns=['mmsi', 'drought_count'])
except Exception:
    drought = pd.DataFrame(columns=['mmsi', 'drought_count'])


Anomaly_A.columns = ['mmsi', 'Timestamp', 'Lat', 'Lon', 'SOG', 'Draught']
Anomaly_A['Timestamp'] = pd.to_datetime(Anomaly_A['Timestamp'])


Anomaly_A['pair_id'] = np.arange(len(Anomaly_A)) // 2
Anomaly_A['Time_Diff'] = Anomaly_A.groupby('pair_id')['Timestamp'].diff()
Anomaly_A = Anomaly_A.dropna().drop(["pair_id"], axis=1)


max_gap = Anomaly_A.groupby("mmsi")["Time_Diff"].max()


Total_jumps = Anomaly_D[["mmsi", "total_jump_nm"]]


final_df = Total_jumps.merge(max_gap, left_on='mmsi', right_index=True, how='right')
final_df = final_df.reset_index(drop=True)


final_df = final_df.merge(drought, on='mmsi', how="left")

final_df.fillna(0, inplace=True)


final_df['hours'] = pd.to_timedelta(final_df['Time_Diff']).dt.total_seconds() / 3600


final_df['DFSI'] = (final_df["hours"] / 2) + (final_df["total_jump_nm"] / 10) + (final_df["drought_count"] * 15)


final_df.sort_values('DFSI', ascending=False, inplace=True)

print(final_df)