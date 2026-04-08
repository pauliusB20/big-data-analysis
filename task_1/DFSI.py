import pandas as pd
import numpy as np

Anomaly_A = pd.read_csv('ANOMALY_A_result.csv',header=None)
Anomaly_C= pd.read_csv('anomaly_c_draught.csv',header=None)
Anomaly_D = pd.read_csv('anomaly_results.csv')

Anomaly_A.columns=['mmsi', 'Timestamp', 'Lat', 'Lon', 'SOG', 'Draught']
Anomaly_C.columns=['mmsi', 'Timestamp', 'Lat', 'Lon', 'SOG', 'Draught']


Anomaly_A['Timestamp'] = pd.to_datetime(Anomaly_A['Timestamp'])
Anomaly_A['pair_id'] = np.arange(len(Anomaly_A)) // 2
Anomaly_A['Time_Diff'] = Anomaly_A.groupby('pair_id')['Timestamp'].diff()
Anomaly_A=Anomaly_A.dropna()
Anomaly_A=Anomaly_A.drop(["pair_id"],axis=1)
max_gap=Anomaly_A.groupby("mmsi")["Time_Diff"].max()
max_gap_df = max_gap.reset_index()
max_gap_df.columns = ['mmsi', 'max_time_gap']


Total_jumps=Anomaly_D[["mmsi","total_jump_nm"]]

final_df = Total_jumps.merge(max_gap, left_on='mmsi', right_index=True, how='right')
final_df = final_df.reset_index(drop=True)

drought = Anomaly_C.groupby('mmsi').size() / 2
drought = drought.reset_index()
drought.columns = ['mmsi', 'drought_count']

final_df=final_df.merge(drought, on='mmsi',how="left")

final_df=final_df.fillna(0, inplace=True)



# 1. Ensure Time_Diff is in timedelta format
final_df['Time_Diff'] = pd.to_timedelta(final_df['Time_Diff'])

# 2. Convert Time_Diff to total hours (as a float)
final_df['hours'] = final_df['Time_Diff'].dt.total_seconds() / 3600
final_df

final_df['DFSI'] = (final_df["hours"]/2)+(final_df["total_jump_nm"]/10)+(final_df["drought_count"])*15

# Optional: Convert NaNs to 0 as you requested previously
final_df['DFSI'] = final_df['DFSI'].fillna(0)

final_df.sort_values('DFSI', ascending=False, inplace=True)
final_df