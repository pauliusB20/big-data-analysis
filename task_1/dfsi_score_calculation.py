import pandas as pd
import numpy as np
import os



if __name__ == "__main__":

    anomaly_a = pd.read_csv('ANOMALY_A_result.csv', header=None)
    anomaly_d = pd.read_csv('anomaly_results.csv')


    if os.path.exists('anomaly_c_draught.csv'):
        anomaly_c = pd.read_csv('anomaly_c_draught.csv', header=None)
        anomaly_c.columns = ['mmsi', 'Timestamp', 'Lat', 'Lon', 'SOG', 'Draught']

        drought = (anomaly_c.groupby('mmsi').size() / 2).reset_index()
        drought.columns = ['mmsi', 'drought_count']
    else:

        drought = pd.DataFrame(columns=['mmsi', 'drought_count'])


    anomaly_a.columns = [
        'mmsi', 
        'Timestamp', 
        'Lat', 
        'Lon', 
        'SOG', 
        'Draught', 
        'cargo_type', 
        'ship_type',
        'nav_status',
        'vessel_type'
    ]
    anomaly_a['Timestamp'] = pd.to_datetime(anomaly_a['Timestamp'])


    anomaly_a['pair_id'] = np.arange(len(anomaly_a)) // 2
    anomaly_a['Time_Diff'] = anomaly_a.groupby('pair_id')['Timestamp'].diff()
    anomaly_a = anomaly_a.dropna().drop(["pair_id"], axis=1)


    max_gap = anomaly_a.groupby("mmsi")["Time_Diff"].max()


    Total_jumps = anomaly_d[["mmsi", "total_jump_nm"]]


    final_df = Total_jumps.merge(max_gap, left_on='mmsi', right_index=True, how='right')
    final_df = final_df.reset_index(drop=True)


    final_df = final_df.merge(drought, on='mmsi', how="left")

    final_df.fillna(0, inplace=True)


    final_df['hours'] = pd.to_timedelta(final_df['Time_Diff']).dt.total_seconds() / 3600


    final_df['DFSI'] = (final_df["hours"] / 2) + (final_df["total_jump_nm"] / 10) + (final_df["drought_count"] * 15)


    final_df.sort_values('DFSI', ascending=False, inplace=True)

    print("Displaying results:")
    print(final_df)