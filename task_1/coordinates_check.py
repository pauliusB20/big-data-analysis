import reverse_geocode as rg
from geopy.geocoders import Nominatim
import pandas as pd
import folium


# latitude = 56.262943267822266
# longitude = 12.263402938842773

FILE = "anomaly_results.csv"
suffix = "anomaly_b_"

def _save_location(loc_name: str, latitude: float, longitude: float) -> str:
    m = folium.Map(location=[latitude, longitude], zoom_start=10)
    folium.Marker([latitude, longitude], popup="Ship location").add_to(m)
    print(f"Saved location {loc_name}.html")
    point = (float(latitude), float(longitude))
    print(f"With coordinates {point}\n")
    m.save(loc_name + ".html")


if __name__ == "__main__":
    
    anomaly_data = pd.read_csv(FILE)
    # column_size = len(list(anomaly_data.columns))
    # anomaly_data.columns = range(column_size)
    
#     rows = len(anomaly_data)
    
    rows = anomaly_data[["latitude", "longitude"]]
    for idx, (lat, long) in enumerate(rows.values):
        loc_name = f"{suffix}{idx}"
        _save_location(loc_name, lat, long)