import folium
import pandas as pd
import numpy as np
import random


FILE = "top_10_anomaly_d.csv"

FILE_CSV_FIELDS_POINTS = [
    "mmsi",
    "max_jump_lat1", 
    "max_jump_lon1",
    "max_jump_lat2",
    "max_jump_lon2"
]

RESULT_HTML = "anomaly_d_gui_map.html"

colors = [
    'red', 'blue', 'green', 'purple', 'orange',
    'darkred', 'lightred', 'beige', 'darkblue',
    'darkgreen', 'cadetblue', 'darkpurple', 'white',
    'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray'
]

def _read_csv_coordinates(file_csv: str) -> list[tuple]:
    data_frame = pd.read_csv(file_csv)
    result = data_frame[FILE_CSV_FIELDS_POINTS].values
    return result

def _get_map_center() -> object:
   center = folium.Map(location=[58.5, 20.0], zoom_start=5) 
   return center
    
    
def _draw_on_map(coordinates: list[np.array]) -> None:
    
    geo_map_center = _get_map_center()
    
    for (mmsi, a_lat, a_lon, b_lat, b_lon) in coordinates:
        
        mmsi = str(int(mmsi))
        point1 = (a_lat, a_lon)
        
        folium.Marker(
            location=point1,
            popup=f"ship mmsi={mmsi} jump A point",
            icon=folium.Icon(color=random.choice(colors))
        ).add_to(geo_map_center)

        point2 = (b_lat, b_lon)
        folium.Marker(
            location=point2,
            popup=f"ship mmsi={mmsi} jump B point",
            icon=folium.Icon(color=random.choice(colors))
        ).add_to(geo_map_center)

        # Draw line between points
        folium.PolyLine(
            locations=[point1, point2],
            color="green",
            weight=3,
            opacity=0.8
        ).add_to(geo_map_center)

        # Optional: auto-fit map to both points
        geo_map_center.fit_bounds([point1, point2])

    geo_map_center.save(RESULT_HTML)

    
if __name__ == "__main__":
    
    print("Starting anomaly D jumps visualization...")
    coordinates = _read_csv_coordinates(FILE)
    
    _draw_on_map(coordinates)
    
    print(f"Saved map in path={RESULT_HTML}")
    print("DONE")
    