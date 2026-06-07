import folium
import pandas as pd
import numpy as np
import random


SHIP_A_RESULTS = "ship_a_collision/part-00000-3f9d1ea3-a87b-446d-bdd9-3dd9f751efc5-c000.csv"
SHIP_B_RESULTS = "ship_b_collision/part-00000-133e8e30-1c80-4158-aeac-87664a23dec9-c000.csv"



RESULT_HTML = "task_4_ship_paths.html"

# colors = [
#     'cadetblue', 
#     'blue', 
#     'darkgreen', 
#     'green', 
#     'red', 
#     'white', 
#     'lightblue', 
#     'black', 
#     'orange', 
#     'darkred', 
#     'gray', 
#     'purple', 
#     'darkblue', 
#     'lightgreen', 
#     'pink', 
#     'lightred', 
#     'darkpurple', 
#     'lightgray', 
#     'beige'
# ]


def _read_csv_coordinates(file_csv: str) -> list[tuple]:
    data_frame = pd.read_csv(file_csv)
    return list(map(tuple, data_frame[["Latitude", "Longitude"]].values))

def _get_map_center() -> object:
   center = folium.Map(
        location=[58.5, 20.0],
        zoom_start=5,
        max_zoom=22,
        tiles="CartoDB Positron"
    )
   return center
    
    
def _draw_ship_on_map(geo_map_center: object, ship_name: str, color: str, coordinates: list[tuple]) -> None:    
    
    points = []
    
    for idx, (lat, lon) in enumerate(coordinates):
        
        point1 = (lat, lon)
        
        folium.Marker(
            location=point1,
            popup=f"ship {ship_name} point",
            icon=folium.Icon(color=color)
        ).add_to(geo_map_center)

        points.append(point1)


    # Draw line between points
    folium.PolyLine(
        locations=points,
        color=color,
        weight=5,
        opacity=1
    ).add_to(geo_map_center)
    
    # Optional: auto-fit map to both points
    
    geo_map_center.save(RESULT_HTML)

    print(f"Saved ship {ship_name} data on a visual map")
    
if __name__ == "__main__":
    
    print("Starting anomaly D jumps visualization...")
    geo_map_center = _get_map_center()
    ship_a_coordintes = _read_csv_coordinates(SHIP_A_RESULTS)
    ship_b_coordintes = _read_csv_coordinates(SHIP_B_RESULTS)
    
    
    _draw_ship_on_map(geo_map_center, ship_name="Ship A", color="blue", coordinates=ship_a_coordintes)
    _draw_ship_on_map(geo_map_center, ship_name="Ship B", color="green", coordinates=ship_b_coordintes)
    
    print(f"Saved map in path={RESULT_HTML}")
    print("DONE")
    