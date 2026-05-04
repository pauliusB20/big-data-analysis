from dataclasses import dataclass
# , astuple, fields
from datetime import datetime
import numpy as np

class ShipTypeError(Exception):
    
    def __init__(self, *args):
        super().__init__(*args)

@dataclass
class ShipRow:
    mmsi: str
    timestamp: datetime
    longitude: np.float32
    latitude: np.float32
    sog: np.float32
    draught: np.float32
    cargo_type: str
    ship_type: str
    nav_status: str #New, added for anomaly B
    vessel_type: str #New, added for anomaly B