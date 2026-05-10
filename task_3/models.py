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
    

    # NEW: Added for filtering (Task 3 & 4)
    rot: np.float32
    cog: np.float32
    heading: np.float32

    def _get_doc(self) -> dict:
        return {
            "mmsi":        self.mmsi,
            "timestamp":   self.timestamp,
            "longitude":   float(self.longitude),
            "latitude":    float(self.latitude),
            "sog":         float(self.sog),
            "draught":     float(self.draught),
            "cargo_type":  self.cargo_type,
            "ship_type":   self.ship_type, 
            "nav_status":  self.nav_status,
            "vessel_type": self.vessel_type,

            # NEW for task 3 and 4:
            "rot":         float(self.rot),
            "cog":         float(self.cog),
            "heading":     float(self.heading),
        }