from dataclasses import dataclass, astuple, fields
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
    
    @property
    def point(self) -> tuple:
        return (
            self.latitude,
            self.longitude            
        )
        
    def _is_mmsi_valid(self, mmsi: str) -> bool:
        return (
            mmsi.isdigit() 
            and 2 <= int(mmsi[0]) <= 7 
            and len(mmsi) == 9
            and len(set(mmsi)) > 1
        )
        
    # Conservative coordinates for Baltic sea
    def _is_valid_latitude(self, latitude: float) -> bool:
        return 50.0 < latitude < 70.0

    def _is_valid_longitude(self, longitude: float) -> bool:
        return 5.0 < longitude < 35.0
    
    def _as_tuple(self) -> tuple:
        return (
            self.mmsi,
            self.timestamp,
            self.longitude.item(),
            self.latitude.item(),
            self.sog.item(),
            self.draught.item(),
            self.cargo_type,
            self.ship_type
        )
    
    def _get_header(self) -> list:
        return [field.name for field in fields(self)]
        
    def _as_tuple_db(self) -> list:
        return [
            self.mmsi,
            self.timestamp,
            self.longitude,
            self.latitude,
            self.sog,
            self.draught,
            self.cargo_type,
            self.ship_type
        ]
        
    # TODO: prideti baltic sea
    # Data validator for preventing "dirty data"
    def __post_init__(self) -> None:
       if not self._is_mmsi_valid(self.mmsi):
           raise ShipTypeError("MMSI not ship type!")
       
       if not self._is_valid_latitude(self.latitude):
           raise ValueError("Invalid latitude")
       
       if not self._is_valid_longitude(self.longitude):
           raise ValueError("Invalid longitude")
