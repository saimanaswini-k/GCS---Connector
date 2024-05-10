from dataclasses import dataclass
from typing import List
from uuid import uuid4
from datetime import datetime
from typing import Dict

@dataclass
class CustomMetadata:
    key: str
    value: str

    def to_dict(self) :
        return {
            'key': self.key,
            'value': self.value
        }
    

@dataclass
class ObjectInfo:
    id: str = str(uuid4())
    bucket_name: str = None
    object_name: str = None
    location: str = None
    format: str = None
    file_size_kb: int = 0
    in_time: str = datetime.now().isoformat()
    start_processing_time: str = None
    end_processing_time: str = None
    download_time: float = 0.0
    file_hash: str = None
    num_of_retries: int = 0
    custom_metadata: Dict[str, str] = None

    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'bucket_name': self.bucket_name,
            'object_name': self.object_name,
            'location': self.location,
            'format': self.format,
            'file_size_kb': self.file_size_kb,
            'in_time': self.in_time,
            'start_processing_time': self.start_processing_time,
            'end_processing_time': self.end_processing_time,
            'download_time': self.download_time,
            'file_hash': self.file_hash,
            'num_of_retries': self.num_of_retries,
            'custom_metadata': self.custom_metadata or {}
        }
 
