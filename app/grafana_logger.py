import logging
import json
from datetime import datetime
from enum import Enum
from pathlib import Path


class EventType(Enum):
    PLAYLIST_CREATED = "playlist_created"
    PLAYLIST_UPDATED = "playlist_updated"
    TRACKS_ADDED = "tracks_added"
    ERROR = "error"
    INFO = "info"


class JsonGrafanaLogger(logging.Logger):
    """
    Logger that appends log events to a JSON array in a .json file for direct use in Grafana.
    """

    def __init__(self, name: str, log_file: str):
        """
        Initialize the JSON Grafana logger.
        Args:
            name (str): Name of the logger.
            log_file (str): Path to the JSON log file.
        """
        super().__init__(name)
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        # No FileHandler needed — we manage JSON file directly
        self.setLevel(logging.INFO)

    def log_event(
        self,
        event_type: EventType,
        status: str,
        duration_s: float,
        message: str = "",
        metadata: dict = None,
    ):
        """
        Log an event to the JSON file.
        Args:
            event_type (EventType): Type of the event.
            status (str): Status of the event (e.g., "success", "failure").
            duration_s (float): Duration of the event in seconds.
            message (str): Optional message for the event.
            metadata (dict, optional): Additional metadata to include in the event.
        """
        if metadata is None:
            metadata = {}

        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be an instance of EventType")

        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type.value,
            "status": status,
            "duration_s": duration_s,
            "message": message,
            "metadata": metadata,
        }

        if self.log_file.exists():
            try:
                with self.log_file.open("r") as f:
                    logs = json.load(f)
            except json.JSONDecodeError:
                logs = []
        else:
            logs = []

        logs.append(log_entry)

        with self.log_file.open("w") as f:
            json.dump(logs, f, indent=2)

        self.info(f"Logged event: {log_entry['event_type']} - {status}")
