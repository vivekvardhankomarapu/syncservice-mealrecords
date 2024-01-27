import random
import datetime
import uuid
from typing import Optional, Dict, Any
# Modified the given code to handle empty cases too and made some small changes in testSyncing too for handling empty cases
# Keys for the data dictionary
_DATA_KEYS = ["a", "b", "c"]

class Device:
    def __init__(self, id):
        """
        Device representing a tablet
        every tablet has an id 
        """
        self._id = id
        self.records = []  # Local records stored by the device
        self.sent = []     # Records sent by the device to SyncService

    def obtainData(self) -> dict:
      """Returns a single new datapoint from the device.
        Identified by type `record`. `timestamp` records when the record was sent and `dev_id` is the device id.
        `data` is the data collected by the device."""
  
      """
        Shows obtaining data by the device from the server.
        Returns a dictionary representing a new record if conditions are met, otherwise an empty dictionary.
        """
        if random.random() < 0.4:
          # Sometimes there's no new data
            return {}
        # Create a new record
        rec = {
            'type': 'record',
            'timestamp': datetime.datetime.now().isoformat(),
            'dev_id': self._id,
            'data': {kee: str(uuid.uuid4()) for kee in _DATA_KEYS}
        }

        # Add the new record to the local records and the sent records
        self.sent.append(rec)
        self.records.append(rec)
        return rec

    def probe(self) -> dict:
        """
        probing the server for any updates to be made. 
        Returns a probe request to be sent to the SyncService.
        Identified by type `probe`. `from` is the index number from which the device is asking for the data.
        returning a dictionary representing a probe message with the device's ID and the current record count.
        """
        if random.random() < 0.5:
            return {}
        return {'type': 'probe', 'dev_id': self._id, 'from': len(self.records)}

    def onMessage(self, data: Optional[Dict[str, Any]]):
        """
        Process incoming messages by the device.
        Receives updates from the server
        """
        if random.random() < 0.6:
          # Sometimes devices make mistakes. Let's hope the SyncService handles such failures.
            return

        if data is None:
            return

        if data and isinstance(data, dict):
            if data['type'] == 'update':
                _from = data.get('from', 0)
                if _from > len(self.records):
                    return
                self.records = self.records[:_from] + data.get('data', [])
                return

class SyncService:
    def __init__(self):
        """
        Initialize a SyncService object similar to server's functionalities
        """
        self.records = []  # Records stored by the SyncService

    def onMessage(self, data: Optional[Dict[str, Any]]):
        """
        Process incoming messages by the SyncService.
        Handle messages received from devices.
        Return the desired information in the correct format (type `update`, see Device.onMessage and testSyncing to understand format intricacies) in response to a `probe`.
        No return value required on handling a `record`.
        return a dictionary representing an update message in response to a probe or a record message.
        """
        if not data:
            return None

        if 'type' in data and isinstance(data, dict):
            if data['type'] == 'record':
                self.records.append(data)
                return  # No response needed for records
            elif data['type'] == 'probe':
                from_index = data.get('from', 0)
                new_records = self.records[from_index:]
                return {'type': 'update', 'from': from_index, 'data': new_records}

        raise ValueError(f"Unexpected message type: {data.get('type')}")

def testSyncing():
    """
    Test the synchronization process between devices and SyncService.
    """
    devices = [Device(f"dev_{i}") for i in range(10)]
    syn = SyncService()

    _N = int(1e6)
    for i in range(_N):
        for _dev in devices:
            data_from_probe = _dev.probe()
            if data_from_probe:
                syn.onMessage(_dev.obtainData())
                update_message = _dev.onMessage(syn.onMessage(_dev.probe()))
                if update_message:
                    _dev.onMessage(update_message)

    done = False
    while not done:
        for _dev in devices:
            _dev.onMessage(syn.onMessage(_dev.probe()))
        num_recs = len(devices[0].records)
        done = all([len(_dev.records) == num_recs for _dev in devices])

    ver_start = [0] * len(devices)
    for i, rec in enumerate(devices[0].records):
        _dev_idx = int(rec['dev_id'].split("_")[-1])
        assertEquivalent(rec, devices[_dev_idx].sent[ver_start[_dev_idx]])
        for _dev in devices[1:]:
            assertEquivalent(rec, _dev.records[i])
        ver_start[_dev_idx] += 1

def assertEquivalent(d1: dict, d2: dict):
    """
     Make sure two dictionaries that represent records are the same
    """
    assert d1['dev_id'] == d2['dev_id']
    assert d1['timestamp'] == d2['timestamp']
    for kee in _DATA_KEYS:
        assert d1['data'][kee] == d2['data'][kee]

testSyncing()
