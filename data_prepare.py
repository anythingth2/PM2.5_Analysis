# %%
import ujson
from datetime import datetime
import pandas as pd
from pathlib import Path
from tqdm import tqdm, trange

# %%


def create_sensor_csv(sensor_dir, csv_path):
    sensor_dir = Path(sensor_dir)
    rows = []
    for sensor_path in tqdm(sorted(sensor_dir.glob('*.json'))):
        with open(str(sensor_path), 'r') as f:
            sensor_json = ujson.load(f)
        team_id = sensor_path.stem[-2:]
        for row_id, data in sensor_json.items():
            if 'DevEUI_uplink' not in data:
                continue
            data = data['DevEUI_uplink']
            timestamp = datetime.strptime(
                data['Time'][:19], '%Y-%m-%dT%H:%M:%S')
            if 'payload_hex' in data and data['payload_hex'] != '191':
                sensor = int(data['payload_hex'][2:], 16)
            else:
                sensor = None
            rows.append({
                'team_id': team_id,
                'DevEUI': data['DevEUI'],
                'row_id': row_id,
                'timestamp': timestamp,
                'lat': float(data['LrrLAT']),
                'lng': float(data['LrrLON']),
                'sensor': sensor,
            })
    sensor_df = pd.DataFrame(rows)
    sensor_df.to_csv(csv_path)


# %%
create_sensor_csv('dataset/tgr_sensor', 'tgr_sensor.csv')
