# %%
import ujson
from datetime import datetime
import pandas as pd
from pathlib import Path
from tqdm import tqdm, trange
import ujson
import datetime
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
            if 'payload_hex' in data and data['payload_hex'] != '191' and len(data['payload_hex']) == 4:
                sensor = int(data['payload_hex'][2:4], 16)
                rows.append({
                    'team_id': team_id,
                    'DevEUI': data['DevEUI'],
                    'row_id': row_id,
                    'timestamp': timestamp,
                    'lat': float(data['LrrLAT']),
                    'lng': float(data['LrrLON']),
                    'sensor': sensor,
                })
            else:
                continue

    sensor_df = pd.DataFrame(rows)
    sensor_df.to_csv(csv_path)


# %%
# create_sensor_csv('dataset/tgr_sensor', 'tgr_sensor.csv')
# %%


def prepare_berkeley_dataset(input_dir, output_dir):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    region_df = []
    sensor_df = []

    for input_path in tqdm(sorted(input_dir.glob('*.txt'))):
        with open(input_path, 'r') as f:
            lines = f.read().split('\n')
            infos = lines[:9]
            sensor_header = lines[9]
            sensor_content = lines[10:]

        info_dict = {}
        for info in infos:
            info = info.replace('%', '').replace(' ', '')
            topic, data = info.lower().split(':')
            info_dict[topic] = data
        region_df.append((info_dict['city'], info_dict['region'],
                          info_dict['latitude'], info_dict['longitude'], info_dict['population']))

        for sensor_data in sensor_content[:-1]:
            sensor_data = sensor_data.split('\t')
            timestamp_data = [int(v) for v in sensor_data[:4]]
            timestamp = datetime.datetime(*timestamp_data)
            pm = float(sensor_data[4])
            sensor_df.append((info_dict['city'], timestamp, pm))

    region_df = pd.DataFrame(
        region_df, columns=['city', 'region', 'lat', 'lng', 'population'])
    sensor_df = pd.DataFrame(sensor_df, columns=['city', 'timestamp', 'pm'])

    region_df.to_csv(str(output_dir / 'region.csv'))
    sensor_df.to_csv(str(output_dir / 'sensor.csv'))


# %%
prepare_berkeley_dataset('dataset/raw_berkeleyearth', '')

# %%
