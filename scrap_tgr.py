# %%
import requests
from tqdm import tqdm, trange
from pathlib import Path
import ujson
from datetime import datetime
import pandas as pd
import shutil
# %%


def fetch_tgr_dataset(host, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    n_team = 40
    for i in trange(1, n_team-1):
        team_id = f'team{str(i).rjust(2, "0")}'
        r = requests.get(f'{host}/{team_id}.json')
        if not r.text == 'null':
            with open(output_dir / f'{team_id}.json', 'w') as f:
                f.write(r.text)


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

# %%

# %%

# %%
def extract_exported_json(json_path, output_sensor_dir, output_location_dir):

    # %%
    output_sensor_dir = Path(output_sensor_dir)
    if output_sensor_dir.exists():
        shutil.rmtree(str(output_sensor_dir))
    output_sensor_dir.mkdir(exist_ok=True)
    output_location_dir = Path(output_location_dir)
    if output_location_dir.exists():
        shutil.rmtree(str(output_location_dir))
    output_location_dir.mkdir(exist_ok=True)
    # %%
    with open(json_path, 'r') as f:
        exported_dict = ujson.load(f)
    # %%
    location_dict = exported_dict['quiz']['location']
    sensor_dict = exported_dict['quiz']['sensor']

    def extract_chunk_json(chunk_dict, chunk_dir):
        for team_name, _dict in chunk_dict.items():
            if team_name == 'teamXX':
                continue
            with open(chunk_dir / (team_name+'.json'), 'w') as f:
                ujson.dump(_dict, f)

    extract_chunk_json(location_dict, output_location_dir)
    extract_chunk_json(sensor_dict, output_sensor_dir)
# %%

# %%
# fetch_tgr_dataset(
#     'https://tgr2020-quiz.firebaseio.com/quiz/sensor', 'dataset/tgr_sensor')
# fetch_tgr_dataset(
#     'https://tgr2020-quiz.firebaseio.com/quiz/location', 'dataset/tgr_location')

extract_exported_json(json_path='dataset/tgr2020-quiz-export latest.json',
                      output_sensor_dir='dataset/tgr_sensor',
                      output_location_dir='dataset/tgr_location'
                      )

create_sensor_csv('dataset/tgr_sensor', 'dataset/tgr_sensor.csv')


# %%
