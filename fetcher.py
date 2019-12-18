# %%
import requests
from tqdm import tqdm, trange
from pathlib import Path

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


# %%
fetch_tgr_dataset(
    'https://tgr2020-quiz.firebaseio.com/quiz/sensor', 'dataset/tgr_sensor')
fetch_tgr_dataset(
    'https://tgr2020-quiz.firebaseio.com/quiz/location', 'dataset/tgr_location')

# %%
