# %%
import ujson
from datetime import datetime
import pandas as pd
from pathlib import Path
from tqdm import tqdm, trange
import ujson
import datetime

import sys

import requests
import wget
from bs4 import BeautifulSoup
from selenium import webdriver
import re
import shutil
# %%

# %%


# This code was modified from https://github.com/worasom/aqi_thailand
def scrap_berkeley(output_dir):
    url = 'http://berkeleyearth.lbl.gov/air-quality/maps/cities/Thailand/'
    res = requests.get(url)
    # create a soup object of Berkeley earth website
    soup = BeautifulSoup(res.text)

    # find all provinces in this database
    provinces = soup.find_all(href=re.compile('/'))[1:]

    # In[9]:
    output_dir = Path(output_dir)
    if output_dir.exists():
        shutil.rmtree(str(output_dir))
    output_dir.mkdir(parents=True, exist_ok=True)
    # %%

    def download_txt(folder):
        '''Input: soup object that contain the href for downloading data'''
        grab_url = url+folder
        success = False
        while not success:
            try:
                prov_r = requests.get(grab_url)
                prov_s = BeautifulSoup(prov_r.text)
                for tag in prov_s.find_all(href=re.compile('.txt')):

                    data_url = grab_url+tag['href']
                    if 'neighbors' in data_url:
                        continue
                    # name = 'data/' + tag['href']
                    name = str(output_dir / tag['href'])
                    wget.download(data_url, name)
                success = True
            except requests.exceptions.ConnectionError:
                print(f'retry {folder}')
            except ConnectionResetError:
                print(f'retry connected reset {folder}')
        print()
        print(f'{folder} downloaded')

    for province in (provinces):
        download_txt(province['href'])

    for path in output_dir.glob('*.txt'):
        if '(1)' in path.name:
            print('remove duplicate', path)
            path.unlink()


def prepare_berkeley_dataset(input_dir, output_dir):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    if output_dir.exists():
        shutil.rmtree(str(output_dir))
    output_dir.mkdir(parents=True, exist_ok=True)

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
scrap_berkeley('dataset/raw_berkeleyearth')
prepare_berkeley_dataset('dataset/raw_berkeleyearth', 'dataset/berkeleyearth')

# %%
