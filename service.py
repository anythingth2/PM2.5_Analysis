# %%

from flask import Flask, request
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from keras.models import load_model
from sklearn.externals import joblib
from flask import request

# %%

tgr_df = pd.read_csv('dataset/tgr_sensor.csv')
tgr_df['timestamp'] = pd.to_datetime(tgr_df['timestamp'])


ex_region_df = pd.read_csv('dataset/berkeleyearth/region.csv')
ex_region_df.rename({'Unnamed: 0': 'id'}, axis=1, inplace=True)
ex_sensor_df = pd.read_csv('dataset/berkeleyearth/sensor.csv')
ex_sensor_df.rename({'Unnamed: 0': 'id'}, axis=1, inplace=True)
ex_sensor_df['timestamp'] = pd.to_datetime(ex_sensor_df['timestamp'])
ex_sensor_df = ex_sensor_df.set_index('city')

print('dataset loaded')
# %%


grid_size = 0.001

origin_lat = 12
origin_lng = 90


def grid_mapper(row):
    # row = tgr_df.loc[index]
    lat = row['lat']
    lng = row['lng']
    centroid_lat = origin_lat + (((lat - origin_lat) * 1000 //
                                  (grid_size * 1000)) / 1000) + (grid_size / 2)
    centroid_lng = origin_lng + (((lng - origin_lng) * 1000 //
                                  (grid_size * 1000)) / 1000) + (grid_size / 2)
    return (centroid_lat, centroid_lng)


cells = np.array(tgr_df.apply(grid_mapper, axis=1).to_list())

tgr_df['cell_x'] = cells[:, 0]
tgr_df['cell_y'] = cells[:, 1]


def hourly_tgr_resample(df):

    resampled = df.set_index('timestamp').resample('H')
    return resampled['sensor'].mean().reset_index()


def daily_tgr_resample(df):

    resampled = df.set_index('timestamp').resample('D')
    return resampled['sensor'].mean().reset_index()


hourly_tgr_df = tgr_df.groupby(['cell_x', 'cell_y']).apply(hourly_tgr_resample)
daily_tgr_df = tgr_df.groupby(['cell_x', 'cell_y']).apply(daily_tgr_resample)

print('initialized tgr dataset')
# %%


def create_tgr_json(cell_df, last_n=None):
    lat, lng = cell_df.name
    if last_n is not None:
        cell_df = cell_df.tail(last_n)

    return {
        'lat': lat,
        'lng': lng,
        'history': cell_df.apply(lambda row: {'pm25': row['sensor'] if row['sensor'] is not pd.NaT else None,
                                              'timestamp': row['timestamp'].to_pydatetime()}, axis=1).to_list()
    }


# %%


def daily_mean_interpolate(df):
    df = df.drop('id', axis=1).set_index('timestamp').resample('D').mean()
    df['pm'] = df['pm'].interpolate()
    return df.reset_index()


ex_daily_sensor_df = ex_sensor_df.groupby('city').apply(daily_mean_interpolate)
print('initialized berkeley dataset')
# %%
scaler = joblib.load('scaler.pkl')
# %%

# %%
model = load_model('model.h5')
model.summary()
print('initalized model')
# %%
window_size = 7


def forecast(city_sensor):
    city_sensor = city_sensor.tail(window_size)
    x = city_sensor['pm'].to_numpy()

    x = np.expand_dims(x, axis=0)
    x = scaler.transform(x)
    x = np.expand_dims(x, axis=2)

    y = model.predict(x)
    y = scaler.inverse_transform(y)
    y = y[0][0]

    last_timestamp = city_sensor.tail(1)['timestamp'].iloc[0]
    next_timestamp = last_timestamp + pd.Timedelta(days=1)
    return pd.Series({'timestamp': next_timestamp, 'pm': y})


# %%
forecasting_sensor_df = ex_daily_sensor_df.groupby('city').apply(forecast)
print('initialized forecasting')
# %%


def create_city_info_dict(city_info,):
    return {
        'name': city_info['city'],
        'region': city_info['region'],
        'lat': city_info['lat'],
        'lng': city_info['lng']
    }


def create_sensor_record(sensor_df, last_n=None):
    if last_n is not None:
        sensor_df = sensor_df.tail(last_n)
    return sensor_df.apply(lambda row: {'pm25': row['pm'] if not pd.isna(row['pm']) else None,
                                        'timestamp': row['timestamp']}, axis=1).to_list()


def create_daily_city(city_info):
    output_dict = create_city_info_dict(city_info)
    output_dict['history'] = create_sensor_record(
        ex_daily_sensor_df.loc[city_info['city']], 30)
    timestamp, pm = forecasting_sensor_df.loc[city_info['city']].to_list()
    forecasting_dict = {'timestamp': timestamp, 'pm25': pm}
    output_dict['forecasting'] = [forecasting_dict]
    return output_dict


def create_hourly_city(city_info):
    output_dict = create_city_info_dict(city_info)
    output_dict['history'] = create_sensor_record(
        ex_sensor_df.loc[city_info['city']], 24)
    return output_dict
# %%


# %%
app = Flask(__name__)


@app.route('/api/sensor/tgr', methods=['GET'])
def get_tgr_sensor():
    period = request.args.get('period')
    if period == 'day':
        return {'data': daily_tgr_df.groupby(['cell_x', 'cell_y']).apply(create_tgr_json, 15).to_list()}
    elif period == 'hour':
        return {'data': hourly_tgr_df.groupby(['cell_x', 'cell_y']).apply(
            create_tgr_json, 24).to_list()}

    return {}


@app.route('/api/sensor/berkeley', methods=['GET'])
def get_berkely_sensor():
    period = request.args.get('period')
    if period == 'day':
        return {'data': ex_region_df.apply(create_daily_city,  axis=1).to_list()}
    elif period == 'hour':
        return {'data': ex_region_df.apply(create_hourly_city, axis=1).to_list()}

    return {}


# %%
if __name__ == '__main__':

    app.run(debug=True)
