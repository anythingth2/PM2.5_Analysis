# %%
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from keras.models import load_model
from sklearn.externals import joblib
# %%
tgr_df = pd.read_csv('dataset/tgr_sensor.csv')

ex_region_df = pd.read_csv('dataset/berkeleyearth/region.csv')
ex_region_df.rename({'Unnamed: 0': 'id'}, axis=1, inplace=True)
ex_sensor_df = pd.read_csv('dataset/berkeleyearth/sensor.csv')
ex_sensor_df.rename({'Unnamed: 0': 'id'}, axis=1, inplace=True)
ex_sensor_df['timestamp'] = pd.to_datetime(ex_sensor_df['timestamp'])
ex_sensor_df = ex_sensor_df.set_index('city')
# %%

# %%


def daily_mean_interpolate(df):
    df = df.drop('id', axis=1).set_index('timestamp').resample('D').mean()
    df['pm'] = df['pm'].interpolate()
    return df.reset_index()


ex_daily_sensor_df = ex_sensor_df.groupby('city').apply(daily_mean_interpolate)
# %%
scaler = joblib.load('scaler.pkl')
# %%

# %%
model = load_model('model.h5')
model.summary()

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
    return sensor_df.apply(lambda row: {'pm25': row['pm'], 'timestamp': row['timestamp']}, axis=1).to_list()


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


ex_region_df.apply(create_hourly_city, axis=1).to_list()
# %%

# %%
ex_region_df.apply(create_daily_city,  axis=1).to_list()
