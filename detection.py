from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from keras.models import load_model
import plotly.graph_objects as go
import os


def to_timestamp(time_str):
    time_format = "%Y-%m-%dT%H:%M:%SZ"  
    datetime_obj = datetime.strptime(time_str, time_format)
    return int(datetime_obj.timestamp())

def extract_thing_property(feature):
    parts = feature.split('/')  
    if len(parts) > 1:
        return parts[0], parts[-1]
    return feature, feature  

if os.path.exists("alarm_time.txt"):
    os.remove("alarm_time.txt")

if not os.path.exists("alarm_time.txt"):
    with open("alarm_time.txt", "w") as alarm_file:
        alarm_file.write("thing,property,desc,alarm_time\n")

data = pd.read_csv('data_final.csv')
time = data['time']
original_time = data['time'].copy() 
data.drop(columns=['Unnamed: 0', 'time'], inplace=True)
model = load_model("Model_CyExGas_CyPmax_BrgDE.h5")

data_final = pd.read_csv('data_final.csv')
data_final.drop(columns=['Unnamed: 0', 'time'], inplace=True)
scaler = StandardScaler()
data_final_scaled = scaler.fit_transform(data_final)
data_final_scaled = data_final_scaled.reshape((data_final_scaled.shape[0], 1, data_final_scaled.shape[1]))

predicted_data = model.predict(data_final_scaled)
predicted_data_original = scaler.inverse_transform(predicted_data.reshape(predicted_data.shape[0], predicted_data.shape[2]))
predict = pd.DataFrame(data=predicted_data_original, columns=data_final.columns)

mae = (data - predict).abs()
dg1_columns = [col for col in mae.columns if 'DG1Thing/' in col]
dg1_stats = mae[dg1_columns].describe()
dg1_threshold = dg1_stats.loc['mean'] + 2 * dg1_stats.loc['std']
dg1_threshold = np.where(dg1_threshold < 9, 9, dg1_threshold)
dg1_threshold_df = pd.DataFrame({'name': dg1_columns, 'threshold': dg1_threshold})

de1_columns = [col for col in mae.columns if 'DE1Thing/' in col]
de1_stats = mae[de1_columns].describe()
de1_threshold = de1_stats.loc['mean'] + 2 * de1_stats.loc['std']
de1_threshold = np.where(de1_threshold < 26, 26, de1_threshold)
de1_threshold_df = pd.DataFrame({'name': de1_columns, 'threshold': de1_threshold})

threshold_df = pd.concat([dg1_threshold_df, de1_threshold_df], ignore_index=True)
mae.insert(0, 'time', time)

threshold_dict = threshold_df.set_index('name')['threshold'].to_dict()
anomalies = []

for feature in mae.columns.drop('time'):  
    for index, mae_value in mae[feature].items():
        if mae_value > threshold_dict[feature]:
            anomaly_time_str = mae.at[index, 'time']
            anomaly_timestamp = to_timestamp(anomaly_time_str)
            thing, property = extract_thing_property(feature)
            desc = "Too low" if predict.at[index, feature] > data.at[index, feature] else "Too high"
            #alarm_level = determine_alarm_level(feature, index, mae[feature])
            anomalies.append({
                'Time': anomaly_time_str,  
                'Feature': feature,
                'Value': data.at[index, feature],  
                'MAE': mae_value,
                #'Alarm_Level': alarm_level  
            })
            with open("alarm_time.txt", "a") as alarm_file:
                alarm_file.write(f"{thing},{property},{desc},{anomaly_timestamp}\n")


time_to_remove = time.iloc[0]
anomalies_df = pd.DataFrame(anomalies)
anomalies_df = anomalies_df[anomalies_df['Time'] != time_to_remove]

alarm_time_df = pd.read_csv('alarm_time.txt')
time_to_remove = to_timestamp(time_to_remove)
alarm_time_df = alarm_time_df[alarm_time_df['alarm_time'] != time_to_remove]
alarm_time_df.to_csv('alarm_time.txt', index=False, header=True)


# fig = go.Figure()
# for feature in data.columns:
#     fig.add_trace(go.Scatter(x=time, y=data[feature], mode='lines', name=feature))
# fig.add_trace(go.Scatter(x=anomalies_df['Time'], y=anomalies_df['Value'], mode='markers', name='Anomalies', marker=dict(color='red', size=10))) 
# fig.update_layout(title='Original Data with Combined Anomalies', xaxis_title='Time', yaxis_title='Value', showlegend=True)
# fig.show()


fig = go.Figure()
for feature in data.columns:
    fig.add_trace(go.Scatter(x=time, y=data[feature], mode='lines', name=feature))

for feature in anomalies_df['Feature'].unique():
    feature_anomalies = anomalies_df[anomalies_df['Feature'] == feature]
    fig.add_trace(go.Scatter(x=feature_anomalies['Time'], y=feature_anomalies['Value'], mode='markers', name=feature + ' Anomalies', marker=dict(size=10)))

fig.update_layout(title='Original Data with Combined Anomalies', xaxis_title='Time', yaxis_title='Value', showlegend=True)
fig.show()
