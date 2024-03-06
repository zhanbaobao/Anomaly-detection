# -*- coding: utf-8 -*-
# This is a sample Python script.
import datetime
import json
import requests
import csv
import matplotlib.pyplot as plt
from matplotlib import dates
import matplotlib.ticker as ticker
import pandas as pd
import os


# Press Alt+Shift+X to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # URL
    url = "https://ap-southeast-1.aws.data.mongodb-api.com/app/data-zxdhn/endpoint/data/v1/action/aggregate"

    # API request headers
    headers = {
        'Content-type': 'application/json',
        'Accept': 'text/plain',
        'apiKey': 'ipfQPuBNhC2wMB9VKM4ikgCQeoXJMtl4e06bf4WsMbXvGZbyTwKtd4IHInes8z1E'
    }

    # List of Things and Properties
    data_list = [
        ["DG1Thing", "BrgDE_Temp1"],
        ["DG1Thing", "BrgDE_Temp2"],
        ["DG1Thing", "BrgDE_Temp3"],
        ["DG1Thing", "BrgDE_Temp4"],
        ["DG1Thing", "BrgDE_Temp5"],
        ["DG1Thing", "BrgDE_Temp6"],
        ["DG1Thing", "BrgDE_Temp7"],
        ["DG1Thing", "BrgDE_Temp8"],
        ["DG1Thing", "BrgDE_Temp9"],
        ["DG1Thing", "BrgDE_Temp10"],
        ["DG1Thing", "BrgDE_Temp11"],
        ["DE1Thing", "Cy1ExhGasOutletTemp"],
        ["DE1Thing", "Cy2ExhGasOutletTemp"],
        ["DE1Thing", "Cy3ExhGasOutletTemp"],
        ["DE1Thing", "Cy4ExhGasOutletTemp"],
        ["DE1Thing", "Cy5ExhGasOutletTemp"],
        ["DE1Thing", "Cy6ExhGasOutletTemp"],
        ["DE1Thing", "Cy7ExhGasOutletTemp"],
        ["DE1Thing", "Cy8ExhGasOutletTemp"],
        ["DE1Thing", "Cy9ExhGasOutletTemp"],
        ["DE1Thing", "Cyl1_Pmax"],
        ["DE1Thing", "Cyl2_Pmax"],
        ["DE1Thing", "Cyl3_Pmax"],
        ["DE1Thing", "Cyl4_Pmax"],
        ["DE1Thing", "Cyl5_Pmax"],
        ["DE1Thing", "Cyl6_Pmax"],
        ["DE1Thing", "Cyl7_Pmax"],
        ["DE1Thing", "Cyl8_Pmax"],
        ["DE1Thing", "Cyl9_Pmax"],
        ["DE1Thing", "Load"],
        ["DE1Thing", "Power"]
    ]

    # Input information
    ship_number = "HDGRC7F_W"
    from_date = datetime.datetime(2024, 1, 2, 3, 0, 0)
    to_date = datetime.datetime(2024, 1, 2, 9, 0, 0)

    # Constructing the $or clause dynamically based on data_list
    or_clauses = []
    for thing, prop in data_list:
        or_clauses.append(
            {
                "$and": [
                    {"metadata.thing": {"$eq": thing}},
                    {"metadata.property": {"$eq": prop}}
                ]
            }
        )
        
    body = {
        "collection": "tag_datas",
        "database": "dev-hipom",
        "dataSource": "aiins-tv-test",
        "pipeline": [
            {
                "$match": {
                    "$and": [
                        {"metadata.ship_number": {"$eq": ship_number}},
                        {"$or": or_clauses},
                        {
                            "src_time": {
                                "$gte": {"$date": from_date.isoformat() + ".000Z"},
                                "$lt": {"$date": to_date.isoformat() + ".000Z"}
                            }
                        }
                    ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "ship_number": "$metadata.ship_number",
                        "thing": "$metadata.thing",
                        "property": "$metadata.property",
                        "year": {"$year": "$src_time"},
                        "month": {"$month": "$src_time"},
                        "day": {"$dayOfMonth": "$src_time"},
                        "hour": {"$hour": "$src_time"},
                        "minute": {"$minute": "$src_time"}  # Add minute information
                    },
                    "averageValue": {"$avg": "$value"}
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
            {
                "$group": {
                    "_id": {
                        "ship_number": "$_id.ship_number",
                        "thing": "$_id.thing",
                        "property": "$_id.property"
                    },
                    "items": {
                        "$push": {
                            "year": "$_id.year",
                            "month": "$_id.month",
                            "day": "$_id.day",
                            "hour": "$_id.hour",
                            "minute": "$_id.minute",
                            "value": "$averageValue"
                        }
                    }
                }
            }
        ]
    }
    # API call
    response = requests.post(url, headers=headers, json=body)
    response_dict = response.json()
    response_string = json.dumps(response_dict)
    # print("response = ", response_string)

    # 2 arrays
    datas = []
    # First line
    date_items = ["time"]

    cursor_date = from_date
    while cursor_date < to_date:
        date_item = cursor_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        date_items.append(date_item)
        cursor_date += datetime.timedelta(minutes=1)
    datas.append(date_items)
    
    documents_list = response_dict["documents"]
    for index, document_object in enumerate(documents_list):
        _id_object = document_object["_id"]
        _ship_number = _id_object["ship_number"]
        _thing = _id_object["thing"]
        _property = _id_object["property"]
        items_list = document_object["items"]

        cursor_date = from_date
        line_items = [_thing + "/" + _property]
        for idx, item_object in enumerate(items_list):
            _year = item_object["year"]
            _month = item_object["month"]
            _day = item_object["day"]
            _hour = item_object["hour"]
            _minute = item_object["minute"]
            _value = item_object["value"]

            # Convert to datetime for hourly comparison
            _date = datetime.datetime(
                _year, _month, _day,
                _hour, _minute, 0, 0,
                from_date.tzinfo)

            # Iterate through each minute until _date
            while cursor_date <= _date:
                # Check if cursor_date matches the current minute in _date
                if (cursor_date.year == _date.year) \
                        and (cursor_date.month == _date.month) \
                        and (cursor_date.day == _date.day) \
                        and (cursor_date.hour == _date.hour) \
                        and (cursor_date.minute == _date.minute):
                    # If data is present, set the value
                    line_items.append(_value)
                else:
                    # If data is not present, set an empty string
                    line_items.append("")
                cursor_date += datetime.timedelta(minutes=1)

        empty_count = 0
        # Loop through line_items to count empty strings
        for value in line_items:
            if value == "":
                empty_count += 1
        datas.append(line_items)

    # # Save datas to a CSV file
    csv_file_path = "datas.csv"

    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(datas)

    # Extracting x-axis (time) and y-axis (values) data
    data_series = datas[1:]  # Remaining lists are data series
    time_labels = date_items[1:len(data_series[0])]
    
    # Creating a plot for each data series
    plt.figure(figsize=(11, 7))
    for data_series_item in data_series:
        series_label = data_series_item[0]
        series_data = [float(value) if value != '' else None for value in data_series_item[1:]]  # Convert values to float

        # Plotting the data series
        plt.plot(time_labels, series_data, label=series_label)
    
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(nbins=10))  # Adjust 'nbins' as needed for clarity

    # Adding labels and legend
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title('Data Series Chart')
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    plt.locator_params(axis='x', nbins=len(time_labels) // 20)  # Adjust the number of ticks

    # Display the plot
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()

    #Convert data.csv format
    df = pd.read_csv('datas.csv')
    df_transposed = df.T
    df_transposed = df_transposed.dropna()
    df_transposed = df_transposed[(df_transposed != 0).all(axis=1)]
    df_transposed.to_csv('datas_transposed.csv', header=False, index=True)

    data_final = pd.read_csv('datas_transposed.csv')
    new_column_order = ['time',
        'DG1Thing/BrgDE_Temp1',
        'DG1Thing/BrgDE_Temp2',
        'DG1Thing/BrgDE_Temp3',
        'DG1Thing/BrgDE_Temp4',
        'DG1Thing/BrgDE_Temp5',
        'DG1Thing/BrgDE_Temp6',
        'DG1Thing/BrgDE_Temp7',
        'DG1Thing/BrgDE_Temp8',
        'DG1Thing/BrgDE_Temp9',
        'DG1Thing/BrgDE_Temp10',
        'DG1Thing/BrgDE_Temp11',
        'DE1Thing/Cy1ExhGasOutletTemp',
        'DE1Thing/Cy2ExhGasOutletTemp',
        'DE1Thing/Cy3ExhGasOutletTemp',
        'DE1Thing/Cy4ExhGasOutletTemp',
        'DE1Thing/Cy5ExhGasOutletTemp',
        'DE1Thing/Cy6ExhGasOutletTemp',
        'DE1Thing/Cy7ExhGasOutletTemp',
        'DE1Thing/Cy8ExhGasOutletTemp',
        'DE1Thing/Cy9ExhGasOutletTemp',
        'DE1Thing/Cyl1_Pmax',
        'DE1Thing/Cyl2_Pmax',
        'DE1Thing/Cyl3_Pmax',
        'DE1Thing/Cyl4_Pmax',
        'DE1Thing/Cyl5_Pmax',
        'DE1Thing/Cyl6_Pmax',
        'DE1Thing/Cyl7_Pmax',
        'DE1Thing/Cyl8_Pmax',
        'DE1Thing/Cyl9_Pmax',
        'DE1Thing/Load',
        'DE1Thing/Power'
    ]

    data_final = data_final[new_column_order]
    data_final.to_csv('data_final.csv', header=True, index=True)

    # File path for the file to be deleted
    file_path = 'datas_transposed.csv'

    # Deleting the file if it exists
    if os.path.exists(file_path):
        os.remove(file_path)
