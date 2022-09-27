import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser

def fun():
    #JSON to CSV
    with open('input.json') as json_file:
        jsondata = json.load(json_file)

        data_file = open('data.csv', 'w', newline='')
        csv_writer = csv.writer(data_file)

        count = 0
        for data in jsondata:
            if count == 0:
                header = data.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(data.values())

        data_file.close()

    
    #CSV file processing
    required_columns = ['DateString','Scrip','Open','Close','Volume','DateEpoch']

    df = pd.read_csv("data.csv", usecols=required_columns)
    df=df.reindex(columns=required_columns)
    # output = df.head()
    output = df.loc[df['Scrip'] == '1JANATAMF']

    # x = output.values.tolist()
    date_epochs = output['DateEpoch'].values.tolist()
    # print(date_epochs[0])

    diff = [0]
    diff_days = [0]
    original = ["Yes"]

    for i in range(1,len(output)):
        diff.append(date_epochs[i] - date_epochs[i-1])
        day = int(diff[i]/(24*60*60*1000))
        diff_days.append(day)
        original.append("Yes")

    output["Difference"] = diff
    output["Days"] = diff_days
    output["Original"] = original

    # output = output.head(10)
    # output = output.tail(10)

    length = len(output)

    i = 1

    # print(output.at[2836,"DateString"])

    while i != length:
        diff_day = int(output.at[i,"Days"])
        original_check = output.at[i-1,"Original"]

        if diff_day > 1 and original_check=="Yes":
            # index = i

            dt_str = output.at[i-1,"DateString"]
            # print(type(dt_str))

            # dt_obj = datetime.strptime(dt_str, '%d/%m/%y %H:%M:%S')
            dt_obj = parser.parse(dt_str, dayfirst=True)
            # dt_obj = dt_str

            early_closing = output.at[i-1,"Close"]
            late_opening = output.at[i,"Open"]
            early_volume = output.at[i-1,"Volume"]
            late_volume = output.at[i,"Volume"]

            closing_interpolation_factor = (late_opening - early_closing)/diff_day
            volume_interpolation_factor = (late_volume - early_volume)/diff_day
            interpolated_closing = 0
            interpolated_volume = 0

            # print(i)
            # print(late_opening)
            # print(early_closing)
            # print(closing_interpolation_factor)

            for j in range(diff_day-1):
                index = i+j
                dt_obj = dt_obj + timedelta(days=1)
                # date = dt_obj
                date = dt_obj.strftime("%d/%m/%Y %H:%M:%S")

                scrip = output.at[i, "Scrip"]
                interpolated_closing = early_closing + closing_interpolation_factor*(j+1)
                interpolated_volume = early_volume + volume_interpolation_factor*(j+1)
                date_epoch = output.at[i-1,"DateEpoch"] + 24*3600*1000*(j+1)

                # row = [i, date, scrip, interpolated_closing, interpolated_closing, interpolated_volume, date_epoch]

                row = {
                    "DateString": date,
                    "Scrip": scrip,
                    "Open": interpolated_closing,
                    "Close": interpolated_closing,
                    "Volume": interpolated_volume,
                    "DateEpoch": date_epoch,
                    "Difference": 0,
                    "Days": 0,
                    "Original": "No"
                }

                first_part = output.iloc[:index]
                second_part = output.iloc[index:]

                # first_part.loc[len(df.index)] = row

                first_part = first_part.append(row, ignore_index = True)

                output = pd.concat([first_part, second_part]).reset_index(drop=True)

                # total_rows_added += 1
            
        i += 1
        length = len(output)

            


    # output = output.head()
    # output = output.iloc[:2]

    output.to_csv("output1.csv")


if __name__ == '__main__':
    fun()

