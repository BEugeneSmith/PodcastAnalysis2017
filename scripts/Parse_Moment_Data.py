#!/Users/BEugeneSmith/anaconda3/bin/python3
import pandas as pd
import json,datetime

class Moment_JSON(object):
    def __init__(self,moment_path):
        self.moment_path = moment_path
        self.json_data = self._load_clean_json()
        self.moment_df = self._parse_json_to_dataframe()


    def _load_clean_json(self):
        ''' opens the json output from moment, cleans it, and stores it in a dictionary '''
        data_buff = open(self.moment_path)

        all_lines = data_buff.readlines()
        all_lines = ''.join(all_lines)
        all_lines = all_lines.replace('\n','')
        return json.loads(all_lines)

    def _parse_json_to_dataframe(self):
        ''' Parse the loaded and cleaned json string from dictionary to dataframe '''
        json_dict = self.json_data['days']
        day_labels = ['date','pickups']
        records = {
            'real_datetime':[],
            'pickup_datetime':[],
            'location_accuracy':[],
            'longitude':[],
            'latitude':[]
        }

        for data_ix in range(len(json_dict)):
            real_dt = ''
            for label in day_labels:
                label_data = json_dict[data_ix][label]
                if label =='date':
                    real_dt = label_data
                else:
                    pickups = label_data
                    for pickup in pickups:
                        records['pickup_datetime'].append(pickup['date'])
                        records['location_accuracy'].append(pickup['locationAccuracyInMeters'])
                        records['longitude'].append(pickup['longitude'])
                        records['latitude'].append(pickup['latitude'])
                        records['real_datetime'].append(real_dt)
        return pd.DataFrame(records)

    def store_parsed_json(self):
        ''' save the data parsed from the data in a pipe-delimited file '''
        now = datetime.datetime.now()
        now_str = now.strftime('%Y%m%d')
        self.moment_df.to_csv('moment_data_'+now_str+'.txt',index=False,sep='|')

if __name__ == '__main__':
    moment = Moment_JSON('../data/moment.json')
    moment.store_parsed_json()
