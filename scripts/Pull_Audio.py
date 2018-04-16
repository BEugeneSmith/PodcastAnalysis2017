#!/Users/BEugeneSmith/anaconda3/bin/python3
'''
The purpose of this script is to parse relevant fields from data pulled from iTunes.
Not much manipulation is done, as iTunes produces a relatively clean stream of data
Author

'''

from subprocess import PIPE,Popen
import datetime,os,sys
from pandas.tseries.holiday import USFederalHolidayCalendar
import pandas as pd

class Audio(object):

    def __init__(self,config_name):
        self.today = datetime.datetime.now().date()
        self._holidays = USFederalHolidayCalendar()
        self.config_name = config_name

        self._set_config()
        self.data = self._pull_data()
        self._clean_data()
        self._amend_data()
        self._store_data()

    def _set_config(self):
        ''' determine which apple script to run, podcast or music '''
        if self.config_name == 'podcast':
            self.audio_type = 'Podcast'
        elif self.config_name == 'music':
            self.audio_type = 'Music'
        else:
            sys.exit('unknown config used:{}'.format(self.config_name))

    def _pull_data(self):
        ''' run applescripts and do some initial parsing/categorizing '''

        data_buff = Popen(['osascript pull{}Data.scpt'.format(self.audio_type)],shell=True,stdout=PIPE)
        date_cols = ['datePlayed','dateAdded','dateReleased']
        data = pd.read_table(data_buff.stdout,sep='|',parse_dates=date_cols)
        data.loc[:,'reportRunDate'] = self.today
        if self.audio_type == "Music":
            # The Bonobos track "Flowers" generates an error; as far as I know it's the only album that causes that error
            data['dateReleased'] = data['dateReleased'].apply(lambda x: x if x != "missing value" else 'Monday, December 31, 2012 at 18:00:00')
            data['dateReleased'] = pd.to_datetime(data['dateReleased'])

        return data

    def _clean_data(self):
        ''' remove unwanted records '''

        # determine whether a track was played yesterday
        self.data.loc[:,'expectedDatePlayed'] = self.data['reportRunDate'] - datetime.timedelta(days=1)
        self.data.loc[:,'played_yesterday_ind'] = pd.to_datetime(self.data['datePlayed']).dt.strftime('%Y%m%d') == pd.to_datetime(self.data['expectedDatePlayed']).dt.strftime('%Y%m%d')
        self.data = self.data[self.data['played_yesterday_ind']==True]
        self.data.drop(['played_yesterday_ind','expectedDatePlayed'],inplace=True,axis=1)

    def _amend_data(self):
        ''' add fields that will be helpful in categorization down the line '''

        # create indicators for holiday and weekend play
        self.data.loc[:,'played_weekend_ind'] = self.data['datePlayed'].apply(lambda x: x.weekday() in [5,6])
        self.data.loc[:,'played_holiday_ind'] = self.data['datePlayed'].apply(lambda x: x.weekday() in self._holidays.holidays())

        # pre-calclulate critical time diffs in seconds
        self.data['release_to_play_seconds'] = (self.data['datePlayed'] - pd.to_datetime(self.data['dateReleased'])).dt.total_seconds()
        self.data['release_to_add_seconds'] = (self.data['dateAdded'] - pd.to_datetime(self.data['dateReleased'])).dt.total_seconds()
        self.data['add_to_play_seconds'] = (self.data['datePlayed'] - self.data['dateAdded']).dt.total_seconds()


    def _store_data(self):
        ''' save newly generated data to the data folder '''
        timestr = self.today.strftime('%Y%m%d')
        project_dir = '/Users/BEugeneSmith/Desktop/projects/PodcastAnalysis'
        out_file_name = 'data/{}ListeningPull{}.txt'.format(self.audio_type,timestr)
        self.data.to_csv(os.path.join(project_dir,out_file_name),sep='|',index=False)

if __name__ == "__main__":
    Audio(config_name='podcast')
    Audio(config_name='music')
