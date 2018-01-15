#!/Users/BEugeneSmith/anaconda3/bin/python3
'''
The purpose of this script is to parse relevant fields from data pulled from iTunes.
Not much manipulation is done, as iTunes produces a relatively clean stream of data
Author

'''

from subprocess import PIPE,Popen
import datetime,os,sys
import pandas as pd

class Audio(object):

    def __init__(self,config_name):
        self.today = datetime.datetime.now().date()
        self.config_name = config_name

        self._set_config()
        self.data = self._pull_data()
        self._clean_data()
        self._store_data()

    def _set_config(self):
        if self.config_name == 'podcast':
            self.audio_type = 'Podcast'
        elif self.config_name == 'music':
            self.audio_type = 'Music'
        else:
            sys.exit('unknown config used:{}'.format(self.config_name))

    def _pull_data(self):
        data_buff = Popen(['osascript pull{}Data.scpt'.format(self.audio_type)],shell=True,stdout=PIPE)
        date_cols = ['datePlayed','dateAdded','dateReleased']
        data = pd.read_table(data_buff.stdout,sep='|',parse_dates=date_cols)
        data.loc[:,'reportRunDate'] = self.today
        holidays =
        data.loc[:,'expectedDatePlayed'] = data['reportRunDate'] - datetime.timedelta(days=1)
        data.loc[:,'played_yesterday'] = pd.to_datetime(data['datePlayed']).dt.strftime('%Y%m%d') == pd.to_datetime(data['expectedDatePlayed']).dt.strftime('%Y%m%d')
        return data

    def _clean_data(self):
        self.data['release_to_play_seconds'] = (self.data['datePlayed'] - self.data['dateReleased']).dt.total_seconds()
        self.data['release_to_add_seconds'] = (self.data['dateAdded'] - self.data['dateReleased']).dt.total_seconds()
        self.data['add_to_play_seconds'] = (self.data['datePlayed'] - self.data['dateAdded']).dt.total_seconds()

    def _store_data(self):
        project_dir = '/Users/BEugeneSmith/Desktop/projects/PodcastAnalysis'
        out_file_name = '/data/{}ListeningPull{}{:02d}{:02d}.txt'.format(self.audio_type,self.today.year,self.today.month,self.today.day)
        self.data.to_csv(project_dir+out_file_name,sep='|',index=False)


if __name__ == "__main__":
    Audio(config_name='podcast')
    Audio(config_name='music')
