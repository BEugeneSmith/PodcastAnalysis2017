from datetime import *
import pandas as pd
import glob,os


class AudioAnalysisInput(object):
    # def __init__(self,load_num='',audio_type="Podcast",start='',end=''):
    def __init__(self,audio_type="Podcast",start='',end=''):
        # supply datetime objects

        self.audio_type = "Podcast"
        self.end = datetime.now() if end =='' else end
        # load_num = 7 if (load_num == '' else load_num
        self.start = self.end - timedelta(days=7) if start =='' else start
        self._data,self.loaded_files = self._batch_load_files()
        self._pcast_freq_map = self._load_podcast_freq_map()
        self.data = self._data.merge(self._pcast_freq_map,how='left')
        self._clean_data()
        self._proess_data_categories()
        self._process_data_skips()
        self._process_weeks()
        self._gen_listen_flags()

        nulls = self.data[self.data['frequency'].isnull()]
        if len(nulls) > 0:
            nulls.to_excel('empty ones.xlsx')
            os.sys.exit('there were some shows that were not encoded, check "empty ones.xlsx"')

    def load_file(self,file_str):
        ''' load an individual podcast listening file '''
        data = pd.read_csv(file_str,sep="|")
        data.loc[:,'datePlayed'] = pd.to_datetime(data['datePlayed'])
        data.loc[:,'dateAdded'] = pd.to_datetime(data['dateAdded'])
        data.loc[:,'dateReleased'] = pd.to_datetime(data['dateReleased'])
        return data

    def _load_podcast_freq_map(self):
        ''' load the frequency mapping file '''
        try:
            return pd.read_csv('podcast_frequency_mapping.csv')
        except:
            return pd.read_csv('Analysis/podcast_frequency_mapping.csv')

    def _batch_load_files(self):
        loaded_file_paths = []
        loaded_files = []

        dates_to_load = pd.DatetimeIndex(start=self.start,end=self.end,freq="D")
        for date_to_load in dates_to_load:

            date_path = "data/{}ListeningPull{}.txt".format(self.audio_type,date_to_load.strftime("%Y%m%d"))
            if not os.path.exists(date_path):
                continue
            loaded_df = self.load_file(date_path)

            loaded_files.append(loaded_df)
            loaded_file_paths.append(date_path)
        return pd.concat(loaded_files).reset_index(drop=True),loaded_file_paths

    def _clean_data(self):
        self.data.loc[:,'duration'] = self.data[['duration','default_duration']].apply(lambda x: x['default_duration'] if x['duration'] == 'missing value' else x['duration'],axis=1)
        self.data['duration'] = self.data['duration'].astype(float)

    def _process_weeks(self):
        def gen_date_code(x):
            return "{}_{:02d}".format(x.year,x.week)
        self.data['weekPlayed'] = self.data['datePlayed'].apply(gen_date_code)
        self.data['weekAdded'] = self.data['dateAdded'].apply(gen_date_code)
        self.data['weekReleased'] = self.data['dateReleased'].apply(gen_date_code)

    def _gen_listen_flags(self):
        self.data['played_same_week_ind'] = self.data['weekPlayed']==self.data['weekReleased']
        self.data['played_same_date_ind'] = self.data['datePlayed'].dt.date==self.data['dateReleased'].dt.date

    def _process_data_skips(self):
        self.data.loc[:,'single_skip_ind'] = self.data['duration'] - ((pd.to_datetime(self.data['datePlayed'])-pd.to_datetime(self.data['dateAdded']))).dt.seconds
        self.data['single_skip_ind'] = self.data['single_skip_ind'].apply(lambda x: x>0)
        self.data['batch_skip_ind'] = (self.data['datePlayed'].duplicated() & self.data['playedCount']==1) | (self.data['datePlayed'].duplicated())

    def _proess_data_categories(self):
        # process mappings
        time_mapping = {
            7:'01_earlymorning',
            10:'02_morning',
            14:'03_midday',
            17:'04_afternoon',
            20:'05_evening',
        }

        dur_mapping = {
            60*10:'01_short',
            60*30:'02_medium',
        }

        # mapping functions
        def get_PoD(x):
            ''' get part of day for a given timestamp '''
            hours = sorted(time_mapping.keys())
            for hour in hours:
                if x <= hour:
                    return time_mapping[hour]
            # assume late evening if no other time is chosen
            return '06_lateevening'

        def get_dur(x):
            ''' get bin for a given duration'''
            lengths = sorted(dur_mapping.keys())
            for length in lengths:
                if x <= length:
                    return dur_mapping[length]
            # retiurn long if neither short nor medium
            return '03_long'

        # actual processing f
        self.data.loc[:,'PoD_played'] = self.data['datePlayed'].dt.hour.apply(get_PoD)
        self.data.loc[:,'dur_bin'] = self.data['duration'].apply(get_dur)

class AudioAnalysisLogic(AudioAnalysisInput):
    # def __init__(self,load_num=5,audio_type="Podcast",start='',end=''):
    #     AudioAnalysisInput.__init__(self,load_num=load_num,audio_type=audio_type,start=start,end=end)
    def __init__(self,audio_type="Podcast",start='',end=''):
        AudioAnalysisInput.__init__(self,audio_type=audio_type,start=start,end=end)
        # self._test_filtering()
        self._summ_by_pod_album()
        self._summ_by_period_catg()
        self._summ_by_dur_bin()

    def _test_filtering(self):
        # FOR TESTING
        daily_pods = self.data[self.data['frequency']=='d']
        non_daily_pods = self.data[self.data['frequency']!='d']
        self.data = non_daily_pods

    def _summ_by_pod_album(self):
        self.album_pod_by_PoD_cnt = self.data.pivot_table(index=['album'],columns=['PoD_played'],values='frequency',aggfunc='count').fillna(0)
        cols_to_split = list(self.album_pod_by_PoD_cnt.columns)

        self.album_pod_by_PoD_cnt.loc[:,'listens_for_period'] = self.album_pod_by_PoD_cnt.sum(axis=1)
        self.album_pod_by_PoD_cnt.reset_index(inplace=True,drop=False)
        self.album_pod_by_PoD_cnt.sort_values(ascending=False,by='listens_for_period',inplace=True)

        self.album_pod_by_PoD_pct = pd.DataFrame({col+'_pct':self.album_pod_by_PoD_cnt[col]/self.album_pod_by_PoD_cnt['listens_for_period'] for col in cols_to_split})
        self.album_pod_by_PoD_pct['album'] = self.album_pod_by_PoD_cnt['album']

    def _summ_by_period_catg(self):
        self.catg_pod_by_PoD_cnt = self.data.pivot_table(index=['category'],columns=['PoD_played'],values='frequency',aggfunc='count').fillna(0)
        cols_to_split = list(self.catg_pod_by_PoD_cnt.columns)

        self.catg_pod_by_PoD_cnt.loc[:,'listens_for_period'] = self.catg_pod_by_PoD_cnt.sum(axis=1)
        self.catg_pod_by_PoD_cnt.reset_index(inplace=True,drop=False)
        self.catg_pod_by_PoD_cnt.sort_values(ascending=False,by='listens_for_period',inplace=True)

        self.catg_pod_by_PoD_pct = pd.DataFrame({col+'_pct':self.catg_pod_by_PoD_cnt[col]/self.catg_pod_by_PoD_cnt['listens_for_period'] for col in cols_to_split})
        self.catg_pod_by_PoD_pct['category'] = self.catg_pod_by_PoD_cnt['category']

    def _summ_by_dur_bin(self):
        self.binDur_pod_by_PoD_cnt = self.data.pivot_table(index=['dur_bin'],columns=['PoD_played'],values='frequency',aggfunc='count').fillna(0)
        cols_to_split = list(self.binDur_pod_by_PoD_cnt.columns)

        self.binDur_pod_by_PoD_cnt.loc[:,'listens_for_period'] = self.binDur_pod_by_PoD_cnt.sum(axis=1)
        self.binDur_pod_by_PoD_cnt.reset_index(inplace=True,drop=False)
        self.binDur_pod_by_PoD_cnt.sort_values(ascending=False,by='listens_for_period',inplace=True)


        self.binDur_pod_by_PoD_pct = pd.DataFrame({col+'_pct':self.binDur_pod_by_PoD_cnt[col]/self.binDur_pod_by_PoD_cnt['listens_for_period'] for col in cols_to_split})

        self.binDur_pod_by_PoD_pct['dur_bin'] = self.binDur_pod_by_PoD_cnt['dur_bin']

class AudioAnalysisOutput(AudioAnalysisLogic):
    pass

def main():
    pass

if __name__ == "__main__":
    pass
