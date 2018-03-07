import luigi

from subprocess import PIPE,Popen
import datetime,os,sys
from pandas.tseries.holiday import USFederalHolidayCalendar
import pandas as pd

class PodcastPull(luigi.Task):
    def output(self):

        date = datetime.datetime.now()
        return luigi.LocalTarget("PodcastListeningPull{}{:02d}{:02d}.txt".format(date.year,date.month,date.day))

    def run(self):
        data_buff = Popen(['osascript pullPodcastData.scpt'],shell=True,stdout=PIPE)
        date_cols = ['datePlayed','dateAdded','dateReleased']
        data = pd.read_table(data_buff.stdout,sep='|',parse_dates=date_cols)

        with self.output().open('w') as outfile:
            outfile.write(data.to_csv(sep='|',index=False))

class MusicPull(luigi.Task):
    def output(self):

        date = datetime.datetime.now()
        return luigi.LocalTarget("MusicListeningPull{}{:02d}{:02d}.txt".format(date.year,date.month,date.day))

    def run(self):
        data_buff = Popen(['osascript pullMusicData.scpt'],shell=True,stdout=PIPE)
        date_cols = ['datePlayed','dateAdded','dateReleased']
        data = pd.read_table(data_buff.stdout,sep='|',parse_dates=date_cols)

        with self.output().open('w') as outfile:
            outfile.write(data.to_csv(sep='|',index=False))

class Audio_Manipulate(luigi.Task):

    def requires(self):
        return [MusicPull(),PodcastPull()]

    def output(self):
        pass

    def run(self):
        print('WAHOO')

    def input(self):
        return MusicPull()



if __name__ == "__main__":
    luigi.run()
    # luigi.run(["--local-scheduler"], main_task_cls=PodcastPull)
    # luigi.run(["--local-scheduler"], main_task_cls=MusicPull)
    # python3 -m luigi --module pullaudio MusicPull
    #
    # luigid
