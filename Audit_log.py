import pandas as pd
import os.path
class AuditLog():
    def __init__(self):
        pass

    def get_dataframe(self):
        data = pd.read_json('file_path/audit.json')
        return data

    def process_dataframe(self):
        df = self.get_dataframe()
        
        df = df.reset_index(drop=True)
        df['Timestamp'] = pd.to_datetime(df['modified_dts'])                # Convert object dates to timestamp format
        df = df.sort_values(by=['SUBJID', 'Timestamp'])
        df = df.groupby('SUBJID').shift(-1)
        df = df.head(2).dropna()
        print('\n Actaul Data Frame \n',df)
        print('\n')

        df2 = df.T                                                          # Transpose dataframe
        df2.columns = ['previous_data','current_data']
        
        df2 = df2.drop(df2[df2['previous_data']==df2['current_data']].index)
        df2 = self.drop_columns(df2)
        print('Dataframe after proccess \n ',df2,'\n')

    def drop_columns(self,df):
        return df.drop(['modified_dts','Timestamp'])                        # Removes the dates columns

    def main(self):
        self.process_dataframe()

'''data = '[{"id": 1, "data1": "value1", "data2": "value1", "data3": "value1",  "Timestamp": "2018-07-14 6:46:04"},  \
        {"id": 1, "data1": "value2", "data2": "value1", "data3": "value1",  "Timestamp": "2018-08-06 11:34:47"},  \
        {"id": 1, "data1": "value2", "data2": "value1", "data3": "value2",  "Timestamp": "2018-08-06 11:33:47"},  \
        {"id": 1, "data1": "value2", "data2": "value1", "data3": "value2",  "Timestamp": "2018-08-06 11:33:47"},  \
        {"id": 2, "data1": "value2", "data2": "value1", "data3": "value2",  "Timestamp": "2018-08-06 11:33:47"},  \
        {"id": 2, "data1": "value2", "data2": "value1", "data3": "value2",  "Timestamp": "2018-08-06 11:33:47"}]'''
Audit = AuditLog()
Audit.main()
