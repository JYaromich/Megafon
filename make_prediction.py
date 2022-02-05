import luigi
import numpy as np
import pandas as pd
import pickle

from pip import main
import const

import dask.dataframe as dd

from datetime import datetime

from src.pipeline import *


class TaskMergeData(luigi.Task):
    
    path_to_file = luigi.Parameter()
    path_to_save_file = luigi.Parameter()
    path_to_features_file = luigi.Parameter()
    
    def output(self):
        return luigi.LocalTarget(self.path_to_save_file)
    
    def reduce_mem_usage(df):
        """ iterate through all the columns of a dataframe and modify the data type
            to reduce memory usage.
        """
        start_mem = df.memory_usage(deep=True).sum() / 1024 ** 2
        print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))

        for col in df.columns:
            col_type = df[col].dtype

            if col_type != object:
                c_min = df[col].min()
                c_max = df[col].max()
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)
                else:
                    if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
            else:
                df[col] = df[col].astype('category')

        end_mem = df.memory_usage(deep=True).sum() / 1024 ** 2
        print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
        print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))

        return df    
    
    def run(self):
        # read input data
        data = pd.read_csv(self.path_to_file, index_col=0)
        features = dd.read_csv(self.path_to_features_file, sep='\t')
        features = features.drop(columns=['Unnamed: 0'])
        
        # handle buy_time
        data.buy_time = data.buy_time.apply(lambda x: datetime.fromtimestamp(x))
        features.buy_time = features.buy_time.apply(lambda x: datetime.fromtimestamp(x), meta=('datetime64[ns]'))
        
        # merge input data
        merge_data = pd.merge_asof(
            left=data.sort_values(by='buy_time', ascending=True), 
            right=features.sort_values(
                by='buy_time', ascending=True).compute(),
            by=['id'],
            on=['buy_time'],
            direction='nearest'
        )
        
        columns_list = merge_data.columns.tolist()
        columns_list.remove('buy_time')
        merge_data[columns_list] = self.reduce_mem_usage(merge_data[columns_list])
        
        with self.output().open('w') as f:
            print(merge_data.to_csv(index=False), file=f)
            
# class TaskTrainModel(luigi.Task):
#     path_to_file = luigi.Parameter()
#     path_to_save_file = luigi.Parameter()
#     path_to_features_file = luigi.Parameter()
#     path_to_save_model = luigi.Parameter()

#     def output(self):
#         return luigi.LocalTarget(self.path_to_save_model)
    
#     def  requires(self):
#         return TaskMergeData(path_to_file=str(self.path_to_train_data), 
#                              path_to_save_file=str(self.path_to_save_merge_train_data),
#                              path_to_features_file=str(self.path_to_features_file))
    
#     def run(self):
#         model = pickle.load(open(self.path_to_model, 'rb'))
        
#         with self.input()[0].open('r') as train_file:
#             data_train = pd.read_csv(train_file)

#         model.fit(data_train.drop(self.target_name, axis=1), data_train[self.target_name])
        
#         pickle.dump(
#             model,
#             self.output().open('wb')
#         )


# class TaskPrediction(luigi.Task):
    
    # path_to_train_data = luigi.Parameter()
    # path_to_test_data = luigi.Parameter()
    # path_to_save_merge_train_data = luigi.Parameter()
    # path_to_save_merge_test_data = luigi.Parameter()
    
    # path_to_features_file = luigi.Parameter()
    
    # path_to_save_prediction = luigi.Parameter()
    
    # path_to_model = luigi.Parameter()
    # target_name = luigi.Parameter()
    
    # def output(self):
    #     return luigi.LocalTarget(self.path_to_save_prediction)

    # def requires(self):
    #     return [
    #         TaskTrainModel(path_to_file=str(self.path_to_train_data),
    #                        path_to_save_file=str(self.path_to_save_merge_train_data),
    #                        path_to_features_file=str(self.path_to_features_file),
    #                        path_to_save_model=str(self.path_to_model)
    #         ),
    #         TaskMergeData(path_to_file=str(self.path_to_test_data), 
    #                       path_to_save_file=str(self.path_to_save_merge_test_data ),
    #                       path_to_features_file=str(self.path_to_features_file)),
    #     ]

    # def run(self):
    #     # with self.input()[0].open('rb') as m:
    #     #     model = pickle.load(m, encoding="utf8")
        
    #     model = pickle.load(open(self.path_to_model, 'rb'))
               
    #     with self.input()[1].open('r') as test_file:
    #         data_test = pd.read_csv(test_file)
        
    #     with self.output().open('w') as f:
    #         print(
    #             pd.concat([
    #                 pd.DataFrame(
    #                     data=model.predict(data_test),
    #                     columns=[const.TARGET_COLUMN_NAME]
    #                 ),
    #                 data_test[['buy_time', 'id', 'vas_id']],
    #             ], axis=1).to_csv(),
    #             file=f
    #         )


class TaskPrediction(luigi.Task):
    
    
    path_to_train_data = luigi.Parameter()
    path_to_test_data = luigi.Parameter()
    path_to_save_merge_train_data = luigi.Parameter()
    path_to_save_merge_test_data = luigi.Parameter()
    
    path_to_features_file = luigi.Parameter()
    
    path_to_save_prediction = luigi.Parameter()
    
    path_to_model = luigi.Parameter()
    target_name = luigi.Parameter()
    
    def output(self):
        return luigi.LocalTarget(self.path_to_save_prediction)

    def requires(self):
        return [
            TaskMergeData(path_to_file=str(self.path_to_train_data), 
                        path_to_save_file=str(self.path_to_save_merge_train_data),
                        path_to_features_file=str(self.path_to_features_file)),
            TaskMergeData(path_to_file=str(self.path_to_test_data), 
                        path_to_save_file=str(self.path_to_save_merge_test_data ),
                        path_to_features_file=str(self.path_to_features_file)),
        ]

    def run(self):
        model = pickle.load(open(self.path_to_model, 'rb'))
        
        with self.input()[0].open('r') as train_file:
            data_train = pd.read_csv(train_file)
        
        with self.input()[1].open('r') as test_file:
            data_test = pd.read_csv(test_file)
            
        
        model.fit(data_train.drop(self.target_name, axis=1), data_train[self.target_name])
        
        with self.output().open('w') as f:
            print(
                pd.concat([
                    pd.DataFrame(
                        data=model.predict(data_test),
                        columns=[const.TARGET_COLUMN_NAME]
                    ),
                    data_test[['buy_time', 'id', 'vas_id']],
                ], axis=1).to_csv(),
                file=f
            )
            
            

if __name__ == '__main__':
    luigi.build([
        TaskPrediction(
            path_to_train_data=str(const.TRAIN_PATH),
            path_to_test_data=str(const.TEST_PATH),
            path_to_save_merge_train_data=str(const.MERGE_TRAIN_PATH),
            path_to_save_merge_test_data=str(const.MERGE_TEST_PATH),
            
            path_to_features_file=str(const.FEATURES_PATH),
            
            path_to_save_prediction=str(const.PATH_TO_SAVE_PREDICTION),
            
            path_to_model=str(const.PATH_TO_SAVE_MODEL),
            target_name=str(const.TARGET_COLUMN_NAME)
        )
    ])