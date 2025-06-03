import sys,os
sys.path.insert(0,'/home/hadoop/myenv/lib/python3.12/site-packages')

import pandas as pd 
import numpy as np
import tensorflow as tf

def create_time_features(df:pd.DataFrame,window_size=30):
    """Create time-based features for each city in the DataFrame.
    This function processes the input DataFrame to create time-based features for each city.
    It extracts features such as day of year, day of week, and hour of day, and creates a sliding window of specified size.

    :param df: DataFrame containing the data with 'city_id' and 'hour' columns.
    :type df: pd.DataFrame
    :param window_size: the size of the sliding window to create features, defaults to 30
    :type window_size: int, optional
    :return: Three numpy arrays: X (features), y (target variable), and C (city IDs).
    :rtype: tuple of np.ndarray
    :raises ValueError: if the DataFrame does not contain 'city_id' or 'hour'
    """
    X_list,y_list,C_list=[],[],[]
    for cid in df['city_id'].unique():
        df_c=df[df['city_id']==cid].sort_values('hour').reset_index(drop=True)
        x=df_c.drop(columns=['city_id','hour','Unnamed: 0','precipitacion_h','precipitacion_max','precipitacion_min']).values
        print(df_c.head())
        y=df_c['precipitacion_h'].values
        if len(df_c) < window_size:
            print(f"City {cid} has less than {window_size} records, skipping.")
        for i in range(len(df_c)-window_size):
            X_list.append(x[i:i+window_size])
            y_list.append(y[i+window_size])
            C_list.append(cid)
    return np.array(X_list),np.array(y_list),np.array(C_list)

def dataset(path='./drive/MyDrive/data_project/features/train_hourly.csv',path_validation='./drive/MyDrive/data_project/features/val_hourly.csv',window_size=30,batch_size=32):
    """Load the dataset and create TensorFlow datasets for training and validation.
    This function reads the training and validation data from CSV files, processes them to create time-based features,
    and returns TensorFlow datasets for training and validation.

    :param path: path to the training data CSV file, defaults to './drive/MyDrive/data_project/features/train_hourly.csv'
    :type path: str, optional
    :param path_validation: path to the validation data CSV file, defaults to './drive/MyDrive/data_project/features/val_hourly.csv'
    :type path_validation: str, optional
    :param window_size: the size of the sliding window to create features, defaults to 30
    :type window_size: int, optional
    :param batch_size: the size of the batches for training, defaults to 32
    :type batch_size: int, optional
    :return: Two TensorFlow datasets: one for training and one for validation.
    :rtype: tuple of tf.data.Dataset
    :raises FileNotFoundError: if the specified CSV files do not exist
    """
    df_train=pd.read_csv(path)
    df_validation=pd.read_csv(path_validation)
    X_train_f,y_train,C_train=create_time_features(df_train,window_size=window_size)
    X_val_f,y_val,C_val=create_time_features(df_validation,window_size=window_size)
    print(f"X shape: {X_train_f.shape}, y shape: {y_train.shape}, C shape: {C_train.shape}")
    print(f"Validation X shape: {X_val_f.shape}, y shape: {y_val.shape}, C shape: {C_val.shape}")
    X_train=tf.convert_to_tensor(X_train_f,dtype=tf.float32)
    y_train=tf.convert_to_tensor(y_train,dtype=tf.float32)
    C_train=tf.convert_to_tensor(C_train,dtype=tf.int32)
    X_val=tf.convert_to_tensor(X_val_f,dtype=tf.float32)
    y_val=tf.convert_to_tensor(y_val,dtype=tf.float32)
    C_val=tf.convert_to_tensor(C_val,dtype=tf.int32)
    train_dataset=tf.data.Dataset.from_tensor_slices(((X_train,C_train),y_train))
    train_dataset=train_dataset.batch(32).prefetch(tf.data.AUTOTUNE)
    vali_dataset=tf.data.Dataset.from_tensor_slices(((X_val,C_val),y_val))
    vali_dataset=vali_dataset.batch(32).prefetch(tf.data.AUTOTUNE)     
    for (batch,y) in train_dataset.take(1):
        print(f"Batch shape: {batch[0].shape}, City shape: {y.shape}")
    return train_dataset,vali_dataset
    
if __name__ == "__main__":
    train,val=dataset()
    print(len(train),len(val))