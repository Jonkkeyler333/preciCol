from data_loader import dataset
import tensorflow as tf
from tensorflow.keras import layers, Model , optimizers
from tensorflow.keras.callbacks import EarlyStopping
import datetime
import numpy as np

print(tf.config.list_physical_devices('GPU'))

train_ds,val_ds=dataset()

n_features=26
n_cities=10
embedding_dim=16

#model

X_input=tf.keras.Input(shape=(30,n_features),name='X_input')
C_input=tf.keras.Input(shape=(1,), name='C_input')
c_embedding=layers.Embedding(input_dim=n_cities, output_dim=embedding_dim, name='city_latent_space')(C_input)
c_embedding_flat=layers.Flatten()(c_embedding)
c_embedding_seq=layers.RepeatVector(30)(c_embedding_flat)

X=layers.concatenate([X_input,c_embedding_seq],axis=-1)
rnn_out1=layers.SimpleRNN(128,return_sequences=True,name='rnn1',recurrent_dropout=0.1,dropout=0.2)(X)
rnn_out2=layers.SimpleRNN(128,return_sequences=False,name='rnn2',recurrent_dropout=0.1,dropout=0.1)(rnn_out1)
output=layers.Dense(1,activation=None,name='output')(rnn_out2)
model=Model(inputs=[X_input,C_input],outputs=output)
model.compile(optimizer=optimizers.Adam(learning_rate=1e-4,weight_decay=1e-4),loss='mse',metrics=['mae'])
model.summary()

with tf.device('/GPU:0'):
    early_stop = EarlyStopping(monitor="val_loss",patience=10,restore_best_weights=True)
    history=model.fit(train_ds,validation_data=val_ds,epochs=40,verbose=1,callbacks=[early_stop])
    
log_name = f"experiment_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
with open(log_name, "w") as f:
    f.write("Epoch\tLoss\tVal_Loss\tMAE\tVal_MAE\n")
    for epoch in range(len(history.history['loss'])):
        loss = history.history['loss'][epoch]
        val_loss = history.history['val_loss'][epoch]
        mae = history.history['mae'][epoch]
        val_mae = history.history['val_mae'][epoch]
        f.write(f"{epoch+1}\t{loss:.4f}\t{val_loss:.4f}\t{mae:.4f}\t{val_mae:.4f}\n")
        
model.save('model1_rnn.keras')