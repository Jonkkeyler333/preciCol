# PreciCol - Proyecto Final Analisis de datos a gran escala 

Este proyecto aborda la predicción de precipitación horaria para diez ciudades de Colombia, utilizando un pipeline de Big Data (Spark + HDFS) para la ingesta y preprocesamiento de datos, y una red neuronal recurrente (RNN many‐to‐one con embedding de ciudad) implementada en TensorFlow/Keras para el modelado. El objetivo principal es generar pronósticos confiables para enero de 2025, basados en datos históricos de 2024.

---

## 1. Estructura de Directorios

```text
raíz/
├── data/                   ← CSVs finales de train, validation y test
│   ├── train_hourly.csv
│   ├── val_hourly.csv
│   └── test_hourly.csv
│
├── models/
│   ├── checkpoints/        ← Pesos guardados durante el entrenamiento
│   ├── imgs/               ← Imágenes (gráficos) generadas en entrenamiento/EDA
│   └── logs/               ← Registros de entrenamiento (TensorBoard, etc.)
│
├── notebooks/              
│   ├── EDA.ipynb           ← Notebook de Análisis Exploratorio de Datos
│   └── inferencia.ipynb    ← Notebook para pruebas de inferencia interactiva
│
├── src/                    
│   ├── data/               
│   │   ├── extract_data.py       ← Descarga incremental y volcado a HDFS / Parquet
│   │   ├── extract_data_test.py  ← Script para extraer un subconjunto de prueba
│   │   ├── inspect_data.py       ← Inspección rápida de esquemas y estadísticas
│   │   ├── join_data.py          ← Unión de CSVs parciales en un solo Parquet
│   │   └── preprocessing.py      ← Limpieza inicial y detección de outliers
│   │
│   ├── features/            
│   │   ├── data_enrichment.py    ← Enriquecimiento con API externa (Meteostat, etc.)
│   │   ├── features.py           ← Script principal de ingeniería de características (Spark → Pandas)
│   │   └── features_test.py      ← Versión de features unitarias para dataset de prueba
│   │
│   └── src/                  ← (Código de modelado)
│       ├── data_loader.py        ← Crea tf.data.Dataset a partir de CSVs de features  
│       ├── train.py              ← Entrena la RNN (lee dataset, compila, fit + callbacks)
│      └── plot_train.py         ← Genera plots de pérdida y métricas de entrenamiento


```

## 2. Ejecución del proyecto
Cómo Ejecutar el Proyecto
2.1 Preparar el entorno

    Crear y activar el entorno virtual Python (ejemplo con venv o conda):

cd /ruta/al/repositorio
python3 -m venv myenv
source myenv/bin/activate

Instalar dependencias (en el requirements.txt o manualmente):

    pip install -r requirements.txt

        Incluye: pyspark, tensorflow, scikit-learn, pandas, numpy, matplotlib, etc.

2.2 Generar y limpiar datos (Big Data)

    Asegurarse de que HDFS está activo y disponible:

hdfs dfs -mkdir -p /user/hadoop/data_project/raw
hdfs dfs -mkdir -p /user/hadoop/data_project/features/full_data

Extraer datos crudos de la API (en el driver de Spark):

python src/data/extract_data.py

    Esto descargará lotes de 10 000 registros y los volcará como Parquet en HDFS bajo data_project/raw/.

Unir Parquet parciales y ejecutar limpieza masiva:

    python src/data/join_data.py
    python src/data/preprocessing.py

2.3 Crear features y exportar CSVs

    Encribir Parquet “raw + limpio” en HDFS:

    spark-submit src/features/features.py

        Agrupa lecturas cada hora y calcula estadísticas (suma de precipitación, medias, mínimos, máximos).

        Extrae variables temporales y codifica sinus/cosinus.

        Divide en train y val, aplica StandardScaler y exporta:

            /user/hadoop/data_project/features/train_hourly.csv

            /user/hadoop/data_project/features/val_hourly.csv

            /user/hadoop/data_project/features/train_hourly_features.csv

            /user/hadoop/data_project/features/train_hourly_target.csv

            /user/hadoop/data_project/features/val_hourly_features.csv

            /user/hadoop/data_project/features/val_hourly_target.csv

2.4 Entrenamiento del modelo

    Ejecutar el script de entrenamiento:

python src/src/train.py 

    Se guarda automáticamente el mejor checkpoint en models/checkpoints/.

    Se generan logs en models/logs/ para TensorBoard.

    Al finalizar, el modelo completo (metamodelo + pesos) se guarda en models/checkpoints/final_model.h5.

(Opcional) Verificar curvas de aprendizaje:

    python src/src/plot_train.py \
        --history-path models/logs/history.pkl \
        --output-dir  models/imgs/

2.5 Inferencia para Enero 2025

    Asegurarse de tener el CSV de inferencia (por ejemplo, /data/test_hourly.csv) con todas las features preprocesadas y normalizadas para cada hora de enero:

ls data/test_hourly.csv

Ejecutar el notebook de inferencia:

jupyter-notebook notebooks/inferencia.ipynb

    Cargar el modelo entrenado desde models/checkpoints/final_model.h5.

    Leer data/test_hourly.csv, generar predicciones “batch” por ciudad y exportar predicciones_enero2025.csv con columnas:

    date, city_id, y_true (opcional), y_pred
