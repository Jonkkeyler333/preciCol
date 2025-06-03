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
│       └── plot_train.py         ← Genera plots de pérdida y métricas de entrenamiento
│
└── myenv/                  ← Entorno virtual (dependencias)                    
