import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
plt.style.use('ggplot')

def time_plot(pdf,city):
    fig,ax=plt.subplots(figsize=(10,5))
    ax.plot(pdf['fecha_observacion'], pdf['precipitacion'],linestyle='-',color='skyblue', alpha=0.5)
    non_zero=pdf['precipitacion']>0
    ax.scatter(pdf.loc[non_zero,'fecha_observacion'],pdf.loc[non_zero,'precipitacion'],s=12, color='blue', alpha=1)
    ax.set_xlabel('Fecha Observación')
    ax.set_ylabel('Precipitación (10 min)')
    ax.set_title(f'Precipitación cada 10 min en {city}')
    ax.tick_params(rotation=45)
    fig.tight_layout()
    fig.savefig(f'{city}_time_plot.png')