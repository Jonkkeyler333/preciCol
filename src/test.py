from datetime import datetime
from meteostat import Point, Hourly
import os
import shutil

def clear_meteostat_cache():
    cache_dir = os.environ.get('METEOSTAT_CACHE', '')
    if cache_dir and os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
        os.makedirs(cache_dir)
        
def get_meteostat_data(lat:float,lon:float,alt:int,start_date,end_date):
    ubicacion=Point(lat,lon)
    datos_horarios=Hourly(loc=ubicacion,start=start_date,end=end_date)
    df_mete=datos_horarios.fetch()
    # df_mete.drop_duplicates(inplace=True)
    df_mete.drop(columns=['snow','wpgt','tsun'],inplace=True)
    print(df_mete.info())
    return df_mete.resample('10min').interpolate(method="time")

if __name__ == '__main__':
    clear_meteostat_cache()
    df= get_meteostat_data(
        lat=5.724, 
        lon=-72.92, #7.8891094035409095, -72.49661523780262 ,5.724185979844469, -72.9244557490702
        alt=2527, 
        start_date=datetime(2024, 1, 1), 
        end_date=datetime(2025, 1, 1)
    )
    # inicio = datetime(2024, 1, 1)
    # fin = datetime(2025, 1, 1)
    # ubicacion = Point(1.2136,-77.2811)

    # # Usar parámetros nombrados (loc, start, end)
    # datos_horarios = Hourly(
    #     loc=ubicacion,  # Parámetro clave
    #     start=inicio,
    #     end=fin
    # )

    # df_horario = datos_horarios.fetch()
    # print(df_horario.tail())
    
