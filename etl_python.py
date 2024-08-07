from csv import reader
from collections import defaultdict
import time
from pathlib import Path

PATH_TXT = "data/measurements.txt"

def procesar_temperaturas(PATH_TXT : Path):
    print("Iniciando el procesamiento del archivo....")
    
    start_time = time.time()
# es similar al diccionario que almacena los valores de las variables como diccionario
# {clave : {val1 , val2 , ..... , valn} } - Ejemplo: {hamburguer : {12 , 16 , ..... , X}}
    temperatura_por_station = defaultdict(list) 

    with open(path_txt , 'r' , encoding='utf-8') as file:
        _reader = reader(file , delimiter=';')
        for row in _reader:
            nombre_station , temperatura = str(row[0]) , float(row[1])
            temperatura_por_station[nombre_station].append(temperatura)
    
    print("Datos cargados, Calculando estadisticas....")

    # Diccionario para almacenar los resultados cargados
    results = {}

    for station , temperaturas in temperatura_por_station.items():
        min_temp = min(temperaturas)
        mean_temp = sum(temperaturas) / len(temperaturas)
        max_temp = max(temperaturas)
        results[station] = (min_temp , mean_temp , max_temp)
    
    print("Estadisticas calculadas, Ordenando registros....")

    # Ordenando los resultado por cada estacion
    sorted_results = dict(sorted(results.items()))

    formatted_results = {station : f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}" for station , (min_temp , mean_temp , max_temp) in sorted_results.items() }

    end_time = time.time() # Tiempo final 

    print(f"Procesamiento concluido en {end_time - start_time:.2f} segundos...")

    return formatted_results

if __name__ == "__main__":
    path_txt : Path = Path("data/measurements.txt")
    # 100M > 5min
    resultados = procesar_temperaturas(path_txt)