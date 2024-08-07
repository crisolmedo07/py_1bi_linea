import pandas as pd 
from multiprocessing import Pool , cpu_count
from tqdm import tqdm # Import para observar la barra de progreso

CONCURRENCY = cpu_count()

total_lineas = 1_000_000_000 # Total de lineas conocido
chunksize = 100_000_000 # Define el tamaño de chunk
path_txt = "data/measurements.txt" # path del archivo

def process_chunk(chunk):
    # Agrega los datos dentro de chunk usando Pandas
    aggregated = chunk.groupby('station')['measure'].agg(['min' , 'max' , 'mean']).reset_index()
    return aggregated

def create_df_with_pandas(path_txt , total_lineas , chunksize = chunksize):
    total_chunks = total_lineas // chunksize + ( 1 if total_lineas % chunksize else 0 )
    results = []

    with pd.read_csv(path_txt , sep=';' , header=None , names=['station' , 'measure'] , chunksize=chunksize) as reader:
        # Envolviendo el iterador con tqdm para visualizar el progreso
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader , total=total_chunks , desc="Procesando"):
                # Procesa cada chunk en paralelo
                result = pool.apply_async(process_chunk , (chunk,))
                results.append(result)
            
            results = [result.get() for result in results]
    
    final_df = pd.concat(results , ignore_index=True)

    final_aggregated_df = final_df.groupby('station').agg({
        'min' : 'min',
        'mean' : 'mean',
        'max' : 'max',
        }).reset_index().sort_values('station')

    return final_aggregated_df


if __name__ == "__main__":
    import time

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()
    df = create_df_with_pandas(path_txt, total_lineas, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")

