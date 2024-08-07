import duckdb 
import time


def create_duckdb():
    duckdb.sql("""
    select  
        station, 
        min(temperatura) as min_temperatura,
        avg(temperatura) as avg_temperatura,
        max(temperatura) as max_temperatura
    from  read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=';' , columns={'station':VARCHAR , 'temperatura':'DECIMAL(3,1)'})
    group by station
    order by station
    """).show()

if __name__ == "__main__":
    start_time = time.time()
    create_duckdb()
    end_time = time.time()
    took = end_time - start_time
    print(f"Duckdb Took: {took:.2f} sec")