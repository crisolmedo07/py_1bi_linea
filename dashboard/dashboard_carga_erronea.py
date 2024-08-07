import streamlit as st
import duckdb
import pandas as pd

# Funcion para cargar los datos y ejecutar la consulta
def create_duckdb():
    result = duckdb.sql("""
    select  
        station, 
        min(temperatura) as min_temperatura,
        avg(temperatura) as avg_temperatura,
        max(temperatura) as max_temperatura
    from  read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=';' , columns={'station':VARCHAR , 'temperatura':'DECIMAL(3,1)'})
    group by station
    order by station
    """)

    # Convertir el resultado en un dataframe
    df = result.df()
    return df

# Funcion principal para crear dashboard con streamlit
def main():
    st.title("Resumen de Temperatura en Estaciones")
    st.write("Este dashbocar muestra el resumen de temperatura en Estaciones, incluyendo el mínimo, promedio, máximo de temperaturas.")

    # Cargar datos\
    data = create_duckdb()

    # Mostrar los datos en formato de tablas
    st.dataframe(data)

if __name__ == "__main__":
    main()