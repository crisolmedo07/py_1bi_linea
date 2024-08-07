import streamlit as st 
import duckdb
import pandas as pd


# Funcion para cargar datos en archivo Parquet
def load_data():
    con = duckdb.connect()
    df = con.execute("Select * from 'data/measurements_summary.parquet'").df()
    con.close()
    return df

# Función principal para crear dashboard con streamlit
def main():
    st.title("Resumen de Temperatura en Estaciones")
    st.write("Este dashboard muestra el resumen de temperatura en Estaciones, incluyendo el mínimo, promedio, máximo de temperaturas.")

    # Cargar datos\
    data = load_data()

    # Mostrar los datos en formato de tablas
    st.dataframe(data)

if __name__ == "__main__":
    main()