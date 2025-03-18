import os
import pandas as pd
import psycopg2

path = './vehicle_emissions.xlsx'



try:
    sheets = pd.ExcelFile(path).sheet_names
    print("Sheets available:", sheets)

    df = pd.read_excel(path, sheet_name=sheets[1]) 
    df.columns = df.columns.astype(str)  
    print("Data successfully read :")
    print(df.head())
except Exception as e:
    print("Error while reading th excel file :", e)
    exit()

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname="eco",
        user="postgres",
        password="root",
        host="localhost"
    )
    cur = conn.cursor()

    
    for _, row in df.iterrows():

        
        query = 'INSERT INTO vehicle_emission_factor ('
        values = []

        
        for column in df.columns:
            query += str(column) + ',' 
            value = row[column]
            
            if pd.isna(value):
                values.append(None)
            else:
                values.append(value)

        
        query = query[:-1]

        
        query += ') VALUES (' + ', '.join(['%s'] * len(values)) + ')'

       
        cur.execute(query, tuple(values))  

    device_values = [('ELECTRICITY', 0.04), ('NATURAL_GAS', 2.02) ]

    for x in device_values :
        type = x[0]
        factor = x[1]
        cur.execute("""
                    INSERT INTO device_emission_factor (energy_type, emission_factor)
                    VALUES (%s, %s)
                    """,
                    (type, factor)
        )

    algo_param_values = [('Device', 3, 250, 0.475), ('Vehicle', 10, None, 0.26885)]

    for x in algo_param_values :
        type = x[0]
        frequency = x[1]
        input = x[2]
        factor = x[3]
        cur.execute("""
                    INSERT INTO algo_param (type, usage_frequency, energy_input, emission_factor)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (type, frequency, input, factor)
        )
    conn.commit()


except Exception as e:
    print("An error has occurred: ", e)

finally :
    cur.close()
    conn.close()
