import os
import pandas as pd
import psycopg2
from psycopg2 import sql

################### MAKE SURE ALL THE FILES ARE IN THE SAME DIRECTORY AS THE SCRIPT BEFORE RUNNING IT ############################

files = ["HOME_APPLIANCES", "IT", "OUTDOOR", "TRANSPORT", "ENTERPRISE_INFRASTRUCTURE"]

i = 0

for name in files :
    path = "./" + name + ".xlsx"
    file_base_name = os.path.splitext(os.path.basename(path))[0]

    dtype = "Vehicle" if "TRANSPORT" in path else "Device"
    isIT = "IT" in path


    try:
        df = pd.read_excel(path)
        machine_column = 'Vehicle' if 'Vehicle' in df.columns else 'Device'
        print("Données lues avec succès :")
        print(df.head())
    except Exception as e:
        print("Erreur lors de la lecture du fichier Excel :", e)
        exit()

    try:
        conn = psycopg2.connect(
            dbname="eco",
            user="postgres",
            password="root",
            host="localhost"
        )
        cur = conn.cursor()

        machines = {}
        current_machine = None
        current_component = None
        
        if i == 0 : 
            # to avoid an error with the vehicles (weird error I couldn't fix it so I removed a useless constraint)
            cur.execute("ALTER TABLE machine DROP CONSTRAINT machine_vehicle_type_check;")
            
            i += 1 # to only delete the constraint once

        for _, row in df.iterrows():
            if pd.notna(row[machine_column]):
                current_machine = row[machine_column]
                if current_machine not in machines:
                    machine_data = {
                        'name': current_machine,
                        'defaultFootprint': row.get('Default Footprint', 0),
                        'usage': file_base_name if dtype == "Device" else 'TRANSPORT',  # Utilisation du nom du fichier
                        'img': row['img'],
                        'dtype': dtype,
                        'resources': {}
                    }
                    if dtype == 'Vehicle':
                        machine_data.update({
                            'vehicle_size': None if pd.isna(row.get('Vehicle Size')) else row['Vehicle Size'],
                            'vehicle_type': None if pd.isna(row.get('Vehicle Type')) else row['Vehicle Type']
                        })
                    machines[current_machine] = machine_data

            if pd.notna(row['Component']):
                current_component = row['Component']
                if current_machine and current_component not in machines[current_machine]['resources']:
                    machines[current_machine]['resources'][current_component] = {
                        'name': current_component,
                        'matters': []
                    }

            if pd.notna(row['Material']) and current_machine and current_component:
                matter = {
                    'value': row['Material'],
                    'volume': float(row['Quantity(g)' if isIT else 'Volume (g)']) if pd.notna(row['Quantity(g)' if isIT else 'Volume (g)']) else 0
                }
                machines[current_machine]['resources'][current_component]['matters'].append(matter)

        for machine_name, machine_data in machines.items():
            if dtype == 'Vehicle':
                cur.execute(
                    """
                    INSERT INTO machine (name, img, default_footprint, usage, dtype, vehicle_size, vehicle_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (machine_data['name'], machine_data['img'], machine_data['defaultFootprint'], 'TRANSPORT', 
                    machine_data['dtype'], machine_data.get('vehicle_size'), machine_data.get('vehicle_type'))
                )
            else:
                cur.execute(
                    """
                    INSERT INTO machine (name, default_footprint, usage, img, dtype)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (machine_data['name'], machine_data['defaultFootprint'], machine_data['usage'], 
                    machine_data['img'], machine_data['dtype'])
                )
            
            machine_id = cur.fetchone()[0]

            for component_name, component_data in machine_data['resources'].items():
                cur.execute(
                    """
                    INSERT INTO component (name, machine_id)
                    VALUES (%s, %s)
                    RETURNING id
                    """,
                    (component_data['name'], machine_id)
                )
                component_id = cur.fetchone()[0]

                for matter in component_data['matters']:
                    cur.execute(
                        """
                        INSERT INTO matter (value, volume, component_id)
                        VALUES (%s, %s, %s)
                        """,
                        (matter['value'], matter['volume'], component_id)
                    )

        conn.commit()
        print(f"Données insérées avec succès dans la base de données en tant que {dtype}.")

    except Exception as e:
        print("Erreur lors de l'exécution du script :", e)
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()
