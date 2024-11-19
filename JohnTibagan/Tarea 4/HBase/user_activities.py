import happybase
import pandas as pd

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")
    
    # 2. Crear la tabla con las familias de columnas
    table_name = 'user_activities'
    families = {
        'activity': dict(),  # Información principal de la actividad
        'details': dict()    # Detalles específicos de cada actividad
    }
    
    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)
    
    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'user_activities' creada exitosamente")
    
    # 3. Cargar datos desde el archivo CSV
    csv_file_path = 'social_activities.csv'
    activity_data = pd.read_csv(csv_file_path)
    
    for _, row in activity_data.iterrows():
        row_key = row['activity_id'].encode()  # Usar activity_id como row key
        
        # Organizar los datos en familias de columnas
        data = {
            b'activity:user_id': str(row['user_id']).encode(),
            b'activity:platform': str(row['platform']).encode(),
            b'activity:type': str(row['type']).encode(),
            b'activity:date': str(row['date']).encode(),
            b'activity:duration_seconds': str(row['duration_seconds']).encode(),
            b'activity:search_term': str(row['search_term']).encode(),
            b'details:video_id': str(row['details_video_id']).encode(),
            b'details:title': str(row['details_title']).encode(),
            b'details:post_id': str(row['details_post_id']).encode(),
            b'details:content': str(row['details_content']).encode(),
            b'details:reaction_type': str(row['details_reaction_type']).encode()
        }
        
        table.put(row_key, data)
    
    print("Datos cargados exitosamente")

    # 4. Consultas sobre los datos
    print("\n=== Consulta: Primeras 3 actividades cargadas ===")
    count = 0
    for key, data in table.scan():
        if count < 3:
            print(f"\nActividad ID: {key.decode()}")
            print(f"Usuario: {data[b'activity:user_id'].decode()}")
            print(f"Tipo de actividad: {data[b'activity:type'].decode()}")
            print(f"Detalles del contenido: {data.get(b'details:content', b'').decode()}")
            count += 1
    
    # Filtrar actividades tipo "search"
    print("\n=== Consulta: Filtrar actividades tipo 'search' ===")
    for key, data in table.scan(filter=b"SingleColumnValueFilter('activity', 'type', =, 'binary:search')"):
        print(f"Actividad ID: {key.decode()}, Término de búsqueda: {data[b'activity:search_term'].decode()}")

    # 5. Operaciones de escritura: Inserción
    print("\n=== Inserción de nueva actividad ===")
    new_activity = {
        b'activity:user_id': b'new_user_01',
        b'activity:platform': b'Instagram',
        b'activity:type': b'like',
        b'activity:date': b'2024-11-12T12:00:00',
        b'activity:duration_seconds': b'0',
        b'activity:search_term': b'',
        b'details:post_id': b'P100',
        b'details:reaction_type': b'like'
    }
    table.put(b'activity_101', new_activity)
    print("Nueva actividad insertada con éxito")

    # 6. Actualización de un registro existente
    print("\n=== Actualización de una actividad existente ===")
    table.put(b'activity_101', {b'details:reaction_type': b'love'})
    print("Actividad actualizada con éxito")

    # 7. Eliminación de un registro
    print("\n=== Eliminación de una actividad ===")
    table.delete(b'activity_101')
    print("Actividad eliminada con éxito")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión con HBase
    connection.close()
    print("Conexión cerrada")
