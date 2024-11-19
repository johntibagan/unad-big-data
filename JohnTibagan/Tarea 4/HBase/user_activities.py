import happybase
import pandas as pd

try:
    # 1. Conectar a HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")
    
    # 2. Crear la tabla con familias de columnas
    table_name = 'user_activities'
    families = {
        'activity': dict(),  # Datos principales
        'details': dict()    # Detalles específicos
    }
    
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)
    
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print(f"Tabla '{table_name}' creada exitosamente")

    # 3. Cargar datos desde un CSV
    csv_file_path = 'social_activities.csv'
    activity_data = pd.read_csv(csv_file_path)
    
    for _, row in activity_data.iterrows():
        row_key = row['activity_id'].encode()
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

    # 4. Consultas
    print("\n=== Primeras 3 actividades cargadas ===")
    count = 0
    for key, data in table.scan(limit=3):
        print(f"Actividad ID: {key.decode()}, Tipo: {data[b'activity:type'].decode()}")
        count += 1
    
    # Filtrar actividades de tipo 'search'
    print("\n=== Consultas: Filtrar actividades tipo 'search' ===")
    for key, data in table.scan(filter=b"SingleColumnValueFilter('activity', 'type', =, 'binary:search')"):
        print(f"Actividad ID: {key.decode()}, Término buscado: {data[b'activity:search_term'].decode()}")
    
    # Filtrar actividades realizadas en 'YouTube'
    print("\n=== Consultas: Filtrar actividades en 'YouTube' ===")
    for key, data in table.scan(filter=b"SingleColumnValueFilter('activity', 'platform', =, 'binary:YouTube')"):
        print(f"Actividad ID: {key.decode()}, Usuario: {data[b'activity:user_id'].decode()}")

    # Filtrar actividades con duración mayor a 300 segundos
    print("\n=== Consultas: Filtrar videos vistos más de 300 segundos ===")
    for key, data in table.scan():
        if int(data.get(b'activity:duration_seconds', b'0').decode()) > 300:
            print(f"Actividad ID: {key.decode()}, Duración: {data[b'activity:duration_seconds'].decode()} segundos")

    # 5. Inserción
    print("\n=== Inserción de nueva actividad ===")
    new_activity = {
        b'activity:user_id': b'new_user_01',
        b'activity:platform': b'Instagram',
        b'activity:type': b'like',
        b'activity:date': b'2024-11-12T12:00:00',
        b'details:post_id': b'P100',
        b'details:reaction_type': b'like'
    }
    table.put(b'activity_101', new_activity)
    print("Nueva actividad insertada: activity_101")

    # Consulta previa a la eliminación
    print("\n=== Consulta previa a eliminación ===")
    row = table.row(b'activity_101')
    print(f"Datos antes de eliminar: {row}")

    # 6. Eliminación
    print("\n=== Eliminación de la actividad ===")
    table.delete(b'activity_101')
    print("Actividad eliminada: activity_101")

    # Consulta posterior a la eliminación
    print("\n=== Consulta posterior a eliminación ===")
    row = table.row(b'activity_101')
    if not row:
        print("La actividad no existe, confirmación de eliminación.")
    else:
        print(f"Datos encontrados: {row}")

    # 7. Actualización
    print("\n=== Actualización de una actividad existente ===")
    table.put(b'activity_002', {b'details:reaction_type': b'love'})
    print("Actividad actualizada: activity_002")

    # Consulta para verificar actualización
    print("\n=== Verificar actualización ===")
    updated_row = table.row(b'activity_002')
    print(f"Detalles después de actualización: {updated_row.get(b'details:reaction_type', b'').decode()}")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión con HBase
    connection.close()
    print("Conexión cerrada")