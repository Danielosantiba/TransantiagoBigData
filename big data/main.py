def main(requests):
    # Librerias
    import json
    import pandas as pd
    from pandas import json_normalize 
    from google.cloud import storage 
    import requests
    import gcsfs
    import fsspec

    # Proceso de obtención de JSON de la api de red.cl
    response = requests.get('https://www.red.cl/restservice_v2/rest/getservicios/all')
    data = response.json()

    # Configurar las credenciales de autenticación
    client = storage.Client.from_service_account_json('key.json')

    # Obtener el bucket en el que se almacenarán los datos
    bucket = client.bucket('informacion-gob')

    # Crear un objeto Blob en el bucket
    blob = bucket.blob('recorridos/servicios_red.json')

    # Convertir los datos a formato JSON y escribirlos en el objeto Blob
    blob.upload_from_string(json.dumps(data), content_type='application/json')

    # Loop del Json -- Por ahora 5 primeros
    for key in response.json()[0:5]:
        url_detalle="https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint="+key

        # Realizar una solicitud GET a la API
        response = requests.get(url_detalle)

        # Obtener el contenido de la respuesta en formato JSON
        data = response.json()

        fileName = "recorridos/recorrido_"+key+".json"

        # Crear un objeto Blob en el bucket
        blob = bucket.blob(fileName)

        # Convertir los datos a formato JSON y escribirlos en el objeto Blob
        blob.upload_from_string(json.dumps(data), content_type='application/json')

        # Proceso de separación de JSON en múltiples CSV.
        # Creacion dataframe json negocio
        negocio_data = data['negocio']
        df_negocio = pd.DataFrame.from_dict(negocio_data, orient='index').T

        # Creacion dataframe IDA - HORARIOS
        horarios = data['ida']['horarios']
        df_ida_horarios = pd.DataFrame(horarios)

        # Creacion dataframe IDA - PATH
        path = data['ida']['path']
        df_ida_path = pd.DataFrame(path, columns=['Latitud', 'Longitud'])

        # Creacion dataframe IDA - PARADEROS
        paraderos = data['ida']['paraderos']
        df_ida_paraderos = pd.DataFrame(paraderos)
        # Creacion dataframe IDA - PARADEROS - POS
        paraderos_data_pos = data['ida']['paraderos']
        posiciones = []
        for paradero in paraderos_data_pos:
            posiciones.append(paradero['pos'])
        df_posiciones = pd.DataFrame(posiciones, columns=['latitud', 'longitud'])
 
# Creacion dataframe IDA - PARADEROS - SERVICIOS
        paraderos_data = data['ida']['paraderos']
        servicios_parametros = []
        for paradero in paraderos_data:
            servicios = paradero['servicios']
            for servicio in servicios:
                servicio_parametros = {
                    'id': servicio['id'],
                    'cod': servicio['cod'],
                    'destino': servicio['destino'],
                    'orden': servicio['orden'],
                    'color': servicio['color'],
                    'negocio_nombre': servicio['negocio']['nombre'],
                    'negocio_color': servicio['negocio']['color'],
                    'recorrido_destino': servicio['recorrido']['destino'],
                    'codigo': servicio['codigo']
                }
                servicios_parametros.append(servicio_parametros)
        df_servicios = pd.DataFrame(servicios_parametros)

        # Creacion dataframe IDA - PARADEROS - STOP
        paraderos_data = data['ida']['paraderos']
        stop_data = []
        for paradero in paraderos_data:
            stop_data.append(paradero['stop'])
        df_stop = pd.DataFrame(stop_data)

        # Creacion dataframe regreso - horarios
        regreso_horarios = data['regreso']['horarios']
        df_regreso_horarios = pd.DataFrame(regreso_horarios)

        # Creacion dataframe regreso path
        regreso_path = data['regreso']['path']
        df_regreso_path = pd.DataFrame(regreso_path, columns=['Latitud', 'Longitud'])

        # Creacion de dataframe regreso paraderos
        regreso_paraderos = data['regreso']['paraderos']
        df_regreso_paraderos = pd.DataFrame(regreso_paraderos)
        # Creacion dataframe regreso paraderos pos
        regreso_paraderos_data_pos = data['regreso']['paraderos']
        regreso_posiciones = []
        for paradero in regreso_paraderos_data_pos:
            regreso_posiciones.append(paradero['pos'])
        df_regreso_par_posiciones = pd.DataFrame(regreso_posiciones, columns=['latitud', 'longitud'])

        # Creacion dataframe regreso paraderos servicios
        regreso_servicios_paraderos_data = data['regreso']['paraderos']
        regreso_servicios_parametros = []
        for paradero in regreso_servicios_paraderos_data:
            servicios = paradero['servicios']
            for servicio in servicios:
                servicio_parametros = {
                    'id': servicio['id'],
                    'cod': servicio['cod'],
                    'destino': servicio['destino'],
                    'orden': servicio['orden'],
                    'color': servicio['color'],
                    'negocio_nombre': servicio['negocio']['nombre'],
                    'negocio_color': servicio['negocio']['color'],
                    'recorrido_destino': servicio['recorrido']['destino'],
                    'codigo': servicio['codigo']
                }
                regreso_servicios_parametros.append(servicio_parametros)
        df_regreso_paraderos_servicios = pd.DataFrame(regreso_servicios_parametros)

        # Creacion dataframe regreso paraderos - stop
        regreso_stop_paraderos_data = data['regreso']['paraderos']
        regreso_stop_data = []
        for paradero in regreso_stop_paraderos_data:
            regreso_stop_data.append(paradero['stop'])
        df_regreso_paraderos_stop = pd.DataFrame(regreso_stop_data)
# Creacion de archivos CSV en base a dataframes generados
        # La subida debe ser modificada a nuestro bucket
        # La sintaxis utilizada es la siguiente:
        # NOMBREDF.to_csv(gs://NUESTROBUCKET/CSVs/NOMBREARCHIVO.csv', index=False)

        df_negocio.to_csv('gs://informacion-gob/CSVs/negocio.csv', index=False)
        df_ida_horarios.to_csv('gs://informacion-gob/CSVs/ida_horarios.csv', index=False)
        df_ida_path.to_csv('gs://informacion-gob/CSVs/ida_path.csv', index=False)
        df_ida_paraderos.to_csv('gs://informacion-gob/CSVs/ida_paraderos.csv', index=False)
        df_posiciones.to_csv('gs://informacion-gob/CSVs/ida_paraderos_posiciones.csv', index=False)
        df_servicios.to_csv('gs://informacion-gob/CSVs/ida_paraderos_servicios.csv', index=False)
        df_stop.to_csv('gs://informacion-gob/CSVs/ida_paraderos_stop.csv', index=False)
        df_regreso_horarios.to_csv('gs://informacion-gob/CSVs/regreso_horarios.csv', index=False)
        df_regreso_path.to_csv('gs://informacion-gob/CSVs/regreso_path.csv', index=False)
        df_regreso_paraderos.to_csv('gs://informacion-gob/CSVs/regreso_paraderos.csv', index=False)
        df_regreso_par_posiciones.to_csv('gs://informacion-gob/CSVs/regreso_paraderos_posiciones.csv', index=False)
        df_regreso_paraderos_servicios.to_csv('gs://informacion-gob/CSVs/regreso_paraderos_servicios.csv', index=False)
        df_regreso_paraderos_stop.to_csv('gs://informacion-gob/CSVs/regreso_paraderos_stop.csv', index=False)
        
        return("Completado")