import requests
import json
import csv
from pprint import pprint


def get_all_records(url_base, headers):
    # Obtener todos los registros de la base de airtable
    records = []
    response = requests.get(url=url_base, headers=headers)
    while True:

        jsonResponse = json.loads(response.text.encode("utf8"))
        records = records + jsonResponse["records"]

        # Hay más registros?
        if "offset" in jsonResponse.keys():
            offset = jsonResponse["offset"]
            url_offset = url_base + "?offset=" + offset
            response = requests.get(url=url_offset, headers=headers)
        else:
            # No hay más registros que obtener de la base
            break

    return records

def get_csv_header(base):
    """
    Recibe el nombre de la base de datos de AIRTABLE
    Devuelve la cabecera del csv para la base de AIRTABLE
    """
    bases_headers = {"AREAS": ["id", "createdTime", "ID", "TRIBU", "SQUAD", "ID_AREA", "MODELOS"],
                     "DEFINICION_METRICAS": ["id",
                                             "createdTime",
                                             "DEFINICION",
                                             "UNIDAD_MEDIDA",
                                             "CONFUSION_MATRIX",
                                             "METRICA",
                                             "COD_METRICA",
                                             "MEDICION_METRICAS",
                                             "UMBRALES",
                                             "DECIL"],
                     "MEDICION_METRICAS": ["id",
                                           "createdTime",
                                           "ID_MEDICION",
                                           "CODIGO_MODELO",
                                           "METRICA",
                                           "VALOR",
                                           "FECHA_MEDICION",
                                           "TIPO_CARGA"],
                     "MODELOS": ["id",
                                 "createdTime",
                                 "COD_MODELO",
                                 "NOMBRE_MODELO",
                                 "MEDICION_METRICAS",
                                 "UMBRALES",
                                 "RESPONSABLE",
                                 "DESCRIPCION_GENERAL",
                                 "ALGORTIMO_UTILIZADO",
                                 "FRECUENCIA_EVALUACION",
                                 "TRIGGERS_DE_ENTRENAMIENTO",
                                 "FUENTES_DE_DATOS",
                                 "AREAS",
                                 "EMAIL_RESPONSABLE",
                                 "AUTOR",
                                 "EMAIL_AUTOR",
                                 "QUARTER",
                                 "TIPO_PROBLEMA",
                                 "ULTIMA_ACTUALIZACION",
                                 "FECHA_IMPLEMENTACION",
                                 "FECHA_ULTIMA_MODIFICACION",
                                 "UBICACION_RESULTADOS",
                                 "URL",
                                 "RECOMENDACIONES_DE_USO",
                                 "ANEXOS",
                                 "ORIGENES",
                                 "VERSIONES"],
                     "ORIGENES": ["id", "createdTime", "ESQUEMA", "TABLA", "ID_ORIGEN", "MODELO"],
                     "UMBRALES": ["id",
                                  "createdTime",
                                  "ID_UMBRAL",
                                  "COD_METRICA",
                                  "COD_MODELO",
                                  "UMBRAL",
                                  "INTERPRETACION",
                                  "FECHA_ALTA"],
                     "VERSIONES": ["id",
                                   "createdTime",
                                   "ID_VERSION",
                                   "MODELO",
                                   "VERSION",
                                   "FECHA_VERSION",
                                   "DESCRIPCION_CAMBIOS"]}
    return bases_headers[base]


def limpiar_string(data_cruda):
    """ 
    Recibe un string "data_cruda" y le quita saltos de línea \n y \t
    Tambien cambia ";" por "," para no joder en el csv separado por ";"
    """
    # Salto de linea = 1 espacio
    clean_string = data_cruda.replace("\n", " ")
    # Tab = 4 espacios
    clean_string = clean_string.replace("\t", "    ")
    # Quito puntos y comas también
    clean_string = clean_string.replace(";", ",")
    return clean_string



def json_2_csv(base, records):
    """
    Recibe 2 argumentos:
    - Base: el nombre de la base en AIRTABLE
    - Records: la lista de los registros (diccionarios) de la base de AIRTABLE 
    
    Recibe un diccionario (json) 
    Devuelve csv
    """
    # Obtengo cabecera del csv para la base
    csv_header = get_csv_header(base)
    # Defino el output_file
    output_file = f"{base}.csv"
    # Genero objetos para escribir con lib csv
    data_to_file = open(output_file, 'w', newline='')
    csv_writer = csv.writer(data_to_file, delimiter=";")
    # Escribo cabecera
    rows = []
    rows.append(csv_header)
    # Iteramos sobre los datos
    for record in records:
        row = []
        # Escribe cada key del registro
        for key in csv_header:
            if key == "id" or key == "createdTime":
                row.append(record[key])
            else:
                # Existe la clave en el registro?
                if key in record["fields"].keys():
                    # Si existe
                    # Agrego a row algo no vacío
                    data_cruda = record["fields"][key]
                    # Es lista? Es string?
                    if isinstance(data_cruda, list):
                        row.append(str(data_cruda))
                    elif isinstance(data_cruda, str):
                        row.append(limpiar_string(data_cruda))
                    else:
                        # No es lista ni string
                        # Suele ser un numero
                        row.append(data_cruda)
                else:
                    # No existe en la clave en el registro, es vacío
                    row.append("")
        # Terminó el registro
        rows.append(row)
    csv_writer.writerows(rows)
    return output_file


def run(url,headers,bases_airtable):
    
    for base in bases_airtable:
        url_base = url.format(base)
        # Get records
        records = get_all_records(url_base, headers)
        csv = json_2_csv(base, records)
        print("Impreso csv: " + csv)
    return


if __name__ == "__main__":

    bases_airtable = [
            "MODELOS",
            "DEFINICION_METRICAS",
            "MEDICION_METRICAS",
            "UMBRALES",
            "ORIGENES",
            "VERSIONES",
            "AREAS"
            ]
    
    
    # URL General
    url = "https://api.airtable.com/v0/appLBwk9mmXhZRXXe/{}"
    # Defino headers (esto no hay que subir a un repo público!!!)
    headers= {
        "Authorization": "Bearer 1111keyCwerNNnOzP964n",
        "Content-Type": "application/json"
    }

    run(url,headers,bases_airtable)

