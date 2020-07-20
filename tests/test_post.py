import requests
import json
from pprint import pprint
import urllib
from datetime import datetime
import boto3

root_param = '/env/modelos_analitica/'

#Parameters
def get_param(param):
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=root_param +param, WithDecryption=False)
    value = parameter.get("Parameter").get("Value")
    return(value)


def get_data_ready_2_insert(id_modelo, id_metrica, valores):
    """
    Arma el json a insertar
    Estructuro la métrica
        data = {
            "records": [
                {
                    "fields": {
                        "CODIGO_MODELO": [id_modelo],
                        "METRICA": [id_metrica],
                        "VALOR": valor,
                        "FECHA_MEDICION": date,
                        "TIPO_CARGA": "Automatico"
                    }
                }
            ]
        }
    """
    records = []
    date = datetime.today().strftime('%Y-%m-%dT%H:%M:%SZ')

    if isinstance(valores, list):
        for val in valores:
            if not isinstance(val, int) and not isinstance(val, float):
                raise TypeError("No se puede insertar valores que no sean int o float")
            else:
                fields = {
                    "CODIGO_MODELO": [id_modelo],
                    "METRICA": [id_metrica],
                    "VALOR": val,
                    "FECHA_MEDICION": date,
                    "TIPO_CARGA": "Automatico"
                }
                records.append({"fields":fields})
        return {"records":records}
    else:
        raise TypeError("get_data_ready_2_insert solo acepta lista de ints/floats")


def post_medicion(cod_modelo, cod_metrica, headers, valores):
    """
    Recibe:
        - cod_modelo: El COD_MODELO de la tabla de MODELOS
        - cod_metrica: El COD_METRICA de la tabla de DEFINICION_METRICAS
        - valores (lista de floats o ints): son los valores de la medición
            - Si solo se quiere insertar uno se envia una lista con un valor
    Devuelve:
        - Response con datos insertados si se registró la medición OK
        - Exception: Si hubo un problema al intentar postear la metrica
    """

    try:
        id_modelo = get_id_modelo(cod_modelo, headers)
        id_metrica = get_id_metrica(cod_metrica, headers)
        # Url de mediciones
        url_mediciones = "https://api.airtable.com/v0/appLBwk9mmXhZRXXe/MEDICION_METRICAS"
       
        data = get_data_ready_2_insert(id_modelo,id_metrica, valores)
        # Hago el post a la base
        response = requests.post(url=url_mediciones, headers=headers, json=data)
        if response.status_code == 200:
            print("Insertado correctamente")
        return response
    except Exception as e:
        raise e


def get_id_modelo(cod_modelo, headers):
    """
    COD_MODELO (string) Se saca de AIRTABLE
    
    Recibe el código/nombre del modelo que se le colocó en AIRTABLE
    Devuelve el id del modelo con el COD_MODELO que recibió
    """
    try:
        url_modelos = "https://api.airtable.com/v0/appLBwk9mmXhZRXXe/MODELOS"

        cod_modelo_q = urllib.parse.quote(cod_modelo)
        url = f"{url_modelos}?filterByFormula=COD_MODELO%3D%22{cod_modelo_q}%22"

        response = requests.get(url=url, headers=headers)
        jsonResponse = json.loads(response.text.encode("utf8"))
        records = jsonResponse["records"]
        #pprint(records)
        id_modelo = records[0]["id"]
        #print(id_modelo)
        return id_modelo
    
    except Exception as e:
        print(f"Error while trying to get id_modelo with {cod_modelo}")
        print(f"Error intentando obtener id_modelo de {cod_modelo}")
        print(str(e))

    return
    

def get_id_metrica(cod_metrica, headers):
    """
    COD_METRICA (string) 
    
    Recibe el código/nombre del modelo que se le colocó en AIRTABLE
    Devuelve el id del modelo con el COD_MODELO que recibió
    """
    try:
        url_metricas = "https://api.airtable.com/v0/appLBwk9mmXhZRXXe/DEFINICION_METRICAS"
        # Encoding de cod_metrica
        cod_metrica_q = urllib.parse.quote(cod_metrica)
        url = f"{url_metricas}?filterByFormula=COD_METRICA%3D%22{cod_metrica_q}%22"

        response = requests.get(url=url, headers=headers)
        jsonResponse = json.loads(response.text.encode("utf8"))
        records = jsonResponse["records"]
        #pprint(records)
        id_metrica = records[0]["id"]

        return id_metrica

    except Exception as e:
        print(f"Error while trying to get id_metrica with {cod_metrica}")
        print(f"Error intentando obtener id_metrica de {cod_metrica}")
        print(str(e))

    return 


if __name__ == "__main__":
    
    headers = {
        "Authorization": "Bearer 11111key11CwerNNnOzP964n",
        "Content-Type": "application/json"
    }

    #print(get_id_metrica("KS30", headers))
    #print(get_id_modelo("post testing", headers))

    valores_medicion = [20,30,50,0.6,90]
    post_medicion("post testing", "KS30", headers, valores_medicion)
    
    # Insertar muchas mediciones de metricas distintas para el mismo modelo
    modelo = "post testing"
    metricas = [("KS30",[20]), ("Gain20",[40]), ("KS20",[0.5])]
    for tupla_metrica in metricas:
        metrica, valor = tupla_metrica
        post_medicion(modelo, metrica, headers,valor)