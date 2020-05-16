import requests
import json


def obtener_modelo_idrow(idModelo):
    
    urlBody = "https://api.airtable.com/v0/appfrTHmfLE1sRxeE/Modelos"
    urlParams = "?fields%5B%5D=CODIGO_MODELO&filterByFormula={CODIGO_MODELO}='"+idModelo + "'"

    url = urlBody + urlParams
    payload  = {}
    headers = {
      'Authorization': 'Bearer keyJFI9I8XSvuKz54',
      'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data = payload)
    
    jsonResponse = json.loads(response.text.encode('utf8'))
    jsonResponse = jsonResponse['records']
    id_model_row = jsonResponse[0]['id']
    
    return id_model_row



def obtener_metrica_idrow(metrica):
    
    urlBody = "https://api.airtable.com/v0/appfrTHmfLE1sRxeE/DEFINICION_METRICAS"
    urlParams = "?fields%5B%5D=METRICA&filterByFormula={METRICA}='"+metrica+ "'"
    
    url = urlBody + urlParams
    payload  = {}
    headers = {
      'Authorization': 'Bearer keyJFI9I8XSvuKz54',
      'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data = payload)
    
    jsonResponse = json.loads(response.text.encode('utf8'))
    jsonResponse = jsonResponse['records']
    id_model_row = jsonResponse[0]['id']
    
    return id_model_row




def postear_metrica_modelo_idlevel(modelo_idrow,metrica_idrow,valorMetrica,fecha_medicion):

    url = "https://api.airtable.com/v0/appfrTHmfLE1sRxeE/MEDICION_METRICAS"
    payload = "{\r\n  \"records\": [\r\n    {\r\n      \"fields\": {\r\n        \"CODIGO_MODELO\": [\r\n          \""+modelo_idrow+"\"\r\n        ],\r\n        \"METRICA\": [\r\n          \""+metrica_idrow+"\"\r\n        ],\r\n        \"VALOR\": "+valorMetrica+",\r\n        \"FECHA_MEDICION\": \""+fecha_medicion+"\",\r\n        \"TIPO_CARGA\": \"Automatico\"\r\n      }\r\n    }\r\n  ]\r\n}"
    headers = {
      'Authorization': 'Bearer keyJFI9I8XSvuKz54',
      'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data = payload)
    #print(response.text.encode('utf8'))
    return response




def postear_metrica_modelo(idModelo,metrica,valorMetrica,fecha_medicion):
    
    modelo_idrow = obtener_modelo_idrow(idModelo)
    metrica_idrow = obtener_metrica_idrow(metrica)
    response = postear_metrica_modelo_idlevel(modelo_idrow,metrica_idrow,valorMetrica,fecha_medicion)
    
    return response





#si la metrica que quiero cargar es por ejemplo Ganancia1%, al tener eel simbolo % cuadno se llama el metodo
#obtener_metrica_idrow(metrica) falla, si usa un valor como coverage funciona bien porque no tiene simbolos especiales
#opte por sacar el simbolo %
#metrica = 'Ganancia1%'
idModelo = 'fraudeoriginacion'
metrica = 'Ganancia1'
valorMetrica = '3.50'
fecha_medicion = '2020-05-11T21:17:00.000Z'


response = postear_metrica_modelo(idModelo,metrica,valorMetrica,fecha_medicion)
print(response)











