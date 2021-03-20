#/////////////////////////////////////////////////I M P O R T A N T E /////////////////////////////////////////////////////
# Es importante iniciar primero nuestro servidor de mongodb, para quese pueda crear correctamente la base de datos.

# El programa se modifico con reespecto a la practica 1, es importante tener en cuenta los siguientes puntos
# 1.- Si ha corrido el programa de la practica 1 es probable que ya tenga una BD con este nombre (motor), es importante que la borre
        #y vuelva a correr este programa, pues las BD son diferentes.
# 2.- Se hizo dos archivos separados para una mejor comprension del codigo, en esta primera parte(enlaces.py) se crea la BD y se guardan los enlaces
        # de las paginas que se visitan, asì como tambien se guarda si ya ha sido visitada
# 3.- En la segunda parte (indexado.py) se hace el indexado de las paginas que ya se han visitado
# 4.- Abra la terminal y corra primero el comando: python ./enlaces.py , una vez que termine de ejecutarse
        # corra el segundo comando: python ./indexado.py
# 5.- En enlaces.py puede que haya error al intentar leer alguna pagina, tratamos de solucionar este problema, pero no tuvimos exito
        #puede que sea problema de la libreria, además, por ese problema no se revisan todas las paginas guardadas en la BD
# 6.- En indexado.py hay un momento en que no guarda los titulos, keywords, palabras, etc, parace ser el mismo problema del punto anterior
        # sin embargo, si guarda el menos 20 paginas con estos campos, suficiente para hacer las pruebas necesarias.
        
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#////////////////////////////////////////////////L I B R E R I A S //////////////////////////////////////////////////////
#pymongo
    #-> pip install pymongo
#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


from urllib.request import Request,urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
def insertarEnlace(direccion):   # Insertamos un nuevo enlace.

    # Estructura de los Enlaces.
    nuevaDireccion = {
    "direccion": direccion,
    "titulo": "",
    "keywords": "",
    "descripcion": "",
    "palabra1": "",
    "palabra2": "",
    "palabra3": "",
    "ranking1": 0,
    "ranking2": 0,
    "ranking3": 0,
    "revisada": 0
    }

    enlaces.insert_one(nuevaDireccion)


def visitado(query):    # Cambiamos el estado de "visitada".

    newQuery = { "$set": { "revisada": 1 } }
    enlaces.update_one(query, newQuery)


# MAIN

client = MongoClient('localhost')   # Conexion a la Base de Datos.
db = client['motor']    # Creamos la Base de Datos.
enlaces = db['enlaces'] # Creamos una Coleccion.
enlaces.delete_many({}) # Eliminamos documentos anteriores.
   
# Primer enlace para comenzar.
insertarEnlace("http://sagitario.itmorelia.edu.mx/~rogelio/hola.htm")   

for ciclo in range(1,50):

    enlace = db.enlaces.find_one( {"revisada": 0} ) # Mostramos enlaces "no visitados".
    direccion = enlace['direccion']
    
    req = Request(direccion, headers={'User-Agent': 'Mozilla/5.0'})
   
    url = urlopen(req)
    bs = BeautifulSoup(url.read(), 'html.parser')

    for direcciones in bs.find_all("a"):

        nuevaDireccion = direcciones.get("href")
        
        if re.search("^http", str(nuevaDireccion), re.IGNORECASE):
            
            if not( db.enlaces.find_one( {"direccion": nuevaDireccion} ) ):    # Verificamos que no sea una direccion repetida.
                insertarEnlace(nuevaDireccion)  # Insertamos una nueva direccion.   
    visitado(enlace)    # Marcamos el enlace como "visitado".


# OUTPUT
""" print( 'Direcciones guardadas:\n')
for direcciones in enlaces.find():  # Mostramos las direcciones almacenadas.
    print( direcciones['direccion'] )
 """

client.close()  # Cerramos la conexion.