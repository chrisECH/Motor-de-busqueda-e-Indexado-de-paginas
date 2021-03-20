from urllib.request import Request,urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
import ssl
from nltk.corpus import stopwords #Libreria para elliminar "el" "un" "a" "una" entre otras
from nltk.tokenize import word_tokenize #Libreria para dividir una cadena en palabras
import string #Libreria para cadenas
from collections import Counter #Libreria que sirve como contador
from _collections import OrderedDict #Libreria para ordenamiento 
ssl._create_default_https_context = ssl._create_unverified_context

def insertarDesc(direccion): #Funcion para obtener la descripcion de la pagina y registrarla en la BD
    try:
        for direcciones in bs.find_all('meta',{'name':'description'}): #Obtenemos los meta de la pagina con el name = description
            desc = direcciones.get('content')  #Obtenemos el content de esos meta
            enlaces.update_one({'direccion':direccion}, {'$set': {'descripcion':desc}}) #Guardamos la descripcion al enlace que corresponde
    except:
        print('Ha ocurrido un error con la descripción')

def insertarKeywords(direccion): #Funciòn pra obtener los keywords de la pagina y registrarlos en la BD
    try:
        for direcciones in bs.find_all('meta',{'name':'keywords'}): # Obtenemos los meta de la pagina con el name = keywords
            keywords = direcciones.get('content')#Obtenemos el content de esos meta
            enlaces.update_one({'direccion':direccion}, {'$set': {'keywords':keywords}}) #Guardamos los keywords al enlace que corresponde
    except:
        print('Ha ocurrido un error ccon las keywords')


def insertarTitulo(direccion, titulo): #Funcion para insertar en la BD el titulo de la pagina
    try:
        query = { '$set':{'titulo': titulo}} #Query para insertar el dato del titulo 
        enlaces.update_one({"direccion":direccion}, query) #Guardamos el titulo al enlace que corresponde
    except:
        print('Ha ocurrido un error con un titulo')

def contar_palabras(bs,link): #Funcion para contar las palabras del HTML, resibimos el html y el link de la pagina
    try:
        texto = bs.getText().lower() #Pasamos el HTML a texto y lo convertimos a minisculas
        result = re.sub(r'[^\w\s]','',texto) #eliminamos las comillas simples, comillas dobles, parentesis, etc.
        stop_words = set(stopwords.words('spanish')) #indicamos una "lista" de pronombres en español
        word_tokens = word_tokenize(result) #separamos las cadenas del texto en palabras

        word_tokens = list(filter(lambda token: token not in  string.punctuation,word_tokens)) #eliminamos los signos de puntuacion

        filtro = [] #arreglo que nos servira como filtro más adelante
        for palabra in word_tokens: #verificamos cada palabra que hay en la lista sin singnos de puntuacion
            if palabra not in stop_words: #verificamos si la palabra no esta en la lista de pronombres
                filtro.append(palabra) #si no esta, la añadimos a arreglo filtro

        c=Counter(filtro) #contamos las palabras que se repitan en el arreglo de filtro

        num = 3 #Numero de palabras a mostrar

        y=OrderedDict(c.most_common(num)) #le indicamos que las ordene de las más repetidas a las que menos se repitan, solo se mostraran el numero de palabras que se haya ingresado anteriormente
        lista = list(y)[:3] #Pasamos a una lista las palabras mas comunes
        repetido = list(y.values()) #pasamos a una lista el numero que veces que se repite cada palabra

        #Guardamos en la BD las palabras que màs se repiten
        enlaces.update_one({'direccion':link}, {'$set': {'palabra1': lista[0]}})
        enlaces.update_one({'direccion':link}, {'$set': {'palabra2': lista[1]}})
        enlaces.update_one({'direccion':link}, {'$set': {'palabra3': lista[2]}})

        #Guardamos en la BD cuantas veces se repite cada palabra
        enlaces.update_one({'direccion':link}, {'$set': {'ranking1': repetido[0]}})
        enlaces.update_one({'direccion':link}, {'$set': {'ranking2': repetido[1]}})
        enlaces.update_one({'direccion':link}, {'$set': {'ranking3': repetido[2]}})
    except:
        print('Ha ocurrido un error con las palabras')


client = MongoClient('localhost')   # Conexion a la Base de Datos.
db = client['motor']    # Creamos la Base de Datos.
enlaces = db['enlaces'] # Creamos una Coleccion.


for revisadas in enlaces.find(): #Obtenemos los datos de la BD
    if(revisadas['revisada'] == 1): #Verificamos cuales han sido revisadas
        direccion = revisadas['direccion'] #Obtenemos el enlace de las que ya han sido verificadas
       
        req = Request(direccion, headers={'User-Agent': 'Mozilla/5.0'})
        url = urlopen(req)
        
        bs = BeautifulSoup(url.read(), 'html.parser')
        contar_palabras(bs,direccion) #Mandamos llamar a la funcion y le pasamos el HTML de la pagina
        heads = bs.find_all('head') #Buscamos todos los head que hay en la pagina
        for  entrada in heads:
            try:
                titulo = entrada.title.string #Buscamos la etiqueda titulo que hay en los head y obtenemos el texto
                insertarTitulo(direccion,titulo)#Mandamos llamar a la funcion y le pasamos el enlace de la pagina y el titulo
                insertarKeywords(direccion)#Mandamos llamar a la funcion y le pasamos el enlace de la pagina
                insertarDesc(direccion)#Mandamos llamar a la funcion y le pasamos el enlace de la pagina
            except:
                print('Ha ocurrido un error con los enlaces')
