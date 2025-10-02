import threading
import zmq
import time
import json

from clases import LibroUsuario

# Contexto y socket compartido
context = zmq.Context()
socket = context.socket(zmq.REQ)
# Colocar la IP del Gestor de Carga(la VM)
#socket.connect("tcp://10.43.102.40:5555")
socket.connect("tcp://localhost:5555")

lock = threading.Lock()  # controla acceso al socket

# _________________ PRIMERA ENTREGA ______________________________________________
def devolverLibro(codigo, titulo, autor, sede):
    i = 0

    with lock:
        # Crear el libroUsuario a devolver
        libroUsuario = LibroUsuario(codigo, titulo, autor, sede)
        print(f"[A] devolverLibro enviando petición... {codigo} {titulo} {autor} {sede}")
        
        # Crear mensaje estructurado
        mensaje = {
            "operacion": "devolucion",
            "libro_usuario": libroUsuario.to_dict(),
            "timestamp": time.time()
        }
        
        try:
            #Enviar mensaje JSON al gestor de carga
            socket.send_string(json.dumps(mensaje))
            print("Mensaje enviado al Gestor de Carga")
            
            #Esperar respuesta (Request-Reply)
            if socket.poll(5000):  #Timeout de 5 segundos
                respuesta = socket.recv_string()
                print(f"Respuesta del GC: {respuesta}")
            else:
                print("Timeout: No se recibió respuesta del GC")
                
        except Exception as e:
            print(f"Error enviando mensaje: {e}")
        
        i += 1
        time.sleep(1)  # Simula tiempo entre peticiones
        
#----------------------------------------------------------------------------------------------------------------------------


def renovarLibro(isbn, titulo, autor, sede):
    i = 0
    with lock:
        print("[A] renovarLibro enviando petición...", isbn, titulo, autor, sede)
        mensaje = {
            "operacion": "renovacion",
            "libro_usuario": {
                "codigo": isbn,
                "titulo": titulo,
                "autor": autor,
                "sede": sede
            },
            "timestamp": time.time()
        }
        socket.send_string(json.dumps(mensaje))
        respuesta = socket.recv_string()
        print("[A] renovarLibro recibió:", respuesta)
        print("Mensaje", i, "enviado")
        i += 1
        time.sleep(2)

def solicitarPrestamosLibro(isbn, titulo, autor, sede):
    i = 0
    with lock:
        print("[A] solicitarPrestamosLibro enviando petición...", isbn, titulo, autor, sede)
        mensaje = {
            "operacion": "prestamo",
            "libro_usuario": {
                "codigo": isbn,
                "titulo": titulo,
                "autor": autor,
                "sede": sede
            },
            "timestamp": time.time()
        }
        socket.send_string(json.dumps(mensaje))
        respuesta = socket.recv_string()
        print("[A] solicitarPrestamosLibro recibió:", respuesta)
        print("Mensaje", i, "enviado")
        i += 1
        time.sleep(3)


# Lanzar las tres funciones como hilos
if __name__ == "__main__":
    #Leer de un archivo de texto los requerimientos de prestamos
    file = open("peticiones.txt", "r", encoding="utf-8")
    conten = file.readlines()
    file.close()
    
#Formato del archivo:
#TIPO,ISBN,USUARIO,SEDE
#Dependiendo de el tipo de petición, se llamará a una función u otra
    threads = []
    for line in conten:
        parts = line.strip().split(',')
        if len(parts) != 5:
            print("Línea malformada:", line)
            continue
        tipo, isbn, titulo, autor, sede = parts
        if tipo == "DEVOLUCION":
            t = threading.Thread(target=devolverLibro, args = (isbn, titulo, autor,sede))
        elif tipo == "RENOVACION":
            t = threading.Thread(target=renovarLibro,args = (isbn, titulo, autor,sede) )
        elif tipo == "PRESTAMO":
            t = threading.Thread(target=solicitarPrestamosLibro,args = (isbn, titulo, autor,sede))
        else:
            print("Tipo desconocido:", tipo)
            continue
        threads.append(t)
        t.start()
        
        
        
#    t1 = threading.Thread(target=devolverLibro)
#    t2 = threading.Thread(target=renovarLibro)
#    t3 = threading.Thread(target=solicitarPrestamosLibro)

#    t1.start()
#    t2.start()
#    t3.start()

#    t1.join()
#    t2.join()
#    t3.join()

#--------------------------------------------------------------------------------
# 2. Serializa y envía el objeto:
#with lock:
#    peticion = Peticion("DEVOLUCION", isbn, usuario, sede)
#    data = pickle.dumps(peticion)
#    socket.send(data)
#    respuesta = socket.recv_string()
#    print("[A] devolverLibro recibió:", respuesta)
# ...existing code...

#------------------------------------------------------------------------------------------
#import pickle  # Agrega esta importación

# 1. Definir clase de petición:
#class Peticion:
#    def __init__(self, tipo, isbn, usuario, sede):
#        self.tipo = tipo
#        self.isbn = isbn
#        self.usuario = usuario
#        self.sede = sede
# ...existing code...
#
#