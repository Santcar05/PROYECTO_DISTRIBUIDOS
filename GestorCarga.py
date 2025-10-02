import zmq
import threading
import json
from clases import LibroUsuario

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

def devolverLibro():
    while True:
        # Devolver un libro de la clase libroUsuario a la biblioteca
        mensaje = socket.recv_string()
        print("Has devuelto un libro")
        #Avisar al PS que la devolución está en proceso
        socket.send_string("Devolución procesandose")
        print("Mensaje recibido:", mensaje)
        #Convertir el mensaje recibido en un objeto libroUsuario
        data = json.loads(mensaje)
        libro_usuario_dict = data["libro_usuario"]
        libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
        print("----------------------------------------------------")
        print(f"Libro devuelto: {libro_usuario.codigo}, {libro_usuario.titulo}, {libro_usuario.autor}")
        print("----------------------------------------------------")
        # Enviar el libro a devolver a los actores para que lo trabajen
        
    
def renovarLibro():
    while True:
        mensaje = socket.recv_string()
        print("Has renovado un libro")
        socket.send_string("Renovación procesada")
        print("Mensaje recibido:", mensaje)
    
def solicitarPrestamosLibro():
    while True:
        mensaje = socket.recv_string()
        print("Has solicitado un préstamo de libro")
        socket.send_string("Préstamo procesado")
        print("Mensaje recibido:", mensaje)
    # El préstamo se puede solicitar por un periodo máximo de 2 semanas
    
    
devolverLibro()
renovarLibro()
#solicitarPrestamosLibro()


#----------------------------------------

#import pickle  # Agrega esta importación

#while True:
#    data = socket.recv()
#    peticion = pickle.loads(data)
#    print("Tipo:", peticion.tipo)
#    print("ISBN:", peticion.isbn)
#    print("Usuario:", peticion.usuario)
#    print("Sede:", peticion.sede)
#    socket.send_string("Petición procesada")
# ...existing code...
    