import zmq
import threading

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

def devolverLibro():
    while True:
        mensaje = socket.recv_string()
        print("Has devuelto el libro")
        socket.send_string("Devolución procesada")
        print("Mensaje recibido:", mensaje)
    
def renovarLibro():
    print("Has renovado el libro")
    
def solicitarPrestamosLibro():
    print("Has solicitado un prestamo de libro")
    # El préstamo se puede solicitar por un periodo máximo de 2 semanas
    
    
devolverLibro()
    