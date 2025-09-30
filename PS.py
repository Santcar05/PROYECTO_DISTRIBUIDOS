import threading
import zmq
import time

# Contexto y socket compartido
context = zmq.Context()
socket = context.socket(zmq.REQ)
# Colocar la IP del Gestor de Carga(la VM)
socket.connect("tcp://10.43.102.40:5555")

lock = threading.Lock()  # controla acceso al socket

# -------- PRIMERA ENTREGA ---------
def devolverLibro():
    i = 0
    while True:
        with lock:
            print("[A] devolverLibro enviando petición...")
            socket.send_string("Petición: devolver libro")
            respuesta = socket.recv_string()
            print("[A] devolverLibro recibió:", respuesta)
            print("Mensaje", i, "enviado")
        i += 1
        time.sleep(1)  # Simula tiempo entre peticiones


def renovarLibro():
    i = 0
    while True:
        with lock:
            print("[A] renovarLibro enviando petición...")
            socket.send_string("Petición: renovar libro")
            respuesta = socket.recv_string()
            print("[A] renovarLibro recibió:", respuesta)
            print("Mensaje", i, "enviado")
        i += 1
        time.sleep(2)


def solicitarPrestamosLibro():
    i = 0
    while True:
        with lock:
            print("[A] solicitarPrestamosLibro enviando petición...")
            socket.send_string("Petición: solicitar préstamo de libro")
            respuesta = socket.recv_string()
            print("[A] solicitarPrestamosLibro recibió:", respuesta)
            print("Mensaje", i, "enviado")
        i += 1
        time.sleep(3)


# Lanzar las tres funciones como hilos
if __name__ == "__main__":
    t1 = threading.Thread(target=devolverLibro)
    t2 = threading.Thread(target=renovarLibro)
    t3 = threading.Thread(target=solicitarPrestamosLibro)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()
