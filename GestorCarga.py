import zmq
import threading
import json
from clases import LibroUsuario

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

if __name__ == "__main__":
    while True:
        mensaje = socket.recv_string()
        print("Mensaje recibido:", mensaje)
        try:
            data = json.loads(mensaje)
            operacion = data.get("operacion")
            libro_usuario_dict = data.get("libro_usuario", {})
            if operacion == "devolucion":
                print("Has devuelto un libro")
                libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
                print(f"Libro devuelto: {libro_usuario.codigo}, {libro_usuario.titulo}, {libro_usuario.autor}")
                socket.send_string("Devolución procesandose")
            elif operacion == "renovacion":
                print("Has renovado un libro")
                libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
                print(f"Libro renovado: {libro_usuario.codigo}, {libro_usuario.titulo}, {libro_usuario.autor}")
                socket.send_string("Renovación procesada")
            elif operacion == "prestamo":
                print("Has solicitado un préstamo de libro")
                libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
                print(f"Libro solicitado: {libro_usuario.codigo}, {libro_usuario.titulo}, {libro_usuario.autor}")
                socket.send_string("Préstamo procesado")
            else:
                print("Operación desconocida")
                socket.send_string("Operación desconocida")
        except Exception as e:
            print("Error procesando mensaje:", e)
            socket.send_string("Error en el formato del mensaje")
    