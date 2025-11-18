#!/usr/bin/env python3
"""
GestorCarga.py
Gestor de Carga que recibe peticiones de PS y:
- Publica en tópicos (PUB/SUB) las operaciones.
"""

import zmq
import json
import logging
from Clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] GC: %(message)s")

context = zmq.Context()

# Socket REP para PS -> GC
rep_socket = context.socket(zmq.REP)
rep_socket.bind("tcp://*:5555")
logging.info("Socket REP escuchando en tcp://*:5555")

# Socket PUB para GC -> Actores
pub_socket = context.socket(zmq.PUB)
pub_socket.bind("tcp://*:5556")
logging.info("Socket PUB escuchando en tcp://*:5556")

if __name__ == "__main__":
    logging.info("Gestor de Carga iniciado")

    while True:
        try:
            mensaje = rep_socket.recv_string()
            
            try:
                data = json.loads(mensaje)
                operacion = data.get("operacion")
                libro_usuario_dict = data.get("libro_usuario", {})

                if operacion == "devolucion":
                    logging.info(f"Devolución: {libro_usuario_dict.get('codigo')}")
                    rep_socket.send_string("Devolución enviada al Actor")
                    pub_socket.send_string(f"devolucion {json.dumps(data)}")

                elif operacion == "renovacion":
                    logging.info(f"Renovación: {libro_usuario_dict.get('codigo')}")
                    rep_socket.send_string("Renovación enviada al Actor")
                    pub_socket.send_string(f"renovacion {json.dumps(data)}")
                    
                elif operacion == "prestamo":
                    logging.info(f"Préstamo: {libro_usuario_dict.get('codigo')}")
                    rep_socket.send_string("Préstamo procesado en el GC")
                    pub_socket.send_string(f"prestamo {json.dumps(data)}")
                else:
                    logging.warning(f"Operación desconocida: {operacion}")
                    rep_socket.send_string("Operación desconocida")

            except json.JSONDecodeError as e:
                logging.error(f"Error parseando JSON: {e}")
                rep_socket.send_string("Error: mensaje no es JSON válido")

        except Exception as e:
            logging.error(f"Error: {e}")
            try:
                rep_socket.send_string("Error interno en el GC")
            except:
                pass