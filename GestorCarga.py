#!/usr/bin/env python3
"""
GestorCarga.py
Gestor de Carga que recibe peticiones de PS y:
- Publica en tópicos (PUB/SUB) las operaciones de devoluciones y renovaciones.
- Atiende préstamos de manera síncrona (REQ/REP).
"""

import zmq
import json
import logging
from clases import LibroUsuario

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

context = zmq.Context()
publisher = context.socket(zmq.PUB)
publisher.bind("tcp://*:5556")


# Socket REP para PS -> GC (préstamos, devoluciones, renovaciones)
rep_socket = context.socket(zmq.REP)
rep_socket.bind("tcp://*:5555")
logging.info("Socket REP escuchando en tcp://*:5555")

# Socket PUB para GC -> Actores (devoluciones y renovaciones)
pub_socket = context.socket(zmq.PUB)
pub_socket.bind("tcp://*:5556")
logging.info("Socket PUB escuchando en tcp://*:5556 (para actores)")

if __name__ == "__main__":
    logging.info("Gestor de Carga iniciado. Esperando mensajes de PS...")

    while True:
        try:
            mensaje = rep_socket.recv_string()
            logging.info("Mensaje recibido del PS: %s", mensaje)

            try:
                data = json.loads(mensaje)
                operacion = data.get("operacion")
                libro_usuario_dict = data.get("libro_usuario", {})

                # Convertir a objeto de dominio
                libro_usuario = None
                try:
                    libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
                except Exception:
                    pass

                # --- Procesar operación ---
                if operacion == "devolucion":
                    logging.info("Petición de devolución recibida para libro: %s", libro_usuario_dict)
                    # Responder ACK inmediato al PS
                    rep_socket.send_string("Devolución enviada al Actor")
                    # Publicar en tópico devolucion
                    pub_socket.send_string(f"devolucion {json.dumps(data)}")

                elif operacion == "renovacion":
                    logging.info("Petición de renovación recibida para libro: %s", libro_usuario_dict)
                    rep_socket.send_string("Renovación enviada al Actor")
                    pub_socket.send_string(f"renovacion {json.dumps(data)}")
                    
                elif operacion == "prestamo":
                     logging.info("Petición de préstamo recibida para libro: %s", libro_usuario_dict)
    
                    # Responder al PS inmediatamente
                     rep_socket.send_string("Préstamo procesado en el GC (síncrono)")

                    # Publicar el mensaje a los Actores
                     pub_socket.send_string(f"prestamo {json.dumps(data)}")
                else:
                    logging.warning("Operación desconocida: %s", operacion)
                    rep_socket.send_string("Operación desconocida")

            except json.JSONDecodeError as e:
                logging.error("Error parseando JSON: %s", e)
                rep_socket.send_string("Error: mensaje no es JSON válido")

        except Exception as e:
            logging.error("Error procesando mensaje: %s", e)
            try:
                rep_socket.send_string("Error interno en el GC")
            except Exception:
                pass
