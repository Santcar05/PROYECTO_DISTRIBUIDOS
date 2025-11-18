#!/usr/bin/env python3
"""
Actor_Devolucion.py (con tolerancia a fallos)
"""

import zmq
import json
import logging
import time
from clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] Actor_Devolucion: %(message)s")

context = zmq.Context()

# Socket SUB
sub_socket = context.socket(zmq.SUB)
#sub_socket.connect("tcp://10.43.102.40:5556")
sub_socket.connect("tcp://localhost:5556")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "devolucion")
logging.info("Suscrito al tópico 'devolucion'")

# Configuración de GAs (ambas sedes)
GESTORES = [
    #{"ip": "10.43.102.40", "puerto": 5557, "nombre": "GA-SedeA"},
   # {"ip": "10.43.102.41", "puerto": 5559, "nombre": "GA-SedeB"}
   {"ip": "localhost", "puerto": 5557, "nombre": "GA-SedeA"},
    {"ip": "localhost", "puerto": 5559, "nombre": "GA-SedeB"}
]

gestor_actual = 0
req_sockets = []

# Crear sockets REQ para cada GA
for ga in GESTORES:
    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{ga['ip']}:{ga['puerto']}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)  # Timeout 5 segundos
    req_sockets.append(sock)
    logging.info(f"Conectado a {ga['nombre']} en tcp://{ga['ip']}:{ga['puerto']}")

def enviar_con_failover(mensaje_ga):
    """Intenta enviar a GA primario, si falla usa el secundario"""
    global gestor_actual
    
    intentos = len(GESTORES)
    
    for _ in range(intentos):
        try:
            logging.info(f"Intentando con {GESTORES[gestor_actual]['nombre']}")
            
            req_sockets[gestor_actual].send_string(json.dumps(mensaje_ga))
            respuesta = req_sockets[gestor_actual].recv_string()
            
            logging.info(f"Respuesta de {GESTORES[gestor_actual]['nombre']}: {respuesta}")
            return respuesta
            
        except zmq.Again:
            logging.warning(f"{GESTORES[gestor_actual]['nombre']} no responde, cambiando a failover")
            gestor_actual = (gestor_actual + 1) % len(GESTORES)
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error con {GESTORES[gestor_actual]['nombre']}: {e}")
            gestor_actual = (gestor_actual + 1) % len(GESTORES)
            time.sleep(1)
    
    logging.error("Todos los gestores fallaron")
    return None

if __name__ == "__main__":
    logging.info("Actor de Devolución iniciado con tolerancia a fallos")

    while True:
        try:
            mensaje = sub_socket.recv_string()
            parts = mensaje.split(" ", 1)
            if len(parts) < 2:
                continue

            topico, json_data = parts
            data = json.loads(json_data)

            libro_usuario_dict = data.get("libro_usuario", {})
            libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)

            logging.info(f"Procesando devolución: {libro_usuario.codigo}")

            mensaje_ga = {
                "operacion": "devolucion",
                "libro_usuario": libro_usuario.to_dict()
            }

            respuesta = enviar_con_failover(mensaje_ga)
            
            if respuesta:
                logging.info("Devolución procesada exitosamente")
            else:
                logging.error("Fallo al procesar devolución")

        except Exception as e:
            logging.error(f"Error: {e}")