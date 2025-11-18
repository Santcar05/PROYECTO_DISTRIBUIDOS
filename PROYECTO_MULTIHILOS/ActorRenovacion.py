"""
Actor_Renovacion_Multihilo.py
"""

import zmq
import json
import logging
import time
import threading
import queue
from datetime import datetime, timedelta
from Clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] Actor_Renovacion_MT: %(message)s")

context = zmq.Context()

sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://10.43.102.40:5556")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "renovacion")

GESTORES = [
    {"ip": "localhost", "puerto": 5557, "nombre": "GA-SedeA"},
    {"ip": "localhost", "puerto": 5559, "nombre": "GA-SedeB"}
]

NUM_HILOS = 4
cola_peticiones = queue.Queue()

def crear_socket_req(ga):
    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{ga['ip']}:{ga['puerto']}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    return sock

def trabajador(id_trabajador):
    logging.info(f"[TRABAJADOR-{id_trabajador}] Iniciado")
    
    sockets_ga = [crear_socket_req(ga) for ga in GESTORES]
    gestor_local = 0
    
    while True:
        try:
            try:
                peticion = cola_peticiones.get(timeout=1)
            except queue.Empty:
                continue
            
            libro_usuario = peticion['libro_usuario']
            
            # Calcular nueva fecha
            try:
                fecha_actual = datetime.fromisoformat(libro_usuario.fecha_devolucion)
            except:
                fecha_actual = datetime.now()
            
            nueva_fecha = (fecha_actual + timedelta(weeks=1)).isoformat()
            libro_usuario.fecha_devolucion = nueva_fecha
            
            mensaje_ga = {
                "operacion": "renovacion",
                "libro_usuario": libro_usuario.to_dict()
            }
            
            for _ in range(len(GESTORES)):
                try:
                    sockets_ga[gestor_local].send_string(json.dumps(mensaje_ga))
                    respuesta = sockets_ga[gestor_local].recv_string()
                    logging.info(f"[T-{id_trabajador}] ✓ Renovación procesada")
                    break
                except zmq.Again:
                    gestor_local = (gestor_local + 1) % len(GESTORES)
            
            cola_peticiones.task_done()
            
        except Exception as e:
            logging.error(f"[T-{id_trabajador}] Error: {e}")

def receptor_mensajes():
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
            
            cola_peticiones.put({'libro_usuario': libro_usuario})
            
        except Exception as e:
            logging.error(f"Error: {e}")

if __name__ == "__main__":
    logging.info(f"Actor Renovación MULTIHILO - {NUM_HILOS} hilos")
    
    threading.Thread(target=receptor_mensajes, daemon=True).start()
    
    for i in range(NUM_HILOS):
        threading.Thread(target=trabajador, args=(i,), daemon=True).start()
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Deteniendo...")