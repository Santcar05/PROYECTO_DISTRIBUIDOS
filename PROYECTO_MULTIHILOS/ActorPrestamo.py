"""
Actor_Prestamo_Multihilo.py
Actor con POOL DE HILOS para procesar préstamos en PARALELO.
"""

import zmq
import json
import logging
import time
import threading
import queue
from Clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] Actor_Prestamo_MT: %(message)s")

context = zmq.Context()

# Socket SUB
sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://10.43.102.40:5556")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "prestamo")
logging.info("Suscrito al tópico 'prestamo' (MULTIHILO)")

# Configuración de GAs
GESTORES = [
    {"ip": "localhost", "puerto": 5557, "nombre": "GA-SedeA"},
    {"ip": "localhost", "puerto": 5559, "nombre": "GA-SedeB"}
]

# CONFIGURACIÓN MULTIHILO
NUM_HILOS = 4

# Cola de peticiones
cola_peticiones = queue.Queue()

def crear_socket_req(ga):
    """Crea socket REQ para un GA"""
    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{ga['ip']}:{ga['puerto']}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    return sock

def trabajador(id_trabajador):
    """Hilo trabajador que procesa préstamos"""
    logging.info(f"[TRABAJADOR-{id_trabajador}] Iniciado")
    
    # Cada trabajador tiene sus propios sockets
    sockets_ga = [crear_socket_req(ga) for ga in GESTORES]
    gestor_local = 0
    
    while True:
        try:
            try:
                peticion = cola_peticiones.get(timeout=1)
            except queue.Empty:
                continue
            
            libro_usuario = peticion['libro_usuario']
            logging.info(f"[T-{id_trabajador}] Procesando: {libro_usuario.codigo}")
            
            # Verificar disponibilidad
            msg_verificar = {
                "operacion": "verificar_disponibilidad",
                "libro_usuario": libro_usuario.to_dict()
            }
            
            respuesta_disp = None
            for _ in range(len(GESTORES)):
                try:
                    sockets_ga[gestor_local].send_string(json.dumps(msg_verificar))
                    respuesta_disp = sockets_ga[gestor_local].recv_string()
                    break
                except zmq.Again:
                    gestor_local = (gestor_local + 1) % len(GESTORES)
            
            if not respuesta_disp:
                logging.error(f"[T-{id_trabajador}] Fallo verificación")
                cola_peticiones.task_done()
                continue
            
            resp_data = json.loads(respuesta_disp)
            
            if not resp_data.get("disponible", False):
                logging.warning(f"[T-{id_trabajador}] No disponible")
                cola_peticiones.task_done()
                continue
            
            # Registrar préstamo
            msg_prestamo = {
                "operacion": "prestamo",
                "libro_usuario": libro_usuario.to_dict()
            }
            
            respuesta_prestamo = None
            for _ in range(len(GESTORES)):
                try:
                    sockets_ga[gestor_local].send_string(json.dumps(msg_prestamo))
                    respuesta_prestamo = sockets_ga[gestor_local].recv_string()
                    break
                except zmq.Again:
                    gestor_local = (gestor_local + 1) % len(GESTORES)
            
            if respuesta_prestamo:
                logging.info(f"[T-{id_trabajador}] ✓ Préstamo registrado")
            else:
                logging.error(f"[T-{id_trabajador}] ✗ Fallo préstamo")
            
            cola_peticiones.task_done()
            
        except Exception as e:
            logging.error(f"[T-{id_trabajador}] Error: {e}")
            time.sleep(0.5)

def receptor_mensajes():
    """Recibe mensajes del tópico y los agrega a la cola"""
    logging.info("Receptor iniciado")
    
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
            
            peticion = {'libro_usuario': libro_usuario}
            cola_peticiones.put(peticion)
            
            logging.info(f"📨 Petición en cola: {libro_usuario.codigo} (Cola: {cola_peticiones.qsize()})")
            
        except Exception as e:
            logging.error(f"Error receptor: {e}")
            time.sleep(0.5)

if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info(f"Actor Préstamo MULTIHILO - {NUM_HILOS} hilos")
    logging.info("=" * 60)
    
    # Iniciar receptor
    threading.Thread(target=receptor_mensajes, daemon=True).start()
    
    # Iniciar trabajadores
    for i in range(NUM_HILOS):
        threading.Thread(target=trabajador, args=(i,), daemon=True).start()
    
    logging.info(f"✓ {NUM_HILOS} trabajadores activos")
    
    try:
        while True:
            time.sleep(10)
            logging.info(f"📊 Cola: {cola_peticiones.qsize()} peticiones")
    except KeyboardInterrupt:
        logging.info("Deteniendo...")