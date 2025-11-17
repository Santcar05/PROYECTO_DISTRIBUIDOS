#!/usr/bin/env python3
"""
Actor_Prestamo.py (con tolerancia a fallos)
Actor suscrito al tópico 'prestamo' que valida disponibilidad de libros
con failover automático entre gestores de almacenamiento.
"""

import zmq
import json
import logging
import time
from clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] Actor_Prestamo: %(message)s")

context = zmq.Context()

# Socket SUB para recibir publicaciones del GC
sub_socket = context.socket(zmq.SUB)
#sub_socket.connect("tcp://10.43.102.40:5556")  # Cambiar según IP del GC
sub_socket.connect("tcp://localhost:5556")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "prestamo")
logging.info("Suscrito al tópico 'prestamo'")

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
    """
    Intenta enviar a GA primario, si falla usa el secundario.
    Implementa failover automático con reintentos.
    """
    global gestor_actual
    
    intentos = len(GESTORES)
    
    for intento in range(intentos):
        try:
            logging.info(f"Intento {intento + 1}/{intentos} con {GESTORES[gestor_actual]['nombre']}")
            
            # Enviar mensaje
            req_sockets[gestor_actual].send_string(json.dumps(mensaje_ga))
            
            # Esperar respuesta con timeout
            respuesta = req_sockets[gestor_actual].recv_string()
            
            logging.info(f"✓ Respuesta de {GESTORES[gestor_actual]['nombre']}: {respuesta}")
            return respuesta
            
        except zmq.Again:
            logging.warning(f"✗ {GESTORES[gestor_actual]['nombre']} no responde (timeout), cambiando a failover...")
            gestor_actual = (gestor_actual + 1) % len(GESTORES)
            time.sleep(1)
            
        except zmq.ZMQError as e:
            logging.error(f"✗ Error ZMQ con {GESTORES[gestor_actual]['nombre']}: {e}")
            gestor_actual = (gestor_actual + 1) % len(GESTORES)
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"✗ Error general con {GESTORES[gestor_actual]['nombre']}: {e}")
            gestor_actual = (gestor_actual + 1) % len(GESTORES)
            time.sleep(1)
    
    logging.error("✗✗✗ FALLO TOTAL: Todos los gestores de almacenamiento no responden")
    return None

def guardar_operacion_fallida(libro_usuario, tipo_operacion):
    """Guarda operaciones que no pudieron procesarse"""
    try:
        with open("prestamos_fallidos.log", "a", encoding="utf-8") as f:
            log_entry = {
                "timestamp": time.time(),
                "tipo": tipo_operacion,
                "libro_usuario": libro_usuario.to_dict()
            }
            f.write(json.dumps(log_entry) + "\n")
        logging.info("Operación guardada en prestamos_fallidos.log para reintento posterior")
    except Exception as e:
        logging.error(f"Error guardando operación fallida: {e}")

if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Actor de Préstamo iniciado con TOLERANCIA A FALLOS")
    logging.info(f"Gestores configurados: {len(GESTORES)}")
    for i, ga in enumerate(GESTORES):
        logging.info(f"  [{i}] {ga['nombre']} - {ga['ip']}:{ga['puerto']}")
    logging.info("=" * 60)

    while True:
        try:
            # Recibir mensaje del tópico
            mensaje = sub_socket.recv_string()
            logging.info(f"📨 Mensaje recibido: {mensaje[:100]}...")

            # Parsear mensaje
            parts = mensaje.split(" ", 1)
            if len(parts) < 2:
                logging.warning("⚠ Mensaje malformado (sin JSON)")
                continue

            topico, json_data = parts
            data = json.loads(json_data)

            libro_usuario_dict = data.get("libro_usuario", {})
            libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)

            logging.info(f"📖 Procesando préstamo: [{libro_usuario.codigo}] {libro_usuario.titulo}")

            # PASO 1: Verificar disponibilidad
            mensaje_verificar = {
                "operacion": "verificar_disponibilidad",
                "libro_usuario": libro_usuario.to_dict(),
                "timestamp": time.time()
            }

            logging.info("🔍 Verificando disponibilidad...")
            respuesta_verificar = enviar_con_failover(mensaje_verificar)
            
            if not respuesta_verificar:
                logging.error("✗✗✗ FALLO: No se pudo verificar disponibilidad")
                guardar_operacion_fallida(libro_usuario, "verificar_disponibilidad")
                continue
            
            resp_data = json.loads(respuesta_verificar)
            
            if not resp_data.get("disponible", False):
                logging.warning(f"⚠ Libro NO disponible: {resp_data.get('mensaje')}")
                logging.warning(f"⚠ Sede consultada: {resp_data.get('sede', 'desconocida')}")
                continue
            
            logging.info(f"✓ Libro DISPONIBLE - Ejemplares: {resp_data.get('ejemplares')} en {resp_data.get('sede')}")
            
            # PASO 2: Registrar préstamo
            mensaje_prestamo = {
                "operacion": "prestamo",
                "libro_usuario": libro_usuario.to_dict(),
                "timestamp": time.time()
            }
            
            logging.info("📝 Registrando préstamo...")
            respuesta_prestamo = enviar_con_failover(mensaje_prestamo)
            
            if respuesta_prestamo:
                resp_prestamo = json.loads(respuesta_prestamo)
                if resp_prestamo.get("exito"):
                    logging.info(f"✓✓✓ Préstamo registrado exitosamente: {resp_prestamo.get('mensaje')}")
                else:
                    logging.warning(f"⚠ Préstamo rechazado: {resp_prestamo.get('mensaje')}")
                    guardar_operacion_fallida(libro_usuario, "prestamo")
            else:
                logging.error("✗✗✗ FALLO: No se pudo registrar el préstamo")
                guardar_operacion_fallida(libro_usuario, "prestamo")

        except json.JSONDecodeError as e:
            logging.error(f"✗ Error parseando JSON: {e}")
        except KeyboardInterrupt:
            logging.info("Deteniendo Actor de Préstamo...")
            break
        except Exception as e:
            logging.error(f"✗ Error procesando mensaje: {e}")
            time.sleep(1)