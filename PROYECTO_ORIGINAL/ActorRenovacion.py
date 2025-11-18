#!/usr/bin/env python3
"""
Actor_Renovacion.py (con tolerancia a fallos)
Actor suscrito al tópico 'renovacion' que procesa renovaciones de libros
con failover automático entre gestores de almacenamiento.
"""

import zmq
import json
import logging
import time
from datetime import datetime, timedelta
from clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] Actor_Renovacion: %(message)s")

context = zmq.Context()

# Socket SUB para recibir publicaciones del GC
sub_socket = context.socket(zmq.SUB)
#sub_socket.connect("tcp://10.43.102.40:5556")  # Cambiar según IP del GC
sub_socket.connect("tcp://localhost:5556")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "renovacion")
logging.info("Suscrito al tópico 'renovacion'")

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
            time.sleep(1)  # Espera antes de reintentar
            
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

def guardar_operacion_fallida(libro_usuario, nueva_fecha):
    """Guarda operaciones que no pudieron procesarse para reintentar después"""
    try:
        with open("renovaciones_fallidas.log", "a", encoding="utf-8") as f:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "libro_usuario": libro_usuario.to_dict(),
                "nueva_fecha": nueva_fecha
            }
            f.write(json.dumps(log_entry) + "\n")
        logging.info("Operación guardada en renovaciones_fallidas.log para reintento posterior")
    except Exception as e:
        logging.error(f"Error guardando operación fallida: {e}")

if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Actor de Renovación iniciado con TOLERANCIA A FALLOS")
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

            logging.info(f"📖 Procesando renovación: [{libro_usuario.codigo}] {libro_usuario.titulo}")

            # Calcular nueva fecha de devolución (+1 semana)
            try:
                fecha_actual = datetime.fromisoformat(libro_usuario.fecha_devolucion)
            except:
                # Si la fecha no es válida, usar fecha actual
                fecha_actual = datetime.now()
            
            nueva_fecha = (fecha_actual + timedelta(weeks=1)).isoformat()
            libro_usuario.fecha_devolucion = nueva_fecha
            
            logging.info(f"📅 Nueva fecha de devolución: {nueva_fecha}")

            # Preparar mensaje para el GA
            mensaje_ga = {
                "operacion": "renovacion",
                "libro_usuario": libro_usuario.to_dict(),
                "timestamp": time.time()
            }

            # Enviar con failover automático
            respuesta = enviar_con_failover(mensaje_ga)
            
            if respuesta:
                resp_data = json.loads(respuesta)
                if resp_data.get("exito"):
                    logging.info(f"✓✓✓ Renovación procesada exitosamente: {resp_data.get('mensaje')}")
                else:
                    logging.warning(f"⚠ Renovación rechazada: {resp_data.get('mensaje')}")
                    guardar_operacion_fallida(libro_usuario, nueva_fecha)
            else:
                logging.error("✗✗✗ FALLO: No se pudo procesar la renovación")
                guardar_operacion_fallida(libro_usuario, nueva_fecha)

        except json.JSONDecodeError as e:
            logging.error(f"✗ Error parseando JSON: {e}")
        except KeyboardInterrupt:
            logging.info("Deteniendo Actor de Renovación...")
            break
        except Exception as e:
            logging.error(f"✗ Error procesando mensaje: {e}")
            time.sleep(1)  # Pausa antes de continuar