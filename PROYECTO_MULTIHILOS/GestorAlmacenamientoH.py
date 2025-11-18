#!/usr/bin/env python3
"""
GestorAlmacenamiento_Multihilo.py (VERSIÓN MEJORADA)
Gestor con POOL DE HILOS para procesar peticiones en PARALELO.
"""

import zmq
import json
import logging
import threading
import time
import os
import sys
import queue
from datetime import datetime
from Clases import LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] GA-%(sede)s: %(message)s")

if len(sys.argv) < 2:
    print("Uso: python GestorAlmacenamiento_Multihilo.py <SedeA|SedeB>")
    sys.exit(1)

SEDE = sys.argv[1]

# Configuración según sede
if SEDE == "SedeA":
    PUERTO_REP = 5557
    BD_PRIMARIA = "BD_SedeA.txt"
    BD_PRESTAMOS = "BD_Prestamos_SedeA.txt"
elif SEDE == "SedeB":
    PUERTO_REP = 5559
    BD_PRIMARIA = "BD_SedeB.txt"
    BD_PRESTAMOS = "BD_Prestamos_SedeB.txt"
else:
    print("Sede debe ser 'SedeA' o 'SedeB'")
    sys.exit(1)

logger = logging.LoggerAdapter(logging.getLogger(), {'sede': SEDE})

# CONFIGURACIÓN MULTIHILO
NUM_HILOS_TRABAJADORES = 4  # ← AQUÍ SE CONFIGURA EL NÚMERO DE HILOS

# Estado compartido
bd_lock = threading.Lock()
context = zmq.Context()

# Cola de peticiones
cola_peticiones = queue.Queue()

# Socket REP (recibe peticiones)
rep_socket = context.socket(zmq.REP)
rep_socket.bind(f"tcp://*:{PUERTO_REP}")
logger.info(f"Socket REP escuchando en tcp://*:{PUERTO_REP}")

def cargar_bd():
    """Carga la BD en memoria"""
    libros = {}
    try:
        with open(BD_PRIMARIA, 'r', encoding='utf-8') as f:
            for linea in f:
                partes = linea.strip().split('|')
                if len(partes) >= 5:
                    codigo = partes[0]
                    libros[codigo] = {
                        'titulo': partes[1],
                        'autor': partes[2],
                        'ejemplares': int(partes[3]),
                        'sede': partes[4]
                    }
        logger.info(f"BD cargada: {len(libros)} libros desde {BD_PRIMARIA}")
    except FileNotFoundError:
        logger.warning(f"BD no encontrada, creando {BD_PRIMARIA}")
        with open(BD_PRIMARIA, 'w', encoding='utf-8') as f:
            pass
    return libros

def guardar_bd(libros):
    """Guarda BD en disco"""
    try:
        with open(BD_PRIMARIA, 'w', encoding='utf-8') as f:
            for codigo, info in libros.items():
                f.write(f"{codigo}|{info['titulo']}|{info['autor']}|{info['ejemplares']}|{info['sede']}\n")
    except Exception as e:
        logger.error(f"Error guardando BD: {e}")

def registrar_prestamo(libro_usuario):
    """Registra préstamo en archivo"""
    try:
        with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
            f.write(f"{libro_usuario.codigo}|{libro_usuario.titulo}|{libro_usuario.autor}|"
                   f"{libro_usuario.fecha_prestamo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
    except Exception as e:
        logger.error(f"Error registrando préstamo: {e}")

def trabajador(id_trabajador, libros):
    """
    Hilo trabajador que procesa peticiones en PARALELO.
    """
    logger.info(f"[TRABAJADOR-{id_trabajador}] Iniciado")
    
    while True:
        try:
            # Obtener petición de la cola (timeout 1 segundo)
            try:
                item = cola_peticiones.get(timeout=1)
            except queue.Empty:
                continue
            
            data = item['data']
            socket_respuesta = item['socket']
            
            operacion = data.get("operacion")
            libro_usuario_dict = data.get("libro_usuario", {})
            libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
            
            logger.info(f"[TRABAJADOR-{id_trabajador}] Procesando {operacion}: {libro_usuario.codigo}")
            
            respuesta = {}
            
            # Procesar según operación
            if operacion == "devolucion":
                with bd_lock:
                    codigo = libro_usuario.codigo
                    if codigo in libros:
                        libros[codigo]['ejemplares'] += 1
                        guardar_bd(libros)
                        respuesta = {"exito": True, "mensaje": f"Devolución en {SEDE}. Ejemplares: {libros[codigo]['ejemplares']}"}
                    else:
                        respuesta = {"exito": False, "mensaje": "Libro no encontrado"}
            
            elif operacion == "renovacion":
                try:
                    with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
                        f.write(f"RENOVACION|{libro_usuario.codigo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
                    respuesta = {"exito": True, "mensaje": f"Renovación en {SEDE}"}
                except Exception as e:
                    respuesta = {"exito": False, "mensaje": str(e)}
            
            elif operacion == "verificar_disponibilidad":
                with bd_lock:
                    codigo = libro_usuario.codigo
                    if codigo in libros and libros[codigo]['ejemplares'] > 0:
                        respuesta = {"disponible": True, "ejemplares": libros[codigo]['ejemplares'], "sede": SEDE}
                    else:
                        respuesta = {"disponible": False, "mensaje": "No disponible", "sede": SEDE}
            
            elif operacion == "prestamo":
                with bd_lock:
                    codigo = libro_usuario.codigo
                    if codigo in libros and libros[codigo]['ejemplares'] > 0:
                        libros[codigo]['ejemplares'] -= 1
                        guardar_bd(libros)
                        registrar_prestamo(libro_usuario)
                        respuesta = {"exito": True, "mensaje": f"Préstamo en {SEDE}. Restantes: {libros[codigo]['ejemplares']}"}
                    else:
                        respuesta = {"exito": False, "mensaje": "No se pudo realizar"}
            
            else:
                respuesta = {"exito": False, "mensaje": "Operación desconocida"}
            
            # Enviar respuesta
            socket_respuesta.send_string(json.dumps(respuesta))
            
            logger.info(f"[TRABAJADOR-{id_trabajador}] ✓ Respuesta enviada: {respuesta.get('mensaje', respuesta)}")
            
            cola_peticiones.task_done()
            
        except Exception as e:
            logger.error(f"[TRABAJADOR-{id_trabajador}] Error: {e}")
            try:
                socket_respuesta.send_string(json.dumps({"exito": False, "mensaje": str(e)}))
            except:
                pass

def receptor_peticiones():
    """
    Hilo que recibe peticiones del socket REP y las distribuye a los trabajadores.
    """
    logger.info("Receptor de peticiones iniciado")
    
    while True:
        try:
            mensaje = rep_socket.recv_string()
            data = json.loads(mensaje)
            
            # Agregar a cola para que un trabajador la procese
            item = {
                'data': data,
                'socket': rep_socket  # Socket para enviar respuesta
            }
            
            cola_peticiones.put(item)
            logger.info(f"📨 Petición agregada a cola (Tamaño: {cola_peticiones.qsize()})")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parseando JSON: {e}")
            rep_socket.send_string(json.dumps({"exito": False, "mensaje": "JSON inválido"}))
        except Exception as e:
            logger.error(f"Error recibiendo petición: {e}")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info(f"Gestor de Almacenamiento {SEDE} MULTIHILO")
    logger.info(f"Número de hilos trabajadores: {NUM_HILOS_TRABAJADORES}")
    logger.info("=" * 60)
    
    # Cargar BD
    libros = cargar_bd()
    
    # Iniciar receptor de peticiones
    hilo_receptor = threading.Thread(target=receptor_peticiones, daemon=True)
    hilo_receptor.start()
    
    # Iniciar pool de trabajadores
    hilos = []
    for i in range(NUM_HILOS_TRABAJADORES):
        hilo = threading.Thread(target=trabajador, args=(i, libros), daemon=True)
        hilo.start()
        hilos.append(hilo)
    
    logger.info(f"✓ {NUM_HILOS_TRABAJADORES} trabajadores iniciados")
    
    try:
        while True:
            time.sleep(10)
            logger.info(f"📊 Estado: Cola={cola_peticiones.qsize()} peticiones")
    except KeyboardInterrupt:
        logger.info("Deteniendo GA...")