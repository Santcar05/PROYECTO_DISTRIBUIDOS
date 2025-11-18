"""
GestorAlmacenamiento.py
Gestor con patrón ROUTER-DEALER para concurrencia
"""

import zmq
import json
import logging
import threading
import time
import shutil
import os
import sys
from datetime import datetime
from Clases import LibroBiblioteca, LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] GA-%(sede)s: %(message)s")

if len(sys.argv) < 2:
    print("Uso: python GestorAlmacenamiento.py <SedeA|SedeB>")
    sys.exit(1)

SEDE = sys.argv[1]

# Configuración según sede
if SEDE == "SedeA":
    PUERTO_FRONTEND = 5557
    BD_PRIMARIA = "BD_SedeA.txt"
    BD_PRESTAMOS = "BD_Prestamos_SedeA.txt"
elif SEDE == "SedeB":
    PUERTO_FRONTEND = 5559
    BD_PRIMARIA = "BD_SedeB.txt"
    BD_PRESTAMOS = "BD_Prestamos_SedeB.txt"
else:
    print("Sede debe ser 'SedeA' o 'SedeB'")
    sys.exit(1)

logger = logging.LoggerAdapter(logging.getLogger(), {'sede': SEDE})

# Puerto interno para workers
PUERTO_BACKEND = PUERTO_FRONTEND + 1000  # 6557 o 6559

bd_lock = threading.Lock()
context = zmq.Context()

def cargar_bd():
    """Carga la BD en memoria"""
    libros = {}
    try:
        with open(BD_PRIMARIA, 'r', encoding='utf-8') as f:
            for i, linea in enumerate(f):
                if i == 0 and ('codigo' in linea.lower() or 'titulo' in linea.lower()):
                    continue
                
                partes = linea.strip().split('|')
                if len(partes) != 5:
                    continue
                
                try:
                    codigo = partes[0]
                    libros[codigo] = {
                        'titulo': partes[1],
                        'autor': partes[2],
                        'ejemplares': int(partes[3]),
                        'sede': partes[4]
                    }
                except (ValueError, IndexError):
                    continue
                    
        logger.info(f"BD cargada con {len(libros)} libros")
    except FileNotFoundError:
        logger.warning(f"BD no encontrada, creando {BD_PRIMARIA}")
        with open(BD_PRIMARIA, 'w', encoding='utf-8') as f:
            pass
    return libros

def guardar_bd(libros):
    """Guarda la BD"""
    try:
        with open(BD_PRIMARIA, 'w', encoding='utf-8') as f:
            for codigo, info in libros.items():
                f.write(f"{codigo}|{info['titulo']}|{info['autor']}|{info['ejemplares']}|{info['sede']}\n")
    except Exception as e:
        logger.error(f"Error guardando BD: {e}")

def registrar_prestamo(libro_usuario):
    """Registra préstamo"""
    try:
        with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
            f.write(f"{libro_usuario.codigo}|{libro_usuario.titulo}|{libro_usuario.autor}|"
                   f"{libro_usuario.fecha_prestamo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
    except Exception as e:
        logger.error(f"Error registrando préstamo: {e}")

def procesar_devolucion(libro_usuario, libros):
    """Procesa devolución"""
    codigo = libro_usuario.codigo
    if codigo in libros:
        libros[codigo]['ejemplares'] += 1
        guardar_bd(libros)
        return {"exito": True, "mensaje": f"Devolución en {SEDE}. Ejemplares: {libros[codigo]['ejemplares']}"}
    else:
        return {"exito": False, "mensaje": "Libro no encontrado"}

def procesar_renovacion(libro_usuario, libros):
    """Procesa renovación"""
    try:
        with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
            f.write(f"RENOVACION|{libro_usuario.codigo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
        return {"exito": True, "mensaje": f"Renovación en {SEDE}"}
    except Exception as e:
        return {"exito": False, "mensaje": str(e)}

def verificar_disponibilidad(libro_usuario, libros):
    """Verifica disponibilidad"""
    codigo = libro_usuario.codigo
    if codigo in libros:
        if libros[codigo]['ejemplares'] > 0:
            return {"disponible": True, "ejemplares": libros[codigo]['ejemplares'], "sede": SEDE}
        else:
            return {"disponible": False, "mensaje": "Sin ejemplares", "sede": SEDE}
    else:
        return {"disponible": False, "mensaje": "Libro no existe", "sede": SEDE}

def procesar_prestamo(libro_usuario, libros):
    """Procesa préstamo"""
    codigo = libro_usuario.codigo
    if codigo in libros and libros[codigo]['ejemplares'] > 0:
        libros[codigo]['ejemplares'] -= 1
        guardar_bd(libros)
        registrar_prestamo(libro_usuario)
        return {"exito": True, "mensaje": f"Préstamo en {SEDE}. Ejemplares: {libros[codigo]['ejemplares']}"}
    else:
        return {"exito": False, "mensaje": "No disponible"}

def worker_thread(worker_id, libros):
    """Thread worker que procesa peticiones"""
    worker_socket = context.socket(zmq.REP)
    worker_socket.connect(f"tcp://localhost:{PUERTO_BACKEND}")
    
    logger.info(f"[WORKER-{worker_id}] Iniciado")
    
    while True:
        try:
            # Recibir petición (socket REP recibe directamente)
            mensaje = worker_socket.recv_string()
            data = json.loads(mensaje)
            
            operacion = data.get("operacion")
            libro_usuario_dict = data.get("libro_usuario", {})
            libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)

            logger.info(f"[WORKER-{worker_id}] Procesando {operacion}: {libro_usuario.codigo}")

            with bd_lock:
                if operacion == "devolucion":
                    respuesta = procesar_devolucion(libro_usuario, libros)
                elif operacion == "renovacion":
                    respuesta = procesar_renovacion(libro_usuario, libros)
                elif operacion == "verificar_disponibilidad":
                    respuesta = verificar_disponibilidad(libro_usuario, libros)
                elif operacion == "prestamo":
                    respuesta = procesar_prestamo(libro_usuario, libros)
                else:
                    respuesta = {"exito": False, "mensaje": "Operación desconocida"}

            # Enviar respuesta (socket REP envía directamente)
            worker_socket.send_string(json.dumps(respuesta))
            logger.info(f"[WORKER-{worker_id}] ✓ Procesado")

        except json.JSONDecodeError as e:
            logger.error(f"[WORKER-{worker_id}] Error JSON: {e}")
            worker_socket.send_string(json.dumps({"exito": False, "mensaje": "JSON inválido"}))
        except Exception as e:
            logger.error(f"[WORKER-{worker_id}] Error: {e}")
            try:
                worker_socket.send_string(json.dumps({"exito": False, "mensaje": str(e)}))
            except:
                pass

if __name__ == "__main__":
    logger.info(f"Gestor de Almacenamiento {SEDE} iniciado")
    
    # Cargar BD
    libros = cargar_bd()
    
    # Socket ROUTER (frontend - recibe de Actores)
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(f"tcp://*:{PUERTO_FRONTEND}")
    logger.info(f"Frontend ROUTER en tcp://*:{PUERTO_FRONTEND}")
    
    # Socket DEALER (backend - distribuye a workers)
    backend = context.socket(zmq.DEALER)
    backend.bind(f"tcp://*:{PUERTO_BACKEND}")
    logger.info(f"Backend DEALER en tcp://*:{PUERTO_BACKEND}")
    
    # Iniciar workers
    NUM_WORKERS = 4
    for i in range(NUM_WORKERS):
        thread = threading.Thread(target=worker_thread, args=(i, libros), daemon=True)
        thread.start()
    
    logger.info(f"{NUM_WORKERS} workers iniciados")
    
    # Proxy ROUTER-DEALER (conecta frontend con backend)
    try:
        logger.info("Iniciando proxy ROUTER-DEALER...")
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        logger.info("Deteniendo GA...")
    finally:
        frontend.close()
        backend.close()
        context.term()