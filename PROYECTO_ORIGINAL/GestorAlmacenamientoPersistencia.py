#!/usr/bin/env python3
"""
GestorAlmacenamiento.py
Gestor que maneja operaciones sobre la BD con replicación y tolerancia a fallos.
Cada sede tiene su propio GA que sincroniza con la otra sede.
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
from clases import LibroBiblioteca, LibroUsuario

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] GA-%(sede)s: %(message)s")

# Configuración por sede
if len(sys.argv) < 2:
    print("Uso: python GestorAlmacenamiento.py <SedeA|SedeB>")
    sys.exit(1)

SEDE = sys.argv[1]  # "SedeA" o "SedeB"

# Configuración de puertos y rutas según sede
if SEDE == "SedeA":
    PUERTO_REP = 5557
    PUERTO_HEARTBEAT = 5558
    #IP_SEDE_REMOTA = "10.43.102.41"  # IP de SedeB
    IP_SEDE_REMOTA = "localhost"
    PUERTO_REP_REMOTO = 5559
    PUERTO_HEARTBEAT_REMOTO = 5560
    BD_PRIMARIA = "BD_SedeA.txt"
    BD_REPLICA = "BD_Replica_SedeA.txt"
    BD_PRESTAMOS = "BD_Prestamos_SedeA.txt"
elif SEDE == "SedeB":
    PUERTO_REP = 5559
    PUERTO_HEARTBEAT = 5560
    #IP_SEDE_REMOTA = "10.43.102.40"  # IP de SedeA
    IP_SEDE_REMOTA = "localhost"
    PUERTO_REP_REMOTO = 5557
    PUERTO_HEARTBEAT_REMOTO = 5558
    BD_PRIMARIA = "BD_SedeB.txt"
    BD_REPLICA = "BD_Replica_SedeB.txt"
    BD_PRESTAMOS = "BD_Prestamos_SedeB.txt"
else:
    print("Sede debe ser 'SedeA' o 'SedeB'")
    sys.exit(1)

logger = logging.LoggerAdapter(logging.getLogger(), {'sede': SEDE})

# Estado del sistema
estado = {
    'activo': True,
    'sede_remota_activa': True,
    'ultima_respuesta_remota': time.time(),
    'modo_activo': True  # True = primario, False = respaldo
}

bd_lock = threading.Lock()
context = zmq.Context()

# Socket REP para recibir peticiones de los Actores
rep_socket = context.socket(zmq.REP)
rep_socket.bind(f"tcp://*:{PUERTO_REP}")
logger.info(f"Socket REP escuchando en tcp://*:{PUERTO_REP}")

# Socket PUB para enviar heartbeats
heartbeat_pub = context.socket(zmq.PUB)
heartbeat_pub.bind(f"tcp://*:{PUERTO_HEARTBEAT}")
logger.info(f"Heartbeat PUB en tcp://*:{PUERTO_HEARTBEAT}")

# Socket SUB para recibir heartbeats de la sede remota
heartbeat_sub = context.socket(zmq.SUB)
heartbeat_sub.connect(f"tcp://{IP_SEDE_REMOTA}:{PUERTO_HEARTBEAT_REMOTO}")
heartbeat_sub.setsockopt_string(zmq.SUBSCRIBE, "")
logger.info(f"Escuchando heartbeats de {IP_SEDE_REMOTA}:{PUERTO_HEARTBEAT_REMOTO}")

# Socket REQ para replicación hacia la otra sede
rep_replicacion = context.socket(zmq.REQ)
rep_replicacion.connect(f"tcp://{IP_SEDE_REMOTA}:{PUERTO_REP_REMOTO}")
rep_replicacion.setsockopt(zmq.RCVTIMEO, 3000)  # Timeout 3 segundos
logger.info(f"Socket replicación hacia tcp://{IP_SEDE_REMOTA}:{PUERTO_REP_REMOTO}")

def cargar_bd():
    """Carga la BD en memoria como un diccionario"""
    libros = {}
    archivo = BD_PRIMARIA if os.path.exists(BD_PRIMARIA) else BD_REPLICA
    
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
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
        logger.info(f"BD cargada desde {archivo} con {len(libros)} libros")
    except FileNotFoundError:
        logger.warning(f"BD no encontrada, creando nueva en {BD_PRIMARIA}")
        # Crear BD inicial vacía
        with open(BD_PRIMARIA, 'w', encoding='utf-8') as f:
            pass
    return libros

def guardar_bd(libros):
    """Guarda el diccionario en la BD primaria"""
    try:
        with open(BD_PRIMARIA, 'w', encoding='utf-8') as f:
            for codigo, info in libros.items():
                f.write(f"{codigo}|{info['titulo']}|{info['autor']}|{info['ejemplares']}|{info['sede']}\n")
        logger.info(f"BD guardada en {BD_PRIMARIA}")
    except Exception as e:
        logger.error(f"Error guardando BD: {e}")

def replicar_a_sede_remota(operacion_data):
    """
    Replica la operación a la sede remota de forma asíncrona.
    Si falla, guarda en cola para reintentar.
    """
    def replicar():
        try:
            mensaje_replicacion = {
                "tipo": "replicacion",
                "operacion": operacion_data
            }
            
            rep_replicacion.send_string(json.dumps(mensaje_replicacion))
            
            try:
                respuesta = rep_replicacion.recv_string()
                logger.info(f"Replicación exitosa a sede remota: {respuesta}")
                estado['sede_remota_activa'] = True
            except zmq.Again:
                logger.warning("Timeout en replicación - sede remota no responde")
                estado['sede_remota_activa'] = False
                guardar_operacion_pendiente(operacion_data)
                
        except Exception as e:
            logger.error(f"Error replicando a sede remota: {e}")
            estado['sede_remota_activa'] = False
            guardar_operacion_pendiente(operacion_data)
    
    # Ejecutar en hilo separado para no bloquear
    thread = threading.Thread(target=replicar)
    thread.daemon = True
    thread.start()

def guardar_operacion_pendiente(operacion_data):
    """Guarda operaciones pendientes cuando la réplica no está disponible"""
    try:
        with open(f"operaciones_pendientes_{SEDE}.log", 'a', encoding='utf-8') as f:
            f.write(json.dumps(operacion_data) + "\n")
        logger.info("Operación guardada para replicación posterior")
    except Exception as e:
        logger.error(f"Error guardando operación pendiente: {e}")

def sincronizar_operaciones_pendientes():
    """Intenta sincronizar operaciones pendientes cuando la réplica vuelve"""
    archivo_pendientes = f"operaciones_pendientes_{SEDE}.log"
    
    if not os.path.exists(archivo_pendientes):
        return
    
    try:
        with open(archivo_pendientes, 'r', encoding='utf-8') as f:
            operaciones = [json.loads(line.strip()) for line in f if line.strip()]
        
        if operaciones:
            logger.info(f"Sincronizando {len(operaciones)} operaciones pendientes")
            
            for op in operaciones:
                replicar_a_sede_remota(op)
            
            # Limpiar archivo de pendientes
            os.remove(archivo_pendientes)
            logger.info("Operaciones pendientes sincronizadas")
            
    except Exception as e:
        logger.error(f"Error sincronizando operaciones pendientes: {e}")

def crear_backup_local():
    """Crea backup local de la BD primaria"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{BD_PRIMARIA}.backup_{timestamp}"
        shutil.copy2(BD_PRIMARIA, backup_file)
        logger.info(f"Backup creado: {backup_file}")
    except Exception as e:
        logger.error(f"Error creando backup: {e}")

def registrar_prestamo(libro_usuario):
    """Registra un préstamo en BD_prestamos.txt"""
    try:
        with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
            f.write(f"{libro_usuario.codigo}|{libro_usuario.titulo}|{libro_usuario.autor}|"
                   f"{libro_usuario.fecha_prestamo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
    except Exception as e:
        logger.error(f"Error registrando préstamo: {e}")

def procesar_devolucion(libro_usuario, libros):
    """Incrementa ejemplares disponibles"""
    codigo = libro_usuario.codigo
    if codigo in libros:
        libros[codigo]['ejemplares'] += 1
        guardar_bd(libros)
        
        # Replicar a sede remota
        operacion_data = {
            "operacion": "devolucion",
            "libro_usuario": libro_usuario.to_dict(),
            "timestamp": time.time()
        }
        replicar_a_sede_remota(operacion_data)
        
        return {"exito": True, "mensaje": f"Devolución registrada en {SEDE}. Ejemplares: {libros[codigo]['ejemplares']}"}
    else:
        return {"exito": False, "mensaje": "Libro no encontrado en BD"}

def procesar_renovacion(libro_usuario, libros):
    """Actualiza fecha de devolución"""
    try:
        with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
            f.write(f"RENOVACION|{libro_usuario.codigo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
        
        # Replicar a sede remota
        operacion_data = {
            "operacion": "renovacion",
            "libro_usuario": libro_usuario.to_dict(),
            "timestamp": time.time()
        }
        replicar_a_sede_remota(operacion_data)
        
        return {"exito": True, "mensaje": f"Renovación registrada en {SEDE} hasta {libro_usuario.fecha_devolucion}"}
    except Exception as e:
        return {"exito": False, "mensaje": str(e)}

def verificar_disponibilidad(libro_usuario, libros):
    """Verifica si hay ejemplares disponibles"""
    codigo = libro_usuario.codigo
    if codigo in libros:
        if libros[codigo]['ejemplares'] > 0:
            return {"disponible": True, "ejemplares": libros[codigo]['ejemplares'], "sede": SEDE}
        else:
            return {"disponible": False, "mensaje": "No hay ejemplares disponibles", "sede": SEDE}
    else:
        return {"disponible": False, "mensaje": "Libro no existe en BD", "sede": SEDE}

def procesar_prestamo(libro_usuario, libros):
    """Decrementa ejemplares y registra préstamo"""
    codigo = libro_usuario.codigo
    if codigo in libros and libros[codigo]['ejemplares'] > 0:
        libros[codigo]['ejemplares'] -= 1
        guardar_bd(libros)
        registrar_prestamo(libro_usuario)
        
        # Replicar a sede remota
        operacion_data = {
            "operacion": "prestamo",
            "libro_usuario": libro_usuario.to_dict(),
            "timestamp": time.time()
        }
        replicar_a_sede_remota(operacion_data)
        
        return {"exito": True, "mensaje": f"Préstamo registrado en {SEDE}. Ejemplares restantes: {libros[codigo]['ejemplares']}"}
    else:
        return {"exito": False, "mensaje": "No se pudo realizar el préstamo"}

def procesar_replicacion(data, libros):
    """Procesa una operación de replicación recibida de la otra sede"""
    try:
        operacion = data.get("operacion")
        libro_usuario_dict = data.get("libro_usuario", {})
        libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)
        
        logger.info(f"Aplicando replicación: {operacion} para libro {libro_usuario.codigo}")
        
        if operacion == "devolucion":
            codigo = libro_usuario.codigo
            if codigo in libros:
                libros[codigo]['ejemplares'] += 1
                guardar_bd(libros)
                
        elif operacion == "renovacion":
            with open(BD_PRESTAMOS, 'a', encoding='utf-8') as f:
                f.write(f"RENOVACION_REPLICA|{libro_usuario.codigo}|{libro_usuario.fecha_devolucion}|{datetime.now().isoformat()}\n")
                
        elif operacion == "prestamo":
            codigo = libro_usuario.codigo
            if codigo in libros:
                libros[codigo]['ejemplares'] = max(0, libros[codigo]['ejemplares'] - 1)
                guardar_bd(libros)
                registrar_prestamo(libro_usuario)
        
        return {"exito": True, "mensaje": f"Replicación aplicada en {SEDE}"}
        
    except Exception as e:
        logger.error(f"Error procesando replicación: {e}")
        return {"exito": False, "mensaje": str(e)}

def thread_heartbeat():
    """Envía heartbeats periódicos"""
    while estado['activo']:
        try:
            heartbeat_data = {
                "sede": SEDE,
                "timestamp": time.time(),
                "estado": "activo"
            }
            heartbeat_pub.send_string(json.dumps(heartbeat_data))
            time.sleep(2)  # Heartbeat cada 2 segundos
        except Exception as e:
            logger.error(f"Error enviando heartbeat: {e}")

def thread_monitor_heartbeat():
    """Monitorea heartbeats de la sede remota"""
    TIMEOUT_HEARTBEAT = 10  # 10 segundos sin heartbeat = sede caída
    
    poller = zmq.Poller()
    poller.register(heartbeat_sub, zmq.POLLIN)
    
    while estado['activo']:
        try:
            socks = dict(poller.poll(2000))  # Poll cada 2 segundos
            
            if heartbeat_sub in socks:
                mensaje = heartbeat_sub.recv_string()
                data = json.loads(mensaje)
                
                estado['ultima_respuesta_remota'] = time.time()
                
                if not estado['sede_remota_activa']:
                    logger.info(f"Sede remota {data['sede']} recuperada")
                    estado['sede_remota_activa'] = True
                    # Sincronizar operaciones pendientes
                    sincronizar_operaciones_pendientes()
            
            # Verificar timeout
            tiempo_sin_respuesta = time.time() - estado['ultima_respuesta_remota']
            
            if tiempo_sin_respuesta > TIMEOUT_HEARTBEAT:
                if estado['sede_remota_activa']:
                    logger.warning(f"Sede remota no responde hace {tiempo_sin_respuesta:.1f}s - ASUMIENDO CONTROL TOTAL")
                    estado['sede_remota_activa'] = False
                    estado['modo_activo'] = True
                    crear_backup_local()
            
        except Exception as e:
            logger.error(f"Error monitoreando heartbeat: {e}")

if __name__ == "__main__":
    logger.info(f"Gestor de Almacenamiento {SEDE} iniciado")
    
    # Cargar BD al inicio
    libros = cargar_bd()
    logger.info(f"BD cargada con {len(libros)} libros")
    
    # Iniciar threads de heartbeat
    threading.Thread(target=thread_heartbeat, daemon=True).start()
    threading.Thread(target=thread_monitor_heartbeat, daemon=True).start()
    
    logger.info("Sistema de heartbeat y monitoreo iniciado")

    while estado['activo']:
        try:
            mensaje = rep_socket.recv_string()
            data = json.loads(mensaje)
            
            tipo_mensaje = data.get("tipo", "operacion")
            
            if tipo_mensaje == "replicacion":
                # Es una replicación de la otra sede
                operacion_data = data.get("operacion", {})
                
                with bd_lock:
                    respuesta = procesar_replicacion(operacion_data, libros)
                
                rep_socket.send_string(json.dumps(respuesta))
                logger.info(f"Replicación procesada: {respuesta}")
                
            else:
                # Es una operación normal de un Actor
                operacion = data.get("operacion")
                libro_usuario_dict = data.get("libro_usuario", {})
                libro_usuario = LibroUsuario.from_dict(libro_usuario_dict)

                logger.info(f"Operación recibida: {operacion} para libro {libro_usuario.codigo}")

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

                rep_socket.send_string(json.dumps(respuesta))
                logger.info(f"Respuesta enviada: {respuesta}")

        except json.JSONDecodeError as e:
            logger.error(f"Error parseando JSON: {e}")
            rep_socket.send_string(json.dumps({"exito": False, "mensaje": "JSON inválido"}))
        except Exception as e:
            logger.error(f"Error procesando petición: {e}")
            rep_socket.send_string(json.dumps({"exito": False, "mensaje": str(e)}))