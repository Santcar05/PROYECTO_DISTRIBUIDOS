#!/usr/bin/env python3
"""
PS.py - Proceso Solicitante
Lee peticiones desde un archivo de texto y las envía al Gestor de Carga (GC).
Cada línea del archivo genera un hilo que envía una operación (préstamo, devolución, renovación).
"""

from asyncio import sleep
import threading
import zmq
import time
import json
import sys

from clases import LibroUsuario

# Dirección del Gestor de Carga
GC_ADDRESS = "tcp://localhost:5555"  # Cambia localhost por la IP del GC en otra máquina si aplica

# Contexto y socket compartido (REQ)
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(GC_ADDRESS)

# Lock para evitar condiciones de carrera con el socket REQ
lock = threading.Lock()


def enviar_peticion(operacion, codigo, titulo, autor, sede):
    """
    Envía una petición al Gestor de Carga y espera respuesta.
    """
    with lock:
        # Construir el objeto libroUsuario
        libroUsuario = LibroUsuario(codigo, titulo, autor, sede)

        mensaje = {
            "operacion": operacion.lower(),  # prestamo | devolucion | renovacion
            "libro_usuario": libroUsuario.to_dict(),
            "timestamp": time.time()
        }

        print(f"[PS] Enviando petición {operacion.upper()} -> {codigo}, {titulo}, {autor}, {sede}")
        try:
            # Enviar al GC
            socket.send_string(json.dumps(mensaje))

            # Esperar respuesta (timeout 15s)
            if socket.poll(15000):  # 15000 ms
                respuesta = socket.recv_string()
                print(f"[PS] Respuesta del GC: {respuesta}")
            else:
                print("[PS] Timeout: No se recibió respuesta del GC")

        except Exception as e:
            print(f"[PS] Error enviando petición: {e}")


if __name__ == "__main__":
    #Nombre de archivo (por defecto peticiones.txt)
  
    if len(sys.argv) < 2:
        print("Uso: python PS.py archivo.txt")
        sys.exit(1)

    nombre_archivo = sys.argv[1]  #Primer argumento después del nombre del script
    print(f"Archivo recibido: {nombre_archivo}")

    archivo = nombre_archivo
    try:
        with open(archivo, "r", encoding="utf-8") as f:
            lineas = f.readlines()
    except FileNotFoundError:
        print(f"Archivo {archivo} no encontrado.")
        sys.exit(1)

    threads = []
    for line in lineas:

        parts = line.strip().split(",")
        if len(parts) != 5:
            print("[PS] Línea malformada:", line)
            continue

        tipo, isbn, titulo, autor, sede = parts

        if tipo.upper() == "DEVOLUCION":
            t = threading.Thread(target=enviar_peticion, args=("devolucion", isbn, titulo, autor, sede))
        elif tipo.upper() == "RENOVACION":
            t = threading.Thread(target=enviar_peticion, args=("renovacion", isbn, titulo, autor, sede))
        elif tipo.upper() == "PRESTAMO":
            t = threading.Thread(target=enviar_peticion, args=("prestamo", isbn, titulo, autor, sede))
        else:
            print("[PS] Tipo de operación desconocido:", tipo)
            continue

        threads.append(t)
        t.start()

    # Esperar a que terminen todos los hilos
    for t in threads:
        t.join()

    print("[PS] Todas las peticiones han sido procesadas.")
