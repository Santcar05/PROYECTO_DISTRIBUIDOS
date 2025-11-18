#!/usr/bin/env python3
"""
PS_Medicion.py
Proceso Solicitante con MEDICIÓN DE TIEMPOS para experimentos.
"""

import threading
import zmq
import time
import json
import sys
import csv
from datetime import datetime
from clases import LibroUsuario

GC_ADDRESS = "tcp://localhost:5555"

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(GC_ADDRESS)

lock = threading.Lock()

# Archivo para guardar tiempos
ARCHIVO_TIEMPOS = "resultados_tiempos.csv"

# Contadores y métricas
metricas = {
    'total_peticiones': 0,
    'peticiones_exitosas': 0,
    'peticiones_fallidas': 0,
    'tiempos_respuesta': [],
    'inicio_experimento': None,
    'fin_experimento': None
}

metricas_lock = threading.Lock()

def inicializar_archivo_csv():
    """Inicializa el archivo CSV con encabezados"""
    try:
        with open(ARCHIVO_TIEMPOS, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'operacion', 'codigo', 'titulo', 'sede', 'tiempo_respuesta_ms', 'exito'])
        print(f"[PS] Archivo {ARCHIVO_TIEMPOS} inicializado")
    except Exception as e:
        print(f"[PS] Error inicializando CSV: {e}")

def guardar_metrica(operacion, codigo, titulo, sede, tiempo_respuesta, exito):
    """Guarda una métrica en el archivo CSV"""
    try:
        with open(ARCHIVO_TIEMPOS, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            timestamp = datetime.now().isoformat()
            tiempo_ms = tiempo_respuesta * 1000  # Convertir a milisegundos
            writer.writerow([timestamp, operacion, codigo, titulo, sede, f"{tiempo_ms:.2f}", exito])
    except Exception as e:
        print(f"[PS] Error guardando métrica: {e}")

def enviar_peticion(operacion, codigo, titulo, autor, sede):
    """Envía una petición y mide el tiempo de respuesta"""
    with lock:
        libroUsuario = LibroUsuario(codigo, titulo, autor)

        mensaje = {
            "operacion": operacion.lower(),
            "libro_usuario": libroUsuario.to_dict(),
            "timestamp": time.time()
        }

        print(f"[PS] Enviando {operacion.upper()} -> {codigo} ({sede})")
        
        # INICIO DE MEDICIÓN
        tiempo_inicio = time.time()
        exito = False
        
        try:
            socket.send_string(json.dumps(mensaje))

            if socket.poll(15000):  # Timeout 15 segundos
                respuesta = socket.recv_string()
                
                # FIN DE MEDICIÓN
                tiempo_respuesta = time.time() - tiempo_inicio
                
                print(f"[PS] ✓ Respuesta: {respuesta} (Tiempo: {tiempo_respuesta*1000:.2f}ms)")
                exito = True
                
                # Actualizar métricas
                with metricas_lock:
                    metricas['total_peticiones'] += 1
                    metricas['peticiones_exitosas'] += 1
                    metricas['tiempos_respuesta'].append(tiempo_respuesta)
                
                # Guardar en CSV
                guardar_metrica(operacion, codigo, titulo, sede, tiempo_respuesta, True)
                
            else:
                tiempo_respuesta = time.time() - tiempo_inicio
                print(f"[PS] ✗ Timeout ({tiempo_respuesta:.2f}s)")
                
                with metricas_lock:
                    metricas['total_peticiones'] += 1
                    metricas['peticiones_fallidas'] += 1
                
                guardar_metrica(operacion, codigo, titulo, sede, tiempo_respuesta, False)

        except Exception as e:
            tiempo_respuesta = time.time() - tiempo_inicio
            print(f"[PS] ✗ Error: {e} ({tiempo_respuesta:.2f}s)")
            
            with metricas_lock:
                metricas['total_peticiones'] += 1
                metricas['peticiones_fallidas'] += 1
            
            guardar_metrica(operacion, codigo, titulo, sede, tiempo_respuesta, False)

def calcular_estadisticas():
    """Calcula y muestra estadísticas finales"""
    with metricas_lock:
        print("\n" + "=" * 60)
        print("ESTADÍSTICAS DEL EXPERIMENTO")
        print("=" * 60)
        
        total = metricas['total_peticiones']
        exitosas = metricas['peticiones_exitosas']
        fallidas = metricas['peticiones_fallidas']
        tiempos = metricas['tiempos_respuesta']
        
        print(f"Total de peticiones: {total}")
        print(f"Peticiones exitosas: {exitosas}")
        print(f"Peticiones fallidas: {fallidas}")
        
        if tiempos:
            promedio = sum(tiempos) / len(tiempos)
            promedio_ms = promedio * 1000
            
            # Calcular desviación estándar
            varianza = sum((t - promedio) ** 2 for t in tiempos) / len(tiempos)
            desv_std = varianza ** 0.5
            desv_std_ms = desv_std * 1000
            
            # Calcular min y max
            min_tiempo = min(tiempos) * 1000
            max_tiempo = max(tiempos) * 1000
            
            print(f"\nTIEMPOS DE RESPUESTA:")
            print(f"  - Promedio: {promedio_ms:.2f} ms")
            print(f"  - Desv. Estándar: {desv_std_ms:.2f} ms")
            print(f"  - Mínimo: {min_tiempo:.2f} ms")
            print(f"  - Máximo: {max_tiempo:.2f} ms")
        
        if metricas['inicio_experimento'] and metricas['fin_experimento']:
            duracion_total = metricas['fin_experimento'] - metricas['inicio_experimento']
            print(f"\nDuración total del experimento: {duracion_total:.2f} segundos")
            
            if duracion_total > 0:
                throughput = total / duracion_total
                print(f"Throughput: {throughput:.2f} peticiones/segundo")
        
        print("=" * 60)
        print(f"Resultados guardados en: {ARCHIVO_TIEMPOS}")
        print("=" * 60 + "\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python PS_Medicion.py archivo.txt")
        sys.exit(1)

    nombre_archivo = sys.argv[1]
    print(f"[PS] Archivo de peticiones: {nombre_archivo}")
    
    # Inicializar archivo CSV
    inicializar_archivo_csv()

    try:
        with open(nombre_archivo, "r", encoding="utf-8") as f:
            lineas = f.readlines()
    except FileNotFoundError:
        print(f"[PS] Archivo {nombre_archivo} no encontrado")
        sys.exit(1)

    print(f"[PS] Total de peticiones a procesar: {len(lineas)}")
    print("[PS] Iniciando experimento...\n")
    
    # Marcar inicio del experimento
    metricas['inicio_experimento'] = time.time()
    
    threads = []
    for line in lineas:
        parts = line.strip().split(",")
        if len(parts) != 5:
            print(f"[PS] Línea malformada: {line}")
            continue

        tipo, isbn, titulo, autor, sede = parts

        if tipo.upper() == "DEVOLUCION":
            t = threading.Thread(target=enviar_peticion, args=("devolucion", isbn, titulo, autor, sede))
        elif tipo.upper() == "RENOVACION":
            t = threading.Thread(target=enviar_peticion, args=("renovacion", isbn, titulo, autor, sede))
        elif tipo.upper() == "PRESTAMO":
            t = threading.Thread(target=enviar_peticion, args=("prestamo", isbn, titulo, autor, sede))
        else:
            print(f"[PS] Tipo desconocido: {tipo}")
            continue

        threads.append(t)
        t.start()

    print("[PS] Esperando a que finalicen todas las peticiones...\n")
    
    # Esperar a que terminen todos los hilos
    for t in threads:
        t.join()

    # Marcar fin del experimento
    metricas['fin_experimento'] = time.time()
    
    print("\n[PS] Todas las peticiones procesadas")
    
    # Calcular y mostrar estadísticas
    calcular_estadisticas()