#!/usr/bin/env python3
"""
analizar_resultados.py
Script para analizar resultados y generar gráficos.
"""

import csv
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

def leer_resultados(archivo):
    """Lee el archivo CSV con los resultados"""
    datos = []
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row['tiempo_respuesta_ms'] = float(row['tiempo_respuesta_ms'])
                    row['exito'] = row['exito'] == 'True'
                    datos.append(row)
                except:
                    continue
        return datos
    except FileNotFoundError:
        print(f"Archivo {archivo} no encontrado")
        return []

def calcular_estadisticas(datos):
    """Calcula estadísticas de los datos"""
    tiempos = [d['tiempo_respuesta_ms'] for d in datos if d['exito']]
    
    if not tiempos:
        print("No hay datos para analizar")
        return None
    
    stats = {
        'total': len(datos),
        'exitosas': sum(1 for d in datos if d['exito']),
        'fallidas': sum(1 for d in datos if not d['exito']),
        'promedio': np.mean(tiempos),
        'desv_std': np.std(tiempos),
        'min': np.min(tiempos),
        'max': np.max(tiempos),
        'mediana': np.median(tiempos),
        'percentil_95': np.percentile(tiempos, 95),
        'percentil_99': np.percentile(tiempos, 99)
    }
    
    return stats

def generar_graficos(datos, nombre_experimento="Experimento"):
    """Genera gráficos de los resultados"""
    tiempos = [d['tiempo_respuesta_ms'] for d in datos if d['exito']]
    
    if not tiempos:
        print("No hay datos para graficar")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Análisis de Resultados - {nombre_experimento}', fontsize=16)
    
    # Gráfico 1: Histograma de tiempos de respuesta
    axes[0, 0].hist(tiempos, bins=50, color='skyblue', edgecolor='black')
    axes[0, 0].set_xlabel('Tiempo de Respuesta (ms)')
    axes[0, 0].set_ylabel('Frecuencia')
    axes[0, 0].set_title('Distribución de Tiempos de Respuesta')
    axes[0, 0].axvline(np.mean(tiempos), color='red', linestyle='dashed', linewidth=2, label=f'Promedio: {np.mean(tiempos):.2f}ms')
    axes[0, 0].legend()
    
    # Gráfico 2: Tiempos a lo largo del tiempo
    axes[0, 1].plot(tiempos, marker='o', linestyle='-', markersize=2, alpha=0.6)
    axes[0, 1].set_xlabel('Número de Petición')
    axes[0, 1].set_ylabel('Tiempo de Respuesta (ms)')
    axes[0, 1].set_title('Tiempos de Respuesta a lo Largo del Tiempo')
    axes[0, 1].axhline(np.mean(tiempos), color='red', linestyle='dashed', linewidth=2)
    
    # Gráfico 3: Box plot
    axes[1, 0].boxplot(tiempos, vert=True, patch_artist=True,
                       boxprops=dict(facecolor='lightblue'),
                       medianprops=dict(color='red', linewidth=2))
    axes[1, 0].set_ylabel('Tiempo de Respuesta (ms)')
    axes[1, 0].set_title('Box Plot de Tiempos de Respuesta')
    axes[1, 0].grid(True, alpha=0.3)
    
    # Gráfico 4: Estadísticas por operación
    operaciones = {}
    for d in datos:
        if d['exito']:
            op = d['operacion']
            if op not in operaciones:
                operaciones[op] = []
            operaciones[op].append(d['tiempo_respuesta_ms'])
    
    if operaciones:
        ops = list(operaciones.keys())
        promedios = [np.mean(operaciones[op]) for op in ops]
        
        axes[1, 1].bar(ops, promedios, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
        axes[1, 1].set_xlabel('Tipo de Operación')
        axes[1, 1].set_ylabel('Tiempo Promedio (ms)')
        axes[1, 1].set_title('Tiempo Promedio por Tipo de Operación')
        axes[1, 1].grid(True, alpha=0.3, axis='y')
        
        # Añadir valores encima de las barras
        for i, (op, prom) in enumerate(zip(ops, promedios)):
            axes[1, 1].text(i, prom, f'{prom:.2f}', ha='center', va='bottom')
    
    plt.tight_layout()
    nombre_archivo = f"grafico_{nombre_experimento.replace(' ', '_')}.png"
    plt.savefig(nombre_archivo, dpi=300)
    print(f"Gráfico guardado en: {nombre_archivo}")
    plt.show()

def comparar_experimentos(archivo1, archivo2, nombre1="Serial", nombre2="Multihilo"):
    """Compara dos experimentos"""
    datos1 = leer_resultados(archivo1)
    datos2 = leer_resultados(archivo2)
    
    stats1 = calcular_estadisticas(datos1)
    stats2 = calcular_estadisticas(datos2)
    
    if not stats1 or not stats2:
        print("Error: No se pudieron cargar los datos de ambos experimentos")
        return
    
    print("\n" + "=" * 60)
    print("COMPARACIÓN DE EXPERIMENTOS")
    print("=" * 60)
    
    print(f"\n{nombre1}:")
    print(f"  Promedio: {stats1['promedio']:.2f} ms")
    print(f"  Desv. Std: {stats1['desv_std']:.2f} ms")
    print(f"  Peticiones exitosas: {stats1['exitosas']}/{stats1['total']}")
    
    print(f"\n{nombre2}:")
    print(f"  Promedio: {stats2['promedio']:.2f} ms")
    print(f"  Desv. Std: {stats2['desv_std']:.2f} ms")
    print(f"  Peticiones exitosas: {stats2['exitosas']}/{stats2['total']}")
    
    mejora = ((stats1['promedio'] - stats2['promedio']) / stats1['promedio']) * 100
    print(f"\nMejora en tiempo promedio: {mejora:.2f}%")
    
    # Gráfico comparativo
    fig, ax = plt.subplots(figsize=(10, 6))
    
    categorias = ['Promedio', 'Desv. Std', 'Mediana', 'P95', 'P99']
    valores1 = [stats1['promedio'], stats1['desv_std'], stats1['mediana'], 
                stats1['percentil_95'], stats1['percentil_99']]
    valores2 = [stats2['promedio'], stats2['desv_std'], stats2['mediana'], 
                stats2['percentil_95'], stats2['percentil_99']]
    
    x = np.arange(len(categorias))
    width = 0.35
    
    ax.bar(x - width/2, valores1, width, label=nombre1, color='#FF6B6B')
    ax.bar(x + width/2, valores2, width, label=nombre2, color='#4ECDC4')
    
    ax.set_xlabel('Métrica')
    ax.set_ylabel('Tiempo (ms)')
    ax.set_title('Comparación de Experimentos')
    ax.set_xticks(x)
    ax.set_xticklabels(categorias)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig("comparacion_experimentos.png", dpi=300)
    print("\nGráfico comparativo guardado en: comparacion_experimentos.png")
    plt.show()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso:")
        print("  Analizar un experimento: python analizar_resultados.py archivo.csv")
        print("  Comparar dos experimentos: python analizar_resultados.py archivo1.csv archivo2.csv")
        sys.exit(1)
    
    archivo1 = sys.argv[1]
    
    if len(sys.argv) == 2:
        # Analizar un solo experimento
        print(f"Analizando: {archivo1}")
        datos = leer_resultados(archivo1)
        
        if datos:
            stats = calcular_estadisticas(datos)
            if stats:
                print("\nESTADÍSTICAS:")
                print(f"  Total de peticiones: {stats['total']}")
                print(f"  Exitosas: {stats['exitosas']}")
                print(f"  Fallidas: {stats['fallidas']}")
                print(f"  Promedio: {stats['promedio']:.2f} ms")
                print(f"  Desv. Std: {stats['desv_std']:.2f} ms")
                print(f"  Mínimo: {stats['min']:.2f} ms")
                print(f"  Máximo: {stats['max']:.2f} ms")
                print(f"  Mediana: {stats['mediana']:.2f} ms")
                print(f"  Percentil 95: {stats['percentil_95']:.2f} ms")
                print(f"  Percentil 99: {stats['percentil_99']:.2f} ms")
                
                generar_graficos(datos, "Experimento")
    
    elif len(sys.argv) == 3:
        # Comparar dos experimentos
        archivo2 = sys.argv[2]
        print(f"Comparando:")
        print(f"  Experimento 1: {archivo1}")
        print(f"  Experimento 2: {archivo2}")
        
        comparar_experimentos(archivo1, archivo2)