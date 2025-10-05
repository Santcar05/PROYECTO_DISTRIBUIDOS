#!/usr/bin/env python3
"""
GestorAlmacenamientoPersistencia.py
Módulo para simular el almacenamiento persistente de operaciones
(devolución, renovación, préstamo) en un archivo JSON local.
"""

import json
import os
import time

DB_FILE = "basedatos.json"

def guardar_operacion(data: dict):
    """
    Guarda una operación (dict) en el archivo JSON de base de datos.
    Si el archivo no existe, se crea automáticamente.
    """
    # Añadir timestamp de persistencia
    data = dict(data)  # copia para no alterar referencia
    data["persisted_at"] = time.time()

    # Crear archivo si no existe
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump([], f, indent=4)

    # Leer registros actuales
    with open(DB_FILE, "r", encoding="utf-8") as f:
        try:
            registros = json.load(f)
        except json.JSONDecodeError:
            registros = []

    # Agregar nuevo registro
    registros.append(data)

    # Guardar de nuevo
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(registros, f, indent=4, ensure_ascii=False)

def leer_registros():
    """
    Retorna todos los registros almacenados en el archivo de base de datos.
    """
    if not os.path.exists(DB_FILE):
        return []
    with open(DB_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []

def limpiar_base():
    """
    Limpia todos los registros de la base de datos (archivo reiniciado).
    """
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump([], f, indent=4)
