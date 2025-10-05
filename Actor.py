#!/usr/bin/env python3
"""
Actor.py
Subscriber (Actor) que procesa mensajes de 'devolucion' y 'renovacion' publicados
por el Gestor de Carga (PUB/SUB).  

- Se suscribe a los tópicos especificados (por defecto: devolucion, renovacion).
- Valida JSON, evita duplicados mediante cache de request_id (idempotencia).
- Persiste cada operación llamando a guardar_operacion(...) del GestorAlmacenamientoPersistencia.py
  (si no existe, crea/appends a basedatos_actor.json).
- Uso: python Actor.py --connect tcp://<IP_GC>:5556
"""

import zmq
import json
import time
import argparse
import logging
import hashlib
from collections import OrderedDict

# intentar importar persistencia proporcionada por el equipo
try:
    from GestorAlmacenamientoPersistencia import guardar_operacion
    HAS_PERSISTENCE = True
except Exception:
    HAS_PERSISTENCE = False
    def guardar_operacion(entry):
        """Fallback simple: escribe líneas JSON en basedatos_actor.json"""
        with open("basedatos_actor.json", "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# importar modelo de dominio (opcional, se usa para logging / futura validación)
try:
    from clases import LibroUsuario
    HAS_LIBRO = True
except Exception:
    HAS_LIBRO = False
    class Dummy: pass
    LibroUsuario = Dummy  # no se usará si no está disponible


def gen_request_id(data: dict) -> str:
    """Genera un request_id determinístico por contenido + tiempo breve."""
    payload = json.dumps(data, sort_keys=True, ensure_ascii=False)
    h = hashlib.sha256()
    h.update(payload.encode("utf-8"))
    # añadir timestamp corto para reducir colisiones si se reenvía el mismo payload
    h.update(str(time.time()).encode("utf-8"))
    return h.hexdigest()


def parse_args():
    p = argparse.ArgumentParser(description="Actor SUB para devolucion/renovacion")
    p.add_argument("--connect", "-c", default="tcp://localhost:5556",
                   help="Dirección PUB del GestorCarga (ej: tcp://10.43.102.40:5556)")
    p.add_argument("--topics", "-t", nargs="+", default=["devolucion", "renovacion", "prestamo"],
               help="Tópicos a suscribirse (por defecto: devolucion renovacion prestamo)")
    p.add_argument("--cache", type=int, default=10000,
                   help="Tamaño de cache para ids procesados (idempotencia)")
    p.add_argument("--allow-raw", action="store_true",
                   help="Aceptar mensajes sin topic (útil para debug)")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    logging.info("Conectando al PUB del GestorCarga en %s", args.connect)
    sub.connect(args.connect)

    # subscribir a cada tópico
    for t in args.topics:
        sub.setsockopt_string(zmq.SUBSCRIBE, t)
        logging.info("Suscrito al tópico: %s", t)

    # si quiere recibir mensajes sin topic
    if args.allow_raw:
        sub.setsockopt_string(zmq.SUBSCRIBE, "")

    # pequeño sleep para asegurar que la suscripción se propague
    time.sleep(0.2)

    processed = OrderedDict()  # request_id -> timestamp (mantiene orden)
    MAX_CACHE = args.cache

    logging.info("Actor listo. Esperando mensajes... (ctrl+c para salir)")

    try:
        while True:
            try:
                raw = sub.recv_string()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.error("Error recibiendo mensaje ZMQ: %s", e)
                continue

            # Mensaje esperado: "topic <json_payload>"
            if " " in raw:
                topic, payload = raw.split(" ", 1)
            else:
                # si llegó todo el JSON (sin topic) y allow_raw=True, payload == raw
                topic = None
                payload = raw

            try:
                data = json.loads(payload)
            except json.JSONDecodeError as e:
                logging.error("Payload no es JSON válido: %s -- raw: %s", e, payload[:200])
                continue

            # normalizar operación y obtener request_id (si viene)
            request_id = data.get("request_id") or data.get("libro_usuario", {}).get("request_id")
            if not request_id:
                request_id = gen_request_id(data)

            # idempotencia: ignorar si ya procesado
            if request_id in processed:
                logging.info("Mensaje duplicado detectado (id=%s) - ignorando", request_id)
                # mantener orden: actualizar timestamp al reingreso opcionalmente
                processed.move_to_end(request_id)
                continue

            operacion = data.get("operacion") or data.get("tipo") or topic or "unknown"
            libro_dict = data.get("libro_usuario") or data.get("libro") or data.get("payload") or {}

            titulo = None
            try:
                if libro_dict and isinstance(libro_dict, dict):
                    titulo = libro_dict.get("titulo") or libro_dict.get("title")
                    # opcional: crear objeto domain
                    if HAS_LIBRO:
                        try:
                            libro_usuario = LibroUsuario.from_dict(libro_dict)
                            titulo = getattr(libro_usuario, "titulo", titulo)
                        except Exception:
                            pass
            except Exception:
                pass

            logging.info("Procesando operacion=%s id=%s titulo=%s", operacion, request_id, titulo)

            entry = {
                "request_id": request_id,
                "operacion": operacion,
                "topic": topic,
                "payload": data,
                "processed_at": time.time()
            }

            # persistir (llama la función del equipo si existe; si no, usa fallback)
            try:
                guardar_operacion(entry)
                logging.info("Operación persistida (id=%s)", request_id)
            except Exception as e:
                logging.error("Error al persistir la operación: %s", e)

            # marcar como procesado en cache (y podar si se excede tamaño)
            processed[request_id] = time.time()
            if len(processed) > MAX_CACHE:
                # eliminar el más antiguo
                processed.popitem(last=False)

    except KeyboardInterrupt:
        logging.info("Interrupción por usuario - cerrando Actor.")
    finally:
        try:
            sub.close(linger=0)
            ctx.term()
        except Exception:
            pass


if __name__ == "__main__":
    main()
