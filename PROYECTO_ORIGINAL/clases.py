import zmq
import threading
import json
import time
from datetime import datetime, timedelta
class LibroBiblioteca:
    def __init__(self, codigo, titulo="", autor="", ejemplares_disponibles=0, sede=""):
        self.codigo = codigo
        self.titulo = titulo
        self.autor = autor
        self.ejemplares_disponibles = ejemplares_disponibles
        self.sede = sede
    
    def to_dict(self):
        return {
            'codigo': self.codigo,
            'titulo': self.titulo,
            'autor': self.autor,
            'ejemplares_disponibles': self.ejemplares_disponibles,
            'sede': self.sede
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(
            codigo=data['codigo'],
            titulo=data.get('titulo', ''),
            autor=data.get('autor', ''),
            ejemplares_disponibles=data.get('ejemplares_disponibles', 0),
            sede=data.get('sede', '')
        )

class LibroUsuario:
    def __init__(self, codigo, titulo="", autor="", fecha_prestamo=None, fecha_devolucion=None):
        self.codigo = codigo
        self.titulo = titulo
        self.autor = autor
        self.fecha_prestamo = fecha_prestamo or datetime.now().isoformat()
        self.fecha_devolucion = fecha_devolucion or (datetime.now() + timedelta(weeks=2)).isoformat()
    
    def to_dict(self):
        return {
            'codigo': self.codigo,
            'titulo': self.titulo,
            'autor': self.autor,
            'fecha_prestamo': self.fecha_prestamo,
            'fecha_devolucion': self.fecha_devolucion
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(
            codigo=data['codigo'],
            titulo=data.get('titulo', ''),
            autor=data.get('autor', ''),
            fecha_prestamo=data.get('fecha_prestamo'),
            fecha_devolucion=data.get('fecha_devolucion')
        )


class Mensaje:
    def __init__(self, tipo, libro, usuario, sede, datos_adicionales=None):
        self.tipo = tipo  # "PRESTAMO", "DEVOLUCION", "RENOVACION"
        self.libro = libro
        self.usuario = usuario
        self.sede = sede
        self.timestamp = datetime.now().isoformat()
        self.datos_adicionales = datos_adicionales or {}
    
    def to_dict(self):
        return {
            'tipo': self.tipo,
            'libro': self.libro.to_dict(),
            'usuario': self.usuario,
            'sede': self.sede,
            'timestamp': self.timestamp,
            'datos_adicionales': self.datos_adicionales
        }
    
    @classmethod
    def from_dict(cls, data):
        libro = LibroBiblioteca.from_dict(data['libro'])
        return cls(
            tipo=data['tipo'],
            libro=libro,
            usuario=data['usuario'],
            sede=data['sede'],
            datos_adicionales=data.get('datos_adicionales', {})
        )

class Respuesta:
    def __init__(self, exito, mensaje, datos=None):
        self.exito = exito
        self.mensaje = mensaje
        self.datos = datos or {}
        self.timestamp = datetime.now().isoformat()
    
    def to_dict(self):
        return {
            'exito': self.exito,
            'mensaje': self.mensaje,
            'datos': self.datos,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(
            exito=data['exito'],
            mensaje=data['mensaje'],
            datos=data.get('datos', {})
        )
