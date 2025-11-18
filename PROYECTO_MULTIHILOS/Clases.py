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