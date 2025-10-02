from clases import LibroBiblioteca, LibroUsuario
import datetime
from datetime import timedelta

def libroBibliotecaToLibroUsuario(libro_biblioteca):
    return LibroUsuario(
        tipo="prestamo",
        libro=libro_biblioteca,
        fecha_prestamo=datetime.now(),
        fecha_devolucion=datetime.now() + timedelta(weeks=2),
        renovaciones=0
    )
    
def libroUsuarioToLibroBiblioteca(libro_usuario):
    return LibroBiblioteca(
        codigo=libro_usuario.codigo,
        titulo=libro_usuario.titulo,
        autor=libro_usuario.autor,
        ejemplares_disponibles=1,  # Asumiendo que al devolver hay 1 ejemplar disponible
        sede=""  # La sede puede ser asignada según la lógica del sistema
    )