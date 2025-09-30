#los PS generen requerimientos de estos tres tipos, 
# provenientes de un archivo de  texto previamente configurado, 
#de manera que se lean las peticiones de forma automática.


import zmq
#Comunicacion Síncrona en Prestamo y renovación de libros

#--------PRIMERA ENTREGA---------
def devolverLibro():
    print("Has devuelto el libro")
    
def renovarLibro():
    print("Has renovado el libro")
    
#--------SEGUNDA ENTREGA---------
def solicitarPrestamosLibro():
    print("Has solicitado un prestamo de libro")
    # El préstamo se puede solicitar por un periodo máximo de 2 semanas

