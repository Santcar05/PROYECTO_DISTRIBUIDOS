# interactuar con el Gestor de Carga (GC) y el Gestor de  Almacenamiento (GA). 
# Existirán al menos 2 procesos actores que se comunicarán con el Gestor  
# bajo el patrón Publicador/Suscriptor para devoluciones y renovaciones de libros,
# y al menos un  tercer actor destinado a atender las solicitudes de préstamos de forma síncrona.
import zmq