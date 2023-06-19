from background_task import background
from django.core.cache import cache
from django.db import connection

@background(schedule=10)  
def actualizar_datos():
    with connection.cursor() as cursor:
        cursor.callproc('public.sp_condicion_pacientes')
        results = cursor.fetchall()
        field_names = [name[0] for name in cursor.description]
        data = [dict(zip(field_names, result)) for result in results]
        cache.set('sp_condicion_pacientes_cache', data)
        

@background(schedule=10)  
def actualizar_datos2():
    with connection.cursor() as cursor:
        cursor.callproc('public.obtener_indicador_errornr')
        results = cursor.fetchall()
        field_names = [name[0] for name in cursor.description]
        data = [dict(zip(field_names, result)) for result in results]
        cache.set('obtener_indicador_errorNR_cache', data)
        

        
        
