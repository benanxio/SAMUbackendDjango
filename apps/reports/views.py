import time
from rest_framework.response import Response
from rest_framework.views import APIView
from django.core.cache import cache
from rest_framework.permissions import AllowAny
from apps.reports.tasks import actualizar_datos, actualizar_datos2
from apps.reports.utils import my_custom_sql, my_custom_sql2
from background_task.models import Task
from background_task import background
from django.db import connection


class MyView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        start_time = time.time()

        data = cache.get('sp_condicion_pacientes_cache')
        if data is None:
            actualizar_datos(repeat=Task.DAILY)  
            data = my_custom_sql()
            message = 'Actualización en progreso'
        else:
            message = 'Datos obtenidos de la caché'

        end_time = time.time()

        response_data = {
            "message": message,
            "time": end_time - start_time,
            "results": data
        }

        return Response(response_data)
    
class MyView2(APIView):
    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        start_time = time.time()

        data = cache.get('obtener_indicador_errorNR_cache')
        if data is None:
            actualizar_datos2(repeat=Task.DAILY)  
            data = my_custom_sql2()
            message = 'Actualización en progreso'
        else:
            message = 'Datos obtenidos de la caché'

        end_time = time.time()

        response_data = {
            "message": message,
            "time": end_time - start_time,
            "results": data
        }

        return Response(response_data)