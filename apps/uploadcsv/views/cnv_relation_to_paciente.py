from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import F

from apps.uploadcsv.models import DATA_CNV, MAESTRO_HIS_PACIENTE
from apps.uploadcsv.serializers import MaestroPacienteSerializer
from apps.uploadcsv.utils import CustomPageNumberPagination

class CNV_RELATION_PACIENTEView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            registros_cnv = DATA_CNV.objects.all()
            pacientes = []
            # Agregar la columna CNV con valores nulos a todos los pacientes
            MAESTRO_HIS_PACIENTE.objects.update(CNV=None)
            
            # Iterar sobre los registros de DATA_CNV
            for registro_cnv in registros_cnv:
                    # Buscar los registros de MAESTRO_HIS_PACIENTE donde Numero_Documento sea igual a CNV
                pacientes_query = MAESTRO_HIS_PACIENTE.objects.all().filter(
                    Numero_Documento=registro_cnv.CNV)

                # Actualizar el campo CNV_id de cada paciente con el valor de CNV de DATA_CNV
                pacientes_query.update(CNV=registro_cnv.CNV)
                
                # Agregar los pacientes a la lista
                pacientes.extend(pacientes_query)
                
            # Aplicar paginación a la lista de pacientes
            paginator = CustomPageNumberPagination()
            pacientes_paginated = paginator.paginate_queryset(pacientes, request)
            
            # Serializar los pacientes paginados
            serializer = MaestroPacienteSerializer(pacientes_paginated, many=True)
            
            # Devolver los pacientes serializados en la respuesta
            return paginator.get_paginated_response(serializer.data)
            
        except Exception as e:
            return Response(status=status.HTTP_400_BAD_REQUEST, data={'error': str(e)})
