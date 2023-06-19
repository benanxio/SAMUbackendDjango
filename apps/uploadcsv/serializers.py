from rest_framework import serializers

from .models import MAESTRO_HIS_ESTABLECIMIENTO, MAESTRO_HIS_CIE_CPMS, MAESTRO_HIS_NUEVO_ARCHIVO_PLANO, MAESTRO_HIS_UBIGEO_INEI_RENIEC, MAESTRO_HIS_OTRA_CONDICION, MAESTRO_HIS_ETNIA, MAESTRO_HIS_PROFESION, MAESTRO_HIS_TIPO_DOC, MAESTRO_HIS_PAIS, MAESTRO_HIS_CONDICION_CONTRATO, MAESTRO_HIS_FINANCIADOR, MAESTRO_HIS_UPS, MAESTRO_HIS_COLEGIO, MAESTRO_HIS_CENTRO_POBLADO, MAESTRO_HIS_ACTIVIDAD_HIS, MAESTRO_HIS_REGISTRADOR, MAESTRO_HIS_PACIENTE,MAESTRO_HIS_PERSONAL


class MaestroPersonalSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_PERSONAL
        fields = '__all__'


class MaestroRegistradorSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_REGISTRADOR
        fields = '__all__'


class MaestroPacienteSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_PACIENTE
        fields = '__all__'


class MAESTRO_HIS_NUEVO_ARCHIVO_PLANOSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_NUEVO_ARCHIVO_PLANO
        fields = '__all__'


class MAESTRO_HIS_UBIGEO_INEI_RENIECSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_UBIGEO_INEI_RENIEC
        fields = '__all__'


class MAESTRO_HIS_ESTABLECIMIENTOSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_ESTABLECIMIENTO
        fields = '__all__'


class MAESTRO_HIS_CIE_CPMSSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_CIE_CPMS
        fields = '__all__'


class MAESTRO_HIS_OTRA_CONDICION_Serializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_OTRA_CONDICION
        fields = '__all__'


class MAESTRO_HIS_ETNIASerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_ETNIA
        fields = '__all__'


class MAESTRO_HIS_PROFESION_Serializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_PROFESION
        fields = '__all__'


class MAESTRO_HIS_TIPO_DOC_Serializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_TIPO_DOC
        fields = '__all__'


class MAESTRO_HIS_PAIS_Serializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_PAIS
        fields = '__all__'


class MAESTRO_HIS_CONDICION_CONTRATOSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_CONDICION_CONTRATO
        fields = '__all__'


class MAESTRO_HIS_FINANCIADOR_Serializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_FINANCIADOR
        fields = '__all__'



class UpsSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_UPS
        fields = '__all__'


class MaestroHisActividadHisSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_ACTIVIDAD_HIS
        fields = '__all__'

class MAESTRO_HIS_COLEGIO_Serializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_COLEGIO
        fields = '__all__'


class MAESTRO_HIS_CENTRO_POBLADOSerializer(serializers.ModelSerializer):
    class Meta:
        model = MAESTRO_HIS_CENTRO_POBLADO
        fields = '__all__'
