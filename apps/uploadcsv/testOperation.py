import numpy as np
import pandas as pd
from apps.uploadcsv.custom_errors import CustomError, ErrorType
from django.db import models


class DataValidator:
    def __init__(self, filed):
        self.data = None
        self.file = filed
        self.count_data_orignal_csv = 0
        self.count_part_data = 0
        self.count_data_processing = 0

    def indexar(self):
        id_values = []
        for i in range(1, len(self.data)+1):

            id_values.append(i)

        self.data['Id'] = id_values

    def validate_file_type(self):
        if not self.file.name.lower().endswith('.csv'):

            raise CustomError(
                error_type=ErrorType.FILE_TYPE_ERROR,
                message='El archivo debe ser de tipo CSV.'
            )

    def read_csv_file(self, delimiter=";", encoding='utf-8', use_cols=None, drop_cols=[]):
        self.data = pd.read_csv(self.file, delimiter=delimiter,
                                encoding=encoding, na_values=None, usecols=use_cols)
        self.data = self.data.drop(drop_cols, axis=1)

        self.count_data_orignal_csv = self.data.shape[0]

    def clean_data(self, columns_to_string=[], columns_to_int=[], columns_to_float=[]):

        # Reemplazar valores faltantes (NaN) con None
        self.data = self.data.where(pd.notnull(self.data), None)

        # Convertir columnas numéricas a tipo float
        num_cols = self.data.select_dtypes(
            include=[np.number]).columns.tolist()
        self.data[num_cols] = self.data[num_cols].astype(float)

        # Convertir valores numéricos a None cuando corresponda
        self.data[num_cols] = self.data[num_cols].applymap(
            lambda x: None if pd.isna(x) or pd.isnull(x) else int(x))

        # opcional convertir columans a int o string o float

        # Convertir columnas de fecha a tipo datetime y reemplazar NaT con None
        date_cols = self.data.select_dtypes(
            include='datetime').columns.tolist()

        for col in date_cols:
            self.data[col] = pd.to_datetime(
                self.data[col], errors='coerce').dt.date
            self.data[col] = self.data[col].apply(
                lambda x: None if pd.isna(x) else x)

        self.data = self.data.fillna(value=-1)
        # Luego convertir de -1 a None
        self.data.replace(to_replace=-1, value=None, inplace=True)
        self.count_data_processing = len(self.data)

    def replace_none_strange_values(self, values_=[]):

        null_strings = ["", "None", "NaT", "N/A", "<NA>.", "n/a", "null", "nan" "NULL", "-", "<NA>", "<nan>", "#N/A", "#N/A N/A", 'SIN DA',
                            'nullnu', "Id_Cita", "'None'",   "Anio",    "Mes",    "Dia",    "Fecha_Atencion",    "Lote",    "Num_Pag",    "Num_Reg",    "Id_Ups",    "Id_Establecimiento",    "Id_Paciente",    "Id_Personal",    "Id_Registrador",    "Id_Financiador",    "Id_Condicion_Establecimiento",    "Id_Condicion_Servicio",    "Edad_Reg",    "Tipo_Edad",    "Anio_Actual_Paciente",    "Mes_Actual_Paciente",    "Dia_Actual_Paciente",    "Id_Turno",    "Codigo_Item",    "Tipo_Diagnostico",    "Valor_Lab",    "Id_Correlativo",    "Id_Correlativo_Lab",    "Peso",    "Talla",    "Hemoglobina",    "Perimetro_Abdominal",    "Perimetro_Cefalico",    "Id_Otra_Condicion",    "Id_Centro_Poblado",    "Fecha_Ultima_Regla",    "Fecha_Solicitud_Hb",    "Fecha_Resultado_Hb",    "Fecha_Registro",    "Fecha_Modificacion",    "Id_Pais"] + values_

        replace_dict = {s: None for s in null_strings}
        self.data = self.data.replace(replace_dict)

    def split_data(self, num):

        self.data = self.data[:num]
        self.count_part_data = len(self.data)


class ObjectOperations:
    def __init__(self, data):
        self.data = data
        self.field_names = []

    def validate_columns(self, expected_columns):

        missing_columns = [
            column for column in expected_columns if column not in self.data.columns]
        new_columns = [
            column for column in self.data.columns if column not in expected_columns]

        error_messages = []

        if missing_columns or new_columns:
            if missing_columns:

                error_messages.append(
                    f'Faltan Columnas.')

            if new_columns:
                if not missing_columns:
                    error_messages.append(
                        f'Todas las columnas estan incluidas ya.')
                error_messages.append(
                    f' Hay columnas que son nuevas o estan mal escritas.')
                error_messages.append(
                    f'Intente verificar su archivo .CSV que tengan las mismas columnas')

            raise CustomError(
                error_type=ErrorType.VALIDATION_ERROR,
                message=' '.join(error_messages),
                expected_columns=expected_columns,
                details={
                    'missing_columns': missing_columns,
                    'new_columns': new_columns,
                }
            )

    def get_field_names_from_instance(self,  instance: models.Model):
        fields = instance._meta.fields
        field_names = [field.name for field in fields]
        self.field_names = field_names


class ServiceDatabase:
    def __init__(self, data, identifier_field, model):
        self.data = data
        self.model = model
        self.identifier_field = identifier_field
        self.added_objects_count = 0
        self.objects = []
        self.data_count_save = 0
        self.count_data_before = self.model.objects.all().count()

    def create_objects_from_data_nominal(self, foreign_keys=None):

        if foreign_keys is None:
            foreign_keys = {}

        fk_cache = {fk_field: {} for fk_field in foreign_keys.keys()}

        def get_fk_object(fk_field, fk_value):
            if fk_value is None:
                return None
            elif fk_value in fk_cache[fk_field]:
                return fk_cache[fk_field][fk_value]
            else:
                fk_model = foreign_keys[fk_field]

            try:
                fk_object, _ = fk_model.objects.get_or_create(pk=fk_value)
            except fk_model.DoesNotExist:
                # print("El objeto no existe")
                return None
            except Exception as e:
                # print(f"Error creating object for {fk_field}: {fk_value}")
                return None

            fk_cache[fk_field][fk_value] = fk_object
            return fk_object

        try:
            for fk_field in foreign_keys.keys():

                self.data[fk_field] = self.data[fk_field].apply(
                    lambda fk_value: get_fk_object(fk_field, fk_value))

        except Exception as e:

            raise CustomError(
                error_type=ErrorType.DATABASE_ERROR,
                message=f'Ocurrió un error al filtrar llaves foraneas',
                details={'error_details': str(e)}
            )
        try:
            self.objects = [self.model(**row._asdict())
                            for row in self.data.itertuples(index=False)]

            self.added_objects_count = len(self.objects)

        except Exception as e:
            raise CustomError(
                error_type=ErrorType.DATABASE_ERROR,
                message=f'Ocurrió un error al crear objetos de tipo {self.model.__name__}.',
                details={'error_details': str(e)}
            )

    def create_objects_from_data(self):
        try:
            existing_ids = self.model.objects.values_list(
                self.identifier_field, flat=True)
            self.data = self.data[~self.data[self.identifier_field].isin(
                existing_ids)]

            unique_objects = {}
            for _, row in self.data.iterrows():
                row_dict = row.to_dict()
                id_value = row_dict[self.identifier_field]
                if id_value not in unique_objects:
                    unique_objects[id_value] = self.model(**row_dict)
                else:
                    print("algo paso con", id_value)

            self.objects = list(unique_objects.values())
            objects = list(unique_objects.values())
            self.added_objects_count = len(objects)

        except Exception as e:
            raise CustomError(
                error_type=ErrorType.DATABASE_ERROR,
                message=f'Ocurrió un error al crear objetos de tipo {self.model.__name__}.',
                details={'error_details': str(e)}
            )

    def saveData(self, ignore_conflicts=False):

        self.model.objects.bulk_create(
            self.objects, ignore_conflicts=ignore_conflicts)
        self.data_count_save = self.model.objects.all().count()
