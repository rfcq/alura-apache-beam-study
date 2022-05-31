import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

ds_columns = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]


def list_to_dict(element, columns):
    """
        Receives a list and returns a dictionary
    """
    return dict(zip(columns, element))


def text_to_list(element, delimiter='|'):
    """
        Receives text and the delimiter and returns a new list
    """
    return element.split(delimiter)


def change_date_pattern(element):
    """
        Receives a dictionary and creates new field named ano_mes
        and returns dictionary with the new field
    """
    element['ano_mes'] = '-'.join(element['data_iniSE'].split('-')[:2])
    return element


def key_uf(element):
    """
        Receives a dictionary and returns a tuple with state(UF) and element(UF, dict)
    """
    key = element['uf']
    return (key, element)


def return_dengue_cases(element):
    """
        Receives tuple ('RS', [{}, {}] and returns new tuple containing ('RS-2014-12', 8.0)
    """
    uf, registros = element
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield f"{uf}-{registro['ano_mes']}", float(registro['casos'])
        else:
            yield f"{uf}-{registro['ano_mes']}", 0.0


def return_key_uf_ano_mes(element):
    """
        Receives element list and returns tuple containing key and rain value in mm
        ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = element
    ano_mes = '-'.join(data.split('-')[:2])
    key = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return key, mm


def round_value(element):
    """
        Receives a tuple and returns value rounded
    """
    key, mm = element
    return key, round(mm, 2)


def filter_empty_fields(element):
    """
        Removes elements that contains empty keys
        Receives a tuple
    """
    key, data = element
    if all([
        data['ds_chuvas'],
        data['ds_dengue']
    ]):
        return True
    return False


def unpack_elements(element):
    key, data = element
    rain = data['ds_chuvas'][0]
    dengue = data['ds_dengue'][0]
    uf, ano, mes = key.split('-')
    return uf, ano, mes, str(rain), str(dengue)


def prepare_csv(element, delimiter = ';'):
    return f"{delimiter}".join(element)


ds_dengue = (
        pipeline
        | "Dengue Dataset read" >> ReadFromText('alura-apachebeam-sampledados/sample_casos_dengue.txt', skip_header_lines=1)
        | "Convert text to list" >> beam.Map(lambda element: element.split('|'))
        | "Convert list to dict" >> beam.Map(lambda element: dict(zip(ds_columns, element)))
        | "Create ano_mes field" >> beam.Map(change_date_pattern)
        | "Create key by state" >> beam.Map(key_uf)
        | "Group by state" >> beam.GroupByKey()
        | "Unpack dengue cases" >> beam.FlatMap(return_dengue_cases)
        | "Group cases by key" >> beam.CombinePerKey(sum)
        #| "Show result" >> beam.Map(print)
)

ds_chuvas = (
        pipeline
        | "Chuvas Dataset read" >> ReadFromText('alura-apachebeam-sampledados/sample_chuvas.csv', skip_header_lines=1)
        | "Convert to list" >> beam.Map(lambda element: element.split(','))
        | "Create key UF-ANO-MES" >> beam.Map(return_key_uf_ano_mes)
        | "Sum rain amount cases by key" >> beam.CombinePerKey(sum)
        | "Round rain amount value" >> beam.Map(round_value)
        #| "Show Chuvas dataset results" >> beam.Map(print)
)

result = (
    #(ds_chuvas, ds_dengue)
    #| "Stack pcols" >> beam.Flatten()
    #| "Group pcols" >> beam.GroupByKey()
    ({'ds_chuvas': ds_chuvas, 'ds_dengue': ds_dengue})
    | 'Merge pcols' >> beam.CoGroupByKey()
    | 'Filter empty data' >> beam.Filter(filter_empty_fields)
    | 'Unpack elements' >> beam.Map(unpack_elements)
    | 'Prepare csv' >> beam.Map(prepare_csv)
    #| "Show union result" >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

result | 'Create CSV file' >> WriteToText('result', file_name_suffix='.csv', header=header)

pipeline.run()
