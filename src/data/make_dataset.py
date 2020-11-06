'''
Módulo para realizar o enriquecimento da base da Escale.
'''

import requests
import json
import warnings
from functools import reduce
from datetime import datetime
from joblib import Parallel, delayed
from traceback import print_exc

import pandas as pd
from tqdm import tqdm

def get_point(cep, token, max_retries=5):
    '''
    Retorna a latitude e longitude do cep como uma tupla.
    '''
    try:
        base_url=('https://api.geofusion.com.br/geocoder/v1/position' +
                  '?zipCode={cep}')
        response = get_response(base_url.format(cep=cep), token)
        data = json.loads(response.text)
        if 'error' in data:
            return {'geocoder_error', data['error']}
        data['geocoder_error'] = 'None'
        data.pop('address', None)

        return data
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except ConnectionError:
        if max_retries:
            warnings.warn('Erro ao opter novamente o cep {}'.format(cep))
            return get_point(cep, token, max_retries-1)
        else:
            return {'error': 'max_retries'}
    except Exception as ex:
        return {'error': 'error'}

def get_income(lat, lng, token):
    '''
    Retorna a renda domiciliar provável do ponto.
    '''
    try:
        base_url = ('https://api.geofusion.com.br/income/v1/consumer?' +
                    'latitude={lat}&longitude={lng}')
        response = get_response(base_url, token, lat, lng)
        return {'renda_domiciliar_provavel': json.loads(response.text)}
    except Exception as ex:
        return {'Income': 'Error'}

def get_header(token):
    '''
    Retorna o header do request
    '''
    header = {'Authorization': token}
    return header

def get_response(base_url, token, lat=None, lng=None, max_retries=5):
    '''
    Faz a chamada da api.
    '''
    try:
        if lat is not None and lng is not None:
            url = base_url.format(lat=lat, lng=lng)
        else:
            url = base_url
        token = 'Bearer ' + token if 'Bearer' not in token else token
        init = datetime.now()
        response = requests.get(url, headers=get_header(token))
        warn_response_time(url, init)
        return response
    except ConnectionError:
        if max_retries:
            warnings.warn('Erro ao obter a url \'{}\''.format(base_url))
            return get_response(base_url, token, lat, lng, max_retries - 1)
        else:
            return {}

def warn_response_time(url, init):
    '''
    Emite um warging com o tempo de processamento.
    '''
    end = datetime.now()
    miliseconds = ((end - init).microseconds)/1000
    msg = 'A url [{url}] retornou em [{ms}] milisegundos.'
    msg = msg.format(url=url, ms=miliseconds)
    warnings.warn(msg)

def get_intraurban_segmentation(lat, lng, token):
    '''
    retorna os dados da segmentação urbana.
    '''
    try:
        base_url = ('https://api.geofusion.com.br/seg-intra-service/public/' +
                    'enrichPoint?latitude={lat}&longitude={lng}')
        response = get_response(base_url, token, lat, lng)
        data = json.loads(response.text)['probs']
        data['seg_intra_cluster'] = get_intraurban_cluster(lat, lng, token)
        return data
    except Exception as ex:
        return {'Seg. Intra': 'Error'}

def get_intraurban_cluster(lat, lng, token):
    '''
    Retorna a segmentação instraurbana do ponto. Ou seja, aquela com maior
    probabilidade.
    '''
    try:
        base_url = ('https://api.geofusion.com.br/seg-intra-service/public/' +
                    'enrichPointMax?latitude={lat}&longitude={lng}')
        response = get_response(base_url, token, lat, lng)
        cluster = json.loads(response.text)['max']
        return cluster if cluster else 'rural'
    except Exception as ex:
        return 'Error'

def get_pois(lat, lng, token, dispType, locomotion, direction, value):
    '''
    Retorna os pontos de interesse (points of interest).
    '''
    try:
        base_url = ('https://api.geofusion.com.br/places-enricher/v1/summary/' +
                    '{dispType}?locomotion={locomotion}&value={value}&' +
                    'latitude={lat}&longitude={lng}')
        base_url = base_url.format(dispType=dispType, locomotion=locomotion,
                                   lat=lat, lng=lng, value=value)
        if locomotion != 'WALK':
            base_url += 'direction={direction}'.format(direction=direction)

        response = get_response(base_url, token)
        data = json.loads(response.text)
        data_dict = convert_nested_dict(data['summary'], values=[])
        data_dict = {'pois__' + k: v for k, v in data_dict.items()}
        data_dict['pois__total'] = data['total']
        return data_dict
    except Exception as ex:
        return {}

def reduce_potential(potential, data_potential):
    '''
    Totaliza e converte o dicionario aninhado em um único dicionário.
    '''
    dict_potential = convert_nested_dict({potential: data_potential}, values=[])
    dict_potential[potential + '__total'] = sum(list(dict_potential.values()))
    return dict_potential

def reduce_potentials(data_potentials):
    '''
    Agrega os potenciais
    '''
    potentials = (reduce_potential(k, v) for k, v in data_potentials.items())
    return reduce(dict_merge, potentials)

def get_consumption_potential(lat, lng, token, radius, categories):
    '''
    Retorna o potencial de consumo de determinadas categorias, dentro de
    determinado raio.
    '''
    type_data='estimatedConsumptionPotential'
    base_url =('https://api.geofusion.com.br/xray/v1/areas/surroundings/' +
               '{type_data}/RADIUS?value={radius}' +
               '&latitude={lat}&longitude={lng}&categories={categories}')
    str_categories = ','.join(categories)
    url = base_url.format(radius=radius, lat=lat, lng=lng, type_data=type_data,
                          categories=str_categories)
    response = get_response(url, token)
    data_potentials = json.loads(response.text)
    potentials = reduce_potentials(data_potentials)
    return {'consumption_potential__' + k: v for k, v in potentials.items()}

def get_sociodemography(lat, lng, token, radius):
    '''
    Retorna o potencial de consumo de determinadas categorias, dentro de
    determinado raio.
    '''
    type_data='sociodemography'
    base_url =('https://api.geofusion.com.br/xray/v1/areas/surroundings/' +
               '{type_data}/RADIUS?value={radius}&latitude={lat}&' +
               'longitude={lng}')
    url = base_url.format(radius=radius, lat=lat, lng=lng, type_data=type_data)
    response = get_response(url, token)
    data_sociodemography = json.loads(response.text)
    sociodemography = convert_nested_dict(data_sociodemography, values=[])
    return {'sociodemography__' + k: v for k, v in sociodemography.items()}

def is_numeric(data):
    '''
    Confere se o objeto é numérico.
    '''
    return isinstance(data, int) or isinstance(data, float)

def convert_nested_dict(data, prefix='', values=[]):
    '''
    Converte um dicionário para um único dicionário, com as chaves concatenadas.
    '''
    if is_numeric(data):
        return data
    elif not data:
        return data
    else:
        for key, value in data.items():
            new_key = prefix + '__' + key if prefix else key
            new_value = convert_nested_dict(value, new_key, values)
            if is_numeric(new_value):
                values.append({new_key: new_value})
        if values:
            return reduce(dict_merge, values)

def dict_merge(dict_a, dict_b):
    '''
    Faz o join entre dois dicionários.
    '''
    dict_c = dict()
    dict_c.update(dict_a)
    dict_c.update(dict_b)
    return dict_c

def get_point_data(lat, lng, token, dispType, locomotaion, direction, radius,
                   value, categories):
    '''
    Retorna os dados de determinado ponto
    '''
    seg_intraurban = get_intraurban_segmentation(lat, lng, token)
    renda_dompp_provavel = get_income(lat, lng, token)
    pois = get_pois(lat, lng, token, dispType, locomotaion, direction, value)
    consumption = get_consumption_potential(lat, lng, token, radius, categories)
    sociodemography = get_sociodemography(lat, lng, token, radius)
    return {**seg_intraurban, **renda_dompp_provavel, **pois, **consumption,
            **sociodemography}

def enrich_cep(cep_index, cep, token, dispType, locomotaion, direction, value,
               radius, categories):
    '''
    Enriquece um único cep com os dados da Geofusion.
    '''
    try:
        pt_data = get_point(cep, token)
        if pt_data['geocoder_error'].upper() != 'NONE':
            return {'geocoder_error': pt_data['geocoder_error']}
        args = (token, dispType, locomotaion, direction, radius, value,
                categories)
        lat, lng = pt_data['latitude'], pt_data['longitude']
        cep_data = get_point_data(lat, lng, *args)
        return {cep_index: {**pt_data, **cep_data}}
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except:
        print_exc()
        return {cep_index: {'Error': True}}

def main(token, n_jobs=10, filename='data/raw/cep.txt', dispType='TIME',
         locomotaion='WALK', direction='OUT', value=5, radius=100,
         consumption_potential_category=['pacote_de_telefone_tv_e_internet',
             'telefone_celular', 'telefone_fixo']):
    '''
    Método principal do módulo.

    Enriquece os pontos contidos em filename e retorna o dataframe com os dados
    da Geofusion.

    Parâmetros:
    -----------

    token: str
        String com o token API da geofusion.

    filename: str, default='data/raw/cep.txt'
        Caminho do arquivo que contém os ceps dos pontos a serem enriquecidos.

    Retorna:
    --------

    Pandas dataframe com os dados enriquecidos.
    '''
    df_cep = pd.read_csv('data/raw/cep.txt')
    df_cep = df_cep.sample(frac=0.01, random_state=42)
    #df_cep = df_cep.sample(4, random_state=42)
    tqdm_cep = tqdm(df_cep.itertuples(), total=df_cep.shape[0], desc='cep')
    args = (token, dispType, locomotaion, direction, value, radius,
            consumption_potential_category)
    #data = Parallel(n_jobs=n_jobs)(delayed(enrich_cep)(row.Index, row.cep, *args) for row in tqdm_cep)
    data = [enrich_cep(row.Index, row.cep, *args) for row in tqdm_cep]
    df_data = pd.DataFrame.from_dict(reduce(dict_merge, data), orient='index')
    return df_cep.join(df_data).fillna(0)
