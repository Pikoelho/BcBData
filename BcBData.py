import pandas as pd
from typing import Union

##########################
# Author: Tarek Vilela
# Contributor: Murilo Getlinger Coelho @ FGV-EESP
# Last update: 2021-12-01
##########################

def get_series(code: int, name: str = 'value', 
                start: str = None,  end: str = None, 
                return_type: Union[pd.Series, pd.DataFrame] = pd.Series, date_column: str = 'Data') -> pd.Series:
    
    '''
    Function to download series based on series code. 

    code: series code (int).
    name: rename downloaded series (str) (optional) (default is 'value')
    start: start date (str) (yyyy-mm-dd) (optional) (default is None)
    end: end date (str) (yyyy-mm-dd) (optional) (default is None)
    return_type: type of output (class) (optional) (default is pd.Series). Can be either pd.Series or pd.DataFrame. 

    '''

    # url base
    url_base = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=csv'

    # url with the code of the series
    url = url_base.format(code)

    # series download and changing the format of the datestring
    df = pd.read_csv(url, decimal=',', sep=';')
    df.columns = [date_column, name]
    df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y')

    return return_type(df.set_index(date_column).loc[slice(start, end)][name])

# ------ DEPRECATED ------
#def check_lists(code_list, name_list):
#    '''
#    Function to check if arguments used in get_multiple_series is correct.
#    '''
#    
#    len_check = True if len(code_list) == len(name_list) else False
#
#    return len_check


def get_multiple_series(code_list: list, name_list: list, start: str = None, end: str = None) -> pd.DataFrame:
    '''
    Function to download multiple series.
    '''

    n1 = len(code_list)
    n2 = len(name_list)

    if n1 < n2:
        name_list = name_list[:n1]
    elif n1 > n2:
        name_complete = [f'unnamed_{i}' for i in range(1, n1-n2+1)]
        name_list.extend(name_complete)
        
    result = pd.DataFrame()

    for code, name in zip(code_list, name_list):
        series = get_series(code, name, return_type=pd.DataFrame)

        result = pd.concat([result, series], axis=1)

    return result.loc[slice(start, end)]


def create_query_url(frequency: str, Indicators: list = None, start: str = None, end: str = None) -> str:
    
    urls_dict = {'annual': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais',
                 'quarterly': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTrimestrais',
                 'monthly': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativaMercadoMensais',
                 'inflation-12-months': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoInflacao12Meses',
                 'top5s-monthly': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Mensais',
                 'top5s-annual': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Anuais',
                 'institutions': 'https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoInstituicoes'}

    if not frequency in urls_dict:
        raise Exception("Frequency doesn't exist")

    url_base = urls_dict[frequency]

    filters = []

    if Indicators:
        indic_filter = ' or '.join([f"Indicador eq '{i}'" for i in Indicators])
        filters.append(indic_filter)

    if start:
        start_filter = f"Data ge '{start}'"
        filters.append(start_filter)

    if end:
        end_filter = f"Data le '{end}'"
        filters.append(end_filter)
    
    filters_string = ' and '.join(filters).replace(' ', '%20')
    
    select = 'Indicador,Data,DataReferencia,Media,Mediana,DesvioPadrao,Minimo,Maximo,numeroRespondentes,baseCalculo'
    
    query = '?$top=100000000&' + '$filter=' + filters_string + '&' + '$format=text/csv&' + '$select=' + select

    url = url_base + query
    url = url.replace('â', '%C3%A2').replace('çã', '%C3%A7%C3%A3o').replace('í', '%C3%AD')

    return url


def get_market_expectations(frequency: str, Indicators: list = None, start: str = None, end: str = None) -> pd.DataFrame:
    '''
    Function to download market expectations.
    You must choose a frequency or from a especial category, such as 'top5s-monthly'.
    '''

    url = create_query_url(frequency, Indicators, start, end)
    
    df = pd.read_csv(url, sep=',', decimal=',')
    df['Data'] = pd.to_datetime(df['Data'])

    if frequency == 'monthly':
        df['DataReferencia'] = pd.to_datetime(df['DataReferencia'], format='%m/%Y')
    
    elif frequency == 'annual':
        df['DataReferencia'] = pd.to_datetime(df['DataReferencia'], format='%Y')

    elif frequency == 'quarterly':
        qs = ('Q' + df['DataReferencia']).str.replace('/', ' ').str.replace(r'(Q\d) (\d+)', r'\2-\1')
        df['DataReferencia'] = pd.PeriodIndex(qs, freq='Q').to_timestamp()

    return df


def mom2index(data: Union[pd.Series, pd.DataFrame]) -> Union[pd.Series, pd.DataFrame]:
    '''
    Function to transform month-over-month series to index.
    Beware, normally SGS series return MoM in %, so you must divide by 100 before
    running this function.
    '''

    def _mom2index(series: pd.Series) -> pd.Series:
        index = (series.dropna() + 1).cumprod()
        index /= index.iloc[0]

        return index
        
    if isinstance(data, pd.Series):
        index = _mom2index(data)

    elif isinstance(data, pd.DataFrame):
        index = pd.DataFrame()
        for c in data.columns:
            index_ = _mom2index(data[c])
            index = pd.concat([index, index_], axis=1)

    return index


def index2yoy(series: Union[pd.Series, pd.DataFrame]) ->Union[pd.Series, pd.DataFrame]:
    '''
    Function to calculate year-over-year (12 months).
    '''
    
    yoy = series / series.shift(12) - 1

    return yoy


def mom2yoy(series: Union[pd.Series, pd.DataFrame]) -> Union[pd.Series, pd.DataFrame]:
    '''
    Function to calculate MoM to YoY.
    First calculate index then calculate YoY.
    '''
    
    return index2yoy(mom2index(series))
