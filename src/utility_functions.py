import sys
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import os
from .errors import environmentError


def log_progress(count, total, status=''):
    '''
    Prints a progress bar to the command line.
    
    :param count: Current progress, eg 2 if 2 out of x tasks are done
    :param total: Total tasks to do
    :param status: Status message to print behind the progress bar
    '''
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def query_mc_api(
    query_string, 
    x_mcd_id=os.environ.get('X_MCD_ID'), 
    x_mcd_token=os.environ.get('X_MCD_TOKEN'), 
    query_params = '{}'):
    """Runs the provided query against the monte carlo api and returns the result

    :param query_string: Query to run against the monte carlo api
    :type query_string: string
    :param x_mcd_id: API Id for authentificatoion, defaults to os.environ.get('X_MCD_ID')
    :type x_mcd_id: string, optional
    :param x_mcd_token: API key for authentification, defaults to os.environ.get('X_MCD_TOKEN')
    :type x_mcd_token: string, optional
    :param query_params: Query parameters, defaults to '{}'
    :type query_params: dict, optional
    :raises ValueError: Incorrect API key setup
    :return: Response of the API
    :rtype: dict
    """  
    if x_mcd_id is None or x_mcd_token is None:
        raise environmentError(variable='Monte Carlo API keys')

    transport = RequestsHTTPTransport(
        url='https://api.getmontecarlo.com/graphql', 
        headers={
            'x-mcd-id': x_mcd_id, 
            'x-mcd-token': x_mcd_token})
    query = gql(query_string)
    client = Client(transport=transport, fetch_schema_from_transport=True)
    return client.execute(query, variable_values=query_params)

  