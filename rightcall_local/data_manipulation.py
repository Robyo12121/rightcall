import elasticsearch_tools
# import s3 as s3py
import logging
import boto3
from requests_aws4auth import AWS4Auth


def setup_logging(loglevel='INFO'):
    # Logging
    levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if loglevel not in levels:
        raise ValueError(f"Invalid log level choice {loglevel}")
    logger = logging.getLogger('rightcall')
    logger.setLevel(loglevel)
    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def change_date(item_dict, newDate):
    """Change the date of a datetime field
        while preserving the original time."""
    current_datetime = item_dict['date']
    logger.debug(f"Current datetime: {current_datetime}")
    time = current_datetime.split(' ')[1]
    logger.debug(f"Time: {time}")
    new_datetime = ' '.join((newDate, time))
    logger.debug(f"New datetime: {new_datetime}")
    item_dict['date'] = new_datetime
    logger.debug(f"Returning Item: {item_dict}")
    return item_dict


def skill_to_country(item_dict, mapping):
    # logger.debug(f"skill_to_country called with {item_dict}, {mapping}")
    if 'country' in item_dict:
        logger.warning(f"'Country' already found in item. Aborting.")
        return item_dict
    if item_dict['skill'] in mapping.keys():
        logger.info(f"{item_dict['skill']} found")
        item_dict['country'] = mapping[item_dict['skill']]
        logger.info(f"Setting country to {mapping[item_dict['skill']]}")
    else:
        item_dict['country'] = 'Unknown'
        logger.info(f"Value not found in mapping. Setting Unknown")
    logger.info(f"Deleting skill field")
    del item_dict['skill']
    logger.debug(f"New Item: {item_dict}")
    return item_dict


def remove_useless_keywords(item_dict, keywords):
    # for item in item_dict['keyPhrases']:
    pass


def modify_by_search(es, query_dict, function, *args, dryrun=True):
    """
    INPUT: es - elasticsearch_tools.Elasticsearch() instance
           query_dict - dictionary containing elasticsearch query
           function - function to apply to each search result
                      (must return complete document to be reindexed)
           *args - any arguments to be passed to the function
    """
    results = es.search(query_dict)
    logger.info(f"Number of items retreived: {len(results)}")
    for i, item in enumerate(results):
        logger.info(f"Working on {i}")
        try:
            item = function(item, *args)
        except KeyError as k_err:
            logger.error(f"KeyError: {k_err}, Item: {item}")
        except Exception as e:
            logger.error(f"Something went wrong with: Item: {item}, Error: {e}")
        if not dryrun:
            es.put_item(item['referenceNumber'], item)


def cleanup_bad_docs(es, dryrun=True):
    bad_docs_query = {"from": 0, "size": 100, "query": {"bool": {"must_not": {"exists": {"field": "referenceNumber"}}}}}
    bad_docs = es.search(bad_docs_query, return_metadata=True)
    logger.debug(f"Type: {type(bad_docs)}")
    ids = [item['_id'] for item in bad_docs['hits']['hits']]
    num_ids = len(ids)
    logger.debug(ids)
    delete_bad_docs_query = {"query": {"ids": {"values": ids}}}
    response = es.delete_by_query(delete_bad_docs_query, dryrun=dryrun)
    if not dryrun:
        if response['deleted'] == num_ids:
            return 'success'
        else:
            return 'failed'
    return response


if __name__ == '__main__':
    region = 'eu-west-1'
    BUCKET = 'comprehend.rightcall'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        'es',
        session_token=credentials.token)
    es = elasticsearch_tools.Elasticsearch(
        'search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com',
        region,
        index='demo',
        auth=awsauth)
    logger = setup_logging('DEBUG')

    logger.info(cleanup_bad_docs(es))
    # mapping_dict = {'ISRO_TEVA_US_EN': 'US',
    #                 'ISRO_TEVA_US_VIP_EN': 'US',
    #                 'ISRO_TEVA_GB_EN': 'GB',
    #                 'ISRO_TEVA_GB_P1_EN': 'GB',
    #                 'ISRO_TEVA_IE_EN': 'IE'}
    # search_query = {"from": 0, "size": 100, "query": {"match_all": {}}}
    # modify_by_search(es, search_query, skill_to_country, mapping_dict)
