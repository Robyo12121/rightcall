#! /user/bin/python
import click
from rightcall import elasticsearch_tools
from rightcall import configure as cfg
from rightcall import dynamodb_tools
from rightcall import odigo_downloader
from requests_aws4auth import AWS4Auth
import boto3
import json
import logging


credentials = boto3.Session().get_credentials()


class Config(object):

    def __init__(self, elasticsearch=False, dynamodb=False, downloader=False):
        if elasticsearch:
            self.setup_es()
        if dynamodb:
            self.setup_db()
        if downloader:
            self.setup_dl()

    def setup_es(self):
        es_conf = cfg.Configure('elasticsearch', ('host', 'index', 'region'))
        es_data = es_conf.get(es_conf.file)
        self.es = elasticsearch_tools.Elasticsearch(
            host=es_data['host'],
            index=es_data['index'],
            region=es_data['region'],
            auth=AWS4Auth(credentials.access_key,
                          credentials.secret_key,
                          es_data['region'],
                          'es',
                          session_token=credentials.token))

    def setup_db(self):
        db_conf = cfg.Configure('dynamodb', ('region', 'table', 'endpoint'))
        db_data = db_conf.get(db_conf.file)
        self.db = dynamodb_tools.RightcallTable(
            db_data['region'],
            db_data['table'],
            db_data['endpoint'])

    def setup_dl(self):
        dl_conf = cfg.Configure('odigo',
                                ('username',
                                 'password',
                                 'driverpath',
                                 'default_download_dir',
                                 'headless',
                                 ))
        dl_data = dl_conf.get(dl_conf.file)
        if dl_data['headless'] == 'True':
            self.dl = odigo_downloader.Downloader(
                username=dl_data['username'],
                password=dl_data['password'],
                driver_path=dl_data['driverpath'],
                download_path=dl_data['default_download_dir'],
                webdriver_options={'arguments': ['headless']})
        else:
            self.dl = odigo_downloader.Downloader(
                username=dl_data['username'],
                password=dl_data['password'],
                driver_path=dl_data['driverpath'],
                download_path=dl_data['default_download_dir'],
                webdriver_options={})


@click.group()
@click.pass_context
@click.option('--debug/--no-debug', default=False, help="Enable debug log messages")
def rightcall(ctx, debug):
    pass


@rightcall.command()
@click.pass_context
@click.option('-e', '--element', required=False, type=str, help="The element of rightcall you wish to inspect. Eg. 'dynamodb', 'elasticsearch'")
def inspect(ctx, element):
    click.echo(element)
    if element == 'dynamodb':
        click.echo(ctx.obj.db)
    elif element == 'elasticsearch':
        click.echo(ctx.obj.es)
    elif element == 'download':
        click.echo(ctx.obj.dl)
    elif element is None:
        click.echo(f"Rightcall general info would go here")
    else:
        click.echo(f"Unknown option: {element}")


# Elasticsearch command
@rightcall.group()
@click.pass_context
@click.option('--debug/--no-debug', default=False, help="Enable debug log messages")
def elasticsearch(ctx, debug):
    ctx.obj = Config(elasticsearch=True)

@elasticsearch.command()
@click.pass_context
def inspect(ctx):
    click.echo(ctx.obj.es)


@elasticsearch.command()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def configure(ctx, debug):
    conf = cfg.Configure('elasticsearch', ('host', 'index', 'region'), debug=debug)
    conf.run()


@elasticsearch.command()
@click.option('--id', required=True, type=str, help="id/referenceNumber of document in elasticsearch index")
@click.pass_context
def get_item(ctx, id):
    item = ctx.obj.es.get_item(id)
    click.echo(f"{item}")


@elasticsearch.command()
@click.option('--id', required=True, type=str, help="id/referenceNumber of document in elasticsearch index")
@click.option('--dryrun/--no-dryrun', default=False, help="Call with '--dryrun' to see what items will be deleted")
@click.pass_context
def delete_item(ctx, id, dryrun):
    item = ctx.obj.es.delete_item(id, dryrun=dryrun)
    click.echo(f"{item}")


@elasticsearch.command()
@click.option('--id', type=str, required=True, help="referenceNumber/id of document")
@click.option('--item', type=str, required=False, help="""JSON object enclosed in single quotes. eg. '{"referenceNumber": "012345678"}'""")
@click.option('--path', type=str, required=False, help="absolute path to json file")
@click.pass_context
def put_item(ctx, id, item, path):
    click.echo(f"{item}")
    if not path and not item:
        click.echo(f"You must specify either '--item' or '--path'")
    elif path and item:
        click.echo(f"You must specify either '--item' or '--path'")
    if path is None:
        try:
            data = json.loads(item)
        except json.decoder.JSONDecodeError:
            click.echo("Invalid JSON: enter json string enclosed in single quotes: '{\"key\": \"value\"}'")
            return
    else:
        with open(path, 'r') as file:
            data = json.load(file)
    response = ctx.obj.es.put_item(id, data)
    click.echo(f"{response}")


@elasticsearch.command()
@click.option('--id', type=str, required=True, help="referenceNumber/id of document")
@click.option('--item', type=str, required=True, help="""Enclose JSON object in single quotes. eg. '{"referenceNumber": "012345678"}'""")
@click.pass_context
def update(ctx, id, item):
    try:
        i = json.loads(item)
    except json.decoder.JSONDecodeError:
        click.echo("Invalid JSON: enter json string enclosed in single quotes: '{\"key\": \"value\"}'")
        return
    item = ctx.obj.es.update(id, i)
    click.echo(f"{item}")


@elasticsearch.command()
@click.option('--query', type=str, required=True, help="""Elaticsearch query enclosed in single quotes. Eg. '{"query": {"match": {"referenceNumber": "5403e8TVd10419"}}}'""")
@click.option('--return_metadata/--no-return_metadata', default=False)
@click.pass_context
def search(ctx, query, return_metadata):
    try:
        q = json.loads(query)
    except json.decoder.JSONDecodeError:
        click.echo("Invalid JSON: enter json string enclosed in single quotes: '{\"key\": \"value\"}'")
        return
    if 'query' not in q:
        click.echo("Invalid Query: query must be of the form: '{\"query\": {\"<search/match method>\": {\"<your field>\": \"<your search term>\"}}}'")
        return
    results = ctx.obj.es.search(q, return_metadata=return_metadata)
    click.echo(f"{results}")


@elasticsearch.command()
@click.option('--query', type=str, required=True, help="""Elaticsearch query enclosed in single quotes. Eg. '{"query": {"match": {"referenceNumber": "5403e8TVd10419"}}}'""")
@click.option('--dryrun/--no-dryrun', default=True, help="Call with '--dryrun' to see what items will be deleted")
@click.pass_context
def delete_by_query(ctx, query, dryrun):
    try:
        q = json.loads(query)
    except json.decoder.JSONDecodeError:
        click.echo("Invalid JSON: enter json string enclosed in single quotes: '{\"key\": \"value\"}'")
        return
    if 'query' not in q:
        click.echo("Invalid Query: query must be of the form: '{\"query\": {\"<search/match method>\": {\"<your field>\": \"<your search term>\"}}}'")
        return
    result = ctx.obj.es.delete_by_query(q, dryrun=dryrun)
    click.echo(result)


@elasticsearch.command()
@click.pass_context
@click.option('--id', required=True, help="referenceNumber/id of document")
def fully_populated(ctx, id):
    result = ctx.obj.es.fully_populated_in_elasticsearch(id)
    click.echo(result)


@elasticsearch.command()
@click.pass_context
@click.option('--query', type=str, required=True, help="Elasticsearch query")
@click.option('--dryrun/--no-dryrun', default=True)
@click.argument('keywords', type=str, required=True, nargs=-1)
def remove_keywords(ctx, keywords, query, dryrun):
    try:
        q = json.loads(query)
    except json.decoder.JSONDecodeError:
        click.echo("Invalid JSON: enter json string enclosed in single quotes: '{\"key\": \"value\"}'")
        return
    if 'query' not in q:
        click.echo("Invalid Query: query must be of the form: '{\"query\": {\"<search/match method>\": {\"<your field>\": \"<your search term>\"}}}'")
        return
    click.echo(f"Attempting to remove {keywords} from all documents that match {query}")
    ctx.obj.es.modify_by_search(q, ctx.obj.remove_keywords, keywords, dryrun=dryrun)


# Dynamodb
@rightcall.group()
@click.pass_context
@click.option('--debug/--no-debug', default=False)
def dynamodb(ctx, debug):
    ctx.obj = Config(dynamodb=True)


@dynamodb.command()
@click.pass_context
def inspect(ctx):
    click.echo(ctx.obj.db)


@dynamodb.command()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def configure(ctx, debug):
    conf = cfg.Configure('dynamodb', ('region', 'table', 'endpoint'), debug=debug)
    conf.run()


@dynamodb.command()
@click.pass_context
@click.option('--id', required=True, help="Primary key (referenceNumber) of database document")
def get_item(ctx, id):
    result = ctx.obj.db.get_db_item(id)
    click.echo(result)


# Retrieve Command
@rightcall.group()
@click.pass_context
@click.option('--debug/--no-debug', default=False)
def download(ctx, debug):
    ctx.obj = Config(downloader=True)


@download.command()
@click.pass_context
def inspect(ctx):
    click.echo(ctx.obj.dl)


@download.command()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def configure(ctx, debug):
    conf = cfg.Configure('odigo', ('username', 'password', 'driverpath', 'default_download_dir', 'headless'), debug=debug)
    conf.run()


@download.command()
@click.option('--id', 'referenceNumber', required=False, type=str, help="Specify reference number string of call to retrieve")
@click.option('--csv', 'csv_path', required=False, default=None, type=str, help="Specify absolute path to csv file")
@click.option('--dst', 'destination', required=False, default=None, type=str, help="Absolute path for destination folder")
@click.pass_context
def mp3(ctx, referenceNumber, csv_path, destination):
    if not referenceNumber and not csv_path:
        click.echo("You must specify either a reference number or a path to a csv file")
        return

    if not destination:
        click.echo(f"Destination not specified. Downloading to default directory: {ctx.obj.dl.download_path}")

    if referenceNumber and not csv_path:
        ctx.obj.dl.download_mp3_by_ref(referenceNumber)
        return

    elif csv_path and not referenceNumber:
        db_conf = cfg.Configure('dynamodb', ('region', 'table', 'endpoint'))
        db_data = db_conf.get(db_conf.file)
        # Create object using config info to use with sub commands
        ctx.obj.db = dynamodb_tools.RightcallTable(
            db_data['region'],
            db_data['table'],
            db_data['endpoint'])
        ctx.obj.db.write_csv_to_db(csv_path)
        result = ctx.obj.dl.download_mp3_by_csv(csv_path, download_dir=destination)
    click.echo(result)


if __name__ == '__main__':
    logger = logging.getLogger('rightcall')
    logger.setLevel('DEBUG')
    ch = logging.StreamHandler()
    ch.setLevel('DEBUG')
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    rightcall()
