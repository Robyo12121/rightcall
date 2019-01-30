#! /user/bin/python
import click
from rightcall import elasticsearch_tools
from rightcall import configure as cfg
from rightcall import dynamodb_tools
from rightcall import odigo_robin
from requests_aws4auth import AWS4Auth
import boto3
import json

credentials = boto3.Session().get_credentials()


class Config(object):

    def __init__(self, debug=False):
        es_conf = cfg.Configure('elasticsearch', ('host', 'index', 'region'), debug=debug)
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

        db_conf = cfg.Configure('dynamodb', ('region', 'table', 'endpoint'), debug=debug)
        db_data = db_conf.get(db_conf.file)
        self.db = dynamodb_tools.RightcallTable(
            db_data['region'],
            db_data['table'],
            db_data['endpoint'])


@click.group()
@click.pass_context
@click.option('--debug/--no-debug', default=False, help="Enable debug log messages")
def rightcall(ctx, debug):
    ctx.obj = Config(debug=debug)


@rightcall.command()
@click.pass_context
@click.option('-e', '--element', required=False, type=str, help="The element of rightcall you wish to inspect. Eg. 'dynamodb', 'elasticsearch'")
def inspect(ctx, element):
    click.echo(element)
    click.echo(f"Type: {type(element)}")
    if element == 'dynamodb':
        click.echo(ctx.obj.db)
    elif element == 'elasticsearch':
        click.echo(ctx.obj.es)
    elif element is None:
        click.echo(f"Rightcall general info would go here")
    else:
        click.echo(f"Unknown option: {element}")


# Elasticsearch command
@rightcall.group()
def elasticsearch():
    pass


@elasticsearch.command()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def configure(ctx, debug):
    conf = cfg.Configure('elasticsearch', ('host', 'index', 'region'), debug=debug)
    conf.run()


@elasticsearch.command()
@click.option('-s', '--source', 'source', default=None, type=str, help='Source of data')
@click.pass_obj
def add(comp2elas, source):
    click.echo(f'{source}')
    click.echo(f'Getting files from: {source}')
    click.echo(f'Type: {type(source)}')
    unknown_refs = comp2elas.add_new_or_incomplete_items(source)
    click.echo(unknown_refs)


@elasticsearch.command()
@click.option('--id', required=True, type=str, help="id/referenceNumber of document in elasticsearch index")
@click.pass_context
def get_item(ctx, id):
    item = ctx.obj.es.get_item(id)
    click.echo(f"{item}")


@elasticsearch.command()
@click.option('--id', type=str, required=True, help="referenceNumber/id of document")
@click.option('--item', type=str, required=False, help="""JSON object enclosed in single quotes. eg. '{"referenceNumber": "012345678"}'""")
@click.option('--path', type=str, required=False, help="absolute path to json file")
@click.pass_context
def put_item(ctx, id, item, path):
    if path is None:
        data = json.loads(item)
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
    i = json.loads(item)
    item = ctx.obj.es.update(id, i)
    click.echo(f"{item}")


@elasticsearch.command()
@click.option('--query', type=str, required=True, help="Elasticsearch query")
@click.option('--return_metadata/--no-return_metadata', default=False)
@click.pass_context
def search(ctx, query, return_metadata):
    q = json.loads(query)
    results = ctx.obj.es.search(q, return_metadata=return_metadata)
    click.echo(f"{results}")


@elasticsearch.command()
@click.argument('query', type=str, required=True)
@click.option('--dryrun/--no-dryrun', default=True)
@click.pass_context
def delete_by_query(ctx, query, dryrun):
    q = json.loads(query)
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
@click.option('--dryrun/--no-dryrun', default=False)
@click.argument('keywords', type=str, required=True, nargs=-1)
def remove_keywords(ctx, keywords, query, dryrun):
    q = json.loads(query)
    click.echo(f"Attempting to remove {keywords} from all documents that match {query}")
    ctx.obj.es.modify_by_search(q, ctx.obj.es.remove_keywords, keywords, dryrun=dryrun)


# Dynamodb
@rightcall.group()
def dynamodb():
    pass


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
def download():
    pass


@download.command()
# @click.option('--ref', 'referenceNumber', required=False, type=str, help="Specify reference number string of call to retrieve")
@click.option('--csv', 'csv_path', required=False, type=str, help="Specify absolute path to csv file")
@click.option('--dst', 'destination', required=False, type=str, help="Absolute path for destination folder")
def mp3(csv_path, destination):
    click.echo(csv_path, destination)


if __name__ == '__main__':
    rightcall()
