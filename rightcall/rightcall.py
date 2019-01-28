#! /user/bin/python
import click
from rightcall import elasticsearch_tools
from rightcall import configure as cfg
import configparser
import os
from pathlib import Path
# import requests
from requests_aws4auth import AWS4Auth
import boto3
import json

credentials = boto3.Session().get_credentials()


class Config(object):

    def __init__(self):
        data = self.read_config('elasticsearch')
        self.es = elasticsearch_tools.Elasticsearch(
            host=data['host'],
            index=data['index'],
            region=data['region'],
            auth=AWS4Auth(credentials.access_key,
                          credentials.secret_key,
                          data['region'],
                          'es',
                          session_token=credentials.token))

    def read_config(self, header):
        self.parser = configparser.ConfigParser()
        config_file = Path(os.environ.get('HOME')) / '.rightcall' / 'config.ini'
        self.parser.read(config_file)
        data = {}
        for key in self.parser[header]:
            data[key] = self.parser[header][key]
        return data


@click.group()
@click.pass_context
def rightcall(ctx):
    ctx.obj = Config()


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
@click.pass_context
def inspect(ctx):
    print(ctx.obj.es)


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
@click.option('--item', type=str, required=True, help="""Enclose JSON object in single quotes. eg. '{"referenceNumber": "012345678"}'""")
@click.pass_context
def put_item(ctx, id, item):
    i = json.loads(item)
    item = ctx.obj.es.put_item(id, i)
    click.echo(f"{item}")


@elasticsearch.command()
@click.option('--id', type=str, required=True, help="referenceNumber/id of document")
@click.option('--item', type=str, required=True, help="""Enclose JSON object in single quotes. eg. '{"referenceNumber": "012345678"}'""")
@click.pass_context
def update(ctx, id, item):
    i = json.loads(item)
    item = ctx.obj.es.update(id, i)
    click.echo(f"{item}")


@elasticsearch.command()
@click.argument('query', type=str, required=True)
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


# Retrieve Command
@rightcall.group()
def retrieve():
    pass


@retrieve.command()
@click.option('--ref', 'referenceNumber', type=str, help="Specify reference number string of call to retrieve")
def call(referenceNumber):
    click.echo(referenceNumber)


if __name__ == '__main__':
    rightcall()
