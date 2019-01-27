#! /user/bin/python
import click
from rightcall import elasticsearch_tools
from rightcall import configure as cfg
import configparser
import os
from pathlib import Path
import requests
from requests_aws4auth import AWS4Auth
import boto3

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
@click.option('--host', 'host', type=str, required=False, help="URL of elasticsearch endpoint")
@click.option('--index', 'index', type=str, required=False, help="Name of desired index")
@click.option('--region', 'region', type=str, required=False, help="AWS region of elasticsearch endpoint")
@click.pass_context
def elasticsearch(ctx, host, index, region):
    if host is not None:
        ctx.obj.es.host = host
    if index is not None:
        ctx.obj.es.index = index
    if region is not None:
        ctx.obj.es.region = region


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
@click.option('-s', '--source', 'source', default=None, type=str, help='Source of data')
@click.pass_obj
def update(comp2elas, source):
    click.echo(f'Getting files from: {source}')
    unknown_refs = comp2elas.update_existing_items(source)
    click.echo(unknown_refs)


@elasticsearch.command()
@click.argument('doc_id')
@click.pass_context
def get_item(ctx, doc_id):
    item = ctx.obj.es.get_item(doc_id)
    click.echo(f"{item}")


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
