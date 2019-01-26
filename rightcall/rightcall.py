#! /user/bin/python
import click
from rightcall import elasticsearch_tools


@click.group()
def rightcall():
    pass


# Elasticsearch command
@rightcall.group()
@click.option('--host', 'host', type=str, required=False, help="URL of elasticsearch endpoint")
@click.option('--index', 'index', type=str, required=False, help="Name of desired index")
@click.option('--region', 'region', type=str, required=False, help="AWS region of elasticsearch endpoint")
@click.pass_context
def elasticsearch(ctx, host, index, region):
    ctx.obj = elasticsearch_tools.Elasticsearch(host=host, region=region, index=index)


@elasticsearch.command()
@click.pass_context
def inspect(ctx):
    print(ctx.obj)


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
def get_item(doc_id):
    item = elasticsearch_tools.get_item(doc_id)
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
