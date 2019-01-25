<<<<<<< HEAD
#! /user/bin/python
import click
from comprehend_to_elasticsearch import Comp2Elas
=======
import click
import comprehend_to_elasticsearch
>>>>>>> Starting to implement click cli interface

@click.group()
def rightcall_local():
    pass

<<<<<<< HEAD
@rightcall_local.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def elasticsearch(ctx, debug):
    if debug:
        ctx.obj = Comp2Elas('eu-west-1', 'http://localhost:8000', 'comprehend.rightcall',
                            'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/',
                            'http://localhost:9200',
                            loglevel='DEBUG')
    else:
        ctx.obj = Comp2Elas('eu-west-1', 'http://localhost:8000', 'comprehend.rightcall',
                    'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/',
                    'http://localhost:9200')
    click.echo(f"Debug mode is {'on' if debug else 'off'}")

@elasticsearch.command()
@click.option('-s','--source', 'source', default=None, type=str, help='Source of data')
@click.pass_obj
def add(comp2elas, source):
    click.echo(f"{source}")
    click.echo(f"Getting files from: {source}")
    click.echo(f"Type: {type(source)}")
    unknown_refs = comp2elas.add_new_or_incomplete_items(source)
    click.echo(unknown_refs)   

@elasticsearch.command()
@click.option('-s','--source', 'source', default=None, type=str, help='Source of data')
@click.pass_obj
def update(comp2elas, source):
    click.echo(f"Getting files from: {source}")
    unknown_refs = comp2elas.update_existing_items(source)
    click.echo(unknown_refs)
    

=======
@rightcall_local.command()
@click.option('-s','--source', 'source', type=str, required=True,  help='Source of data')
def elasticsearch(source):
    click.echo(f"Getting files from: {source}")
    unknown_refs = comprehend_to_elasticsearch.add_new_or_incomplete_items(source)
    click.echo(unknown_refs)
    
        
@rightcall_local.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your Name:', help='The person to greet')
def hi(count, name):
    for x in range(count):
        click.echo(f"Hi {name}!")
>>>>>>> Starting to implement click cli interface


if __name__ == '__main__':
    rightcall_local()

"""
CLI Tool Design:

Want to be able to run commonly used functions from cli to make things easier.
Be able to use different arguments, and get some results/feedback/output about performance.

Things I want to be able to do:

    Process items from comprehend bucket, add db metadata to it and add to ES index
    automatically in one step. Both new items (that don't exist in Index yet) and update
    existing items.

    'elasticsearch add/update <source-bucket>/<source list> <dest-index>'
<<<<<<< HEAD
=======
    

    



>>>>>>> Starting to implement click cli interface

"""
