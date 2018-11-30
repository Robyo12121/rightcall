import click
import comprehend_to_elasticsearch

@click.group()
def rightcall_local():
    pass

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
    

    




"""
