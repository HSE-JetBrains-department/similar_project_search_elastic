import search.ElasticClass as ElasticClass
import click


es = ElasticClass.ElasticLoader()


@click.group()
def search():
    """
    Simple interface to work with ElasticSearch index.
    """
    pass


@search.command()
@click.argument('index_name', type=str)
def create(index_name: str):
    """
    Create a new index called INDEX_NAME.
    """
    print(es.create_index(index=index_name))


@search.command()
@click.argument('index_name', type=str)
def delete(index_name: str):
    """
    Delete index called INDEX_NAME.
    """
    print(es.delete_index(index=index_name))


@search.command()
@click.argument('index_name', type=str)
@click.argument('path', type=click.Path(exists=True))
@click.option('--count', 'count', default=-1, help='Number of jsons to add')
def add(index_name: str, path: str, count: int):
    """
    Add jsons located in PATH to index INDEX_NAME.
    """
    print(es.add_jsons(index=index_name, directory=path, count=count))


@search.command()
@click.argument('index_name', type=str)
@click.argument('link', type=str)
@click.option('--count', 'count', default=25, help='Number of elements to find')
def find(index_name: str, link: str, count: int):
    """
    Find similar repositories to LINK in index INDEX_NAME.
    """
    res = es.find_by_link(index=index_name, link=link, hits_size=count)
    if res != "Success":
        print(res)


if __name__ == "__main__":
    search()
