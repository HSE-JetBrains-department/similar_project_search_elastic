# import GetJSON
import ElasticClass
import json

elastic = ElasticClass.ElasticLoader()
print('Elastic is running!')

'''
link = input("Type link of repo: ")
print('Wait 👀')
json_repo = json.loads(GetJSON.get_json(link))
res = elastic.get_by_repo('big_index', json_repo)
print(*res[:5], sep='\n', end='\n')
'''


def query():
    name = input("Input name: ")
    repo = elastic.es.search(index='big_index_m', body={
        'query': {
                'match': {
                    'name': name
                }
            }
        })
    if len(repo['hits']['hits']) == 0:
        print("Nothing found.")
        return
    print(repo['hits']['hits'][0]['_source']['name'])
    json_repo = repo['hits']['hits'][0]['_source']
    res = elastic.get_by_repo('big_index_m', json_repo)
    print(*res[:5], sep='\n', end='\n')
    print('\n')


while 1:
    query()



