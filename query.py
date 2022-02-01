import GetJSON
import ElasticClass
import json

elastic = ElasticClass.ElasticLoader()
print('Elastic is running!')

link = input("Type link of repo: ")
print('Wait 👀')
json_repo = json.loads(GetJSON.get_json(link))
res = elastic.get_by_repo('big_index', json_repo)
print(*res[:5], sep='\n', end='\n')


'''res = elastic.es.search(index='big_index', body={'query': {'bool': {'must': 
    {'bool': {'minimum_should_match': 1, 'should': 
        [{'match': {'languages': {'query': 'Python'}}}, 
            {'match': {'languages': {'query': 'Documentation'}}}, 
            {'match': {'imports': {'query': 'os'}}},
            {'match': {'imports': {'query': 'elasticsearchexceptionserrors'}}}, 
            {'match': {'imports': {'query': 'json'}}}, 
            {'match': {'imports': {'query': 'multipledispatchdispatch'}}}, 
            {'match': {'imports': {'query': 'elasticsearchElasticsearch'}}}]}}}}})

print(res['hits']['hits'][0])'''
