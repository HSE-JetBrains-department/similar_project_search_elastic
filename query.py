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
    print(repo['hits']['hits'][0]['_source']['owner'] +
          '/' + repo['hits']['hits'][0]['_source']['name'])
    json_repo = repo['hits']['hits'][0]['_source']
    res = elastic.get_by_repo('big_index_m', json_repo)
    print(*res, sep='\n', end='\n')
    print('\n')


# while 1:
#     query()


def parse_link(link):
    substr = link[len('https://github.com/'):]
    return [substr[:substr.find('/')],
            substr[substr.find('/') + 1:]]


def print_repos_from_group(group):
    print('GROUP: ', group)
    tests = json.loads(str(open('tests.json').read()))
    for link in tests[group]:
        print(link)


def test_group(key):
    # Хотим рассмотреть репозиторий из группы и посмотреть что для него выдал поиск

    tests = json.loads(str(open('tests.json').read()))
    found = 0
    array = []
    cnt = 0
    for link in tests[key]:
        owner, name = parse_link(link)
        repo = elastic.es.search(index='big_index_m', body={
            'query': {
                "dis_max": {
                    "queries": [
                        {"match": {"owner": owner}},
                        {"match": {"name": name}}
                    ]
                }
            }
        })
        if len(repo['hits']['hits']) == 0:
            pass
        elif repo['hits']['hits'][0]['_source']['owner'] == owner \
                and repo['hits']['hits'][0]['_source']['name'] == name:
            found += 1
            array.append('https://github.com/' +
                         repo['hits']['hits'][0]['_source']['owner'] +
                         '/' + repo['hits']['hits'][0]['_source']['name'])
            # saving links to all repos from group

            if cnt == 0:
                query_repo_json = repo['hits']['hits'][0]['_source']
                query_repo_name = repo['hits']['hits'][0]['_source']['owner'] + \
                    '/' + repo['hits']['hits'][0]['_source']['name']
                cnt = 1
        else:
            continue
            # saving name of repo we will search by

    print('TEST CASE:', key, "\t\tSIZE:", len(tests[key]), "FOUND:", found)
    print("Accuracy =", 100 * found / len(tests[key]), '\n||||||||||||||||||||')
    print("Searching by:", query_repo_name)
    res = elastic.get_by_repo('big_index_m', query_repo_json, 10)
    # We want return k docs, where k is group size in our index
    # (intersection group and index)

    intersection = 0
    for link in res:
        print(link)
        if link != array[0] and link in array:
            intersection += 1
    print("METRIC = ", intersection / found * 100, '\n\n')
    print_repos_from_group(key)
    print('--------------------------------------------\n\n\n')


def testing():
    tests = json.loads(str(open('tests.json').read()))
    cnt = 0
    for key in tests.keys():
        test_group(key)


# test_group('Asynchronous Programming')
# print_repos_from_group('Asynchronous Programming')
testing()
