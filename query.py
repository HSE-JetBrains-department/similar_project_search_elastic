# from tokenize import group
# import os
import ElasticClass
import json
import elasticsearch
import logging

elastic = ElasticClass.ElasticLoader()
logging.basicConfig(filename='logs.log', level=logging.CRITICAL)
logging.info('Elastic started')

standard_boosts = {'imports': 1,
                   'identifiers': 1,
                   'splitted_identifiers': 1,
                   'languages': 1,
                   'readme': 1}

best_mean = 0

best_boosts = {'imports': 1,
               'identifiers': 1,
               'splitted_identifiers': 6,
               'languages': 1,
               'readme': 1}


def query(index: str):
    name = input("Input name: ")
    repo = elastic.es.search(index=index, body={
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


def parse_link(link):
    substr = link[len('https://github.com/'):]
    return [substr[:substr.find('/')],
            substr[substr.find('/') + 1:]]


def print_repos_from_group(group, index):
    print('GROUP: ', group)
    tests = json.loads(str(open('tests.json').read()))
    for repo_link in tests[group]:
        owner, name = parse_link(repo_link)
        result = repo_exists_in_index(owner, name, index=index)
        print(repo_link, end=' ')
        if result:
            print('✅')
        else:
            print('❌')


def repo_exists_in_index(owner, name, index, print_action=0):
    repo = elastic.es.search(index=index, body={
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
        return None

    elif repo['hits']['hits'][0]['_source']['owner'] == owner \
            and repo['hits']['hits'][0]['_source']['name'] == name:
        if print_action:
            pass
            #  print(repo['hits']['hits'][0]['_source'])
        return repo['hits']['hits'][0]['_source']


def test_group(key, index, boosts=None, hits_size: int = 10, print_info: bool = False):
    if boosts is None:
        boosts = standard_boosts
    tests = json.loads(str(open('tests.json').read()))
    array = []
    cnt = 0
    found = 0
    query_repo_json = None
    query_repo_name = None
    for repo_link in tests[key]:
        owner, name = parse_link(repo_link)
        result = repo_exists_in_index(owner, name, index=index)
        if not result:
            continue
        else:
            found += 1
            # print("found", result)
            array.append('https://github.com/' +
                         result['owner'] + '/' +
                         result['name'])
            if cnt == 0:
                query_repo_json = result
                query_repo_name = owner + '/' + name
                cnt = 1
            # saving name of repo we will search by

    if found < 5:
        return
    if print_info:
        print('TEST CASE:', key, ", GROUP SIZE:", len(tests[key]), ", FOUND IN INDEX:", found)
        print("Accuracy =", 100 * found / len(tests[key]))
        print_repos_from_group(key, index)
        print("\n🚀🚀🚀🚀🚀 Searching by:", query_repo_name)
    try:
        res = elastic.get_by_repo(index, query_repo_json, boosts=boosts, hits_size=hits_size)
        if print_info:
            for link in res:
                print(link)
    except elasticsearch.exceptions.RequestError as e:
        if print_info:
            print(e, "name of repo:", query_repo_json['name'])
        return
    # We want return k docs, where k is group size in our index
    # (intersection group and index)

    intersection = 0
    for link in res:
        # print(link)
        if link != array[0] and link in array:
            intersection += 1
    metric = intersection / found * 100
    if print_info:
        print("METRIC = ", metric, '\n\n')
        print('--------------------------------------------\n\n\n')
    return metric


def testing(index, boosts=None, hits_size: int = 10, print_info=False):
    global best_boosts
    global best_mean
    if boosts is None:
        boosts = standard_boosts
    tests = json.loads(str(open('tests.json').read()))
    results = []
    for key in tests.keys():
        m = test_group(key, index, boosts=boosts, hits_size=hits_size, print_info=print_info)
        if m is not None:
            results.append([key, m])
    print(boosts)
    print('METRICS:')
    for key, m in results:
        print(f'{key}: {m}')
    print('--------------------------------------------')
    global_result = sum(map(lambda x: x[1], results)) / len(results)
    if print_info:
        with open("./metrics.txt", "a") as f:
            f.write("BOOSTS: ")
            f.write(str(boosts) + '\n')
            f.write("MEAN: ")
            f.write(str(global_result) + '\n')
            f.write("\n\n")
            f.flush()
    if global_result > best_mean:
        best_mean = global_result
        best_boosts = boosts
    print(f'MEAN: {global_result} \n\n')


def find_best_boosts(index: str, start: int, stop: int):
    #  found best:
    #  MEAN: 29.305555555555554
    #  BOOSTS:
    #  {'imports': 1, 'identifiers': 1, 'splitted_identifiers': 4, 'languages': 1, 'readme': 1}
    rng = range(start, stop)
    for imps in rng:
        for idf in rng:
            for spl_idf in rng:
                for lng in rng:
                    for rdm in rng:
                        testing(index, boosts={
                            'imports': imps,
                            'identifiers': idf,
                            'splitted_identifiers': spl_idf,
                            'languages': lng,
                            'readme': rdm
                        },
                                hits_size=25,
                                print_info=False)


my_boosts = {'imports': 1,
             'identifiers': 1,
             'splitted_identifiers': 6,
             'languages': 1,
             'readme': 1}
testing("new_format_10000", boosts=my_boosts, hits_size=25, print_info=False)

'''
find_best_boosts(index="new_format_10000", start=1, stop=5)
'''

'''
test_group('Debugging Tools')
print_repos_from_group('Asynchronous Programming')
'''

'''
res = elastic.get_by_repo('big_index_m',
            repo_exists_in_index('careermonk',
            'data-structures-and-algorithmic-thinking-with-python', 1), 10)
for l in res:
    print(l)
'''

'''
for name in os.listdir('jsons3'):
    print(name, name[:name.find('_')], name[name.find('_') + 1:-5])
    json = repo_exists_in_index(name[:name.find('_')], name[name.find('_') + 1:-5])
    if json is None:
        continue
    res = elastic.get_by_repo('jsons3', json)
    for l in res:
        print(l)
    print('-----------------------------------\n\n')
'''

'''
res = elastic.get_by_repo('new_format_100_with_int',
            repo_exists_in_index('skywind3000',
            'ECDICT', index='new_format_100_with_int', print_action=1), 10)
'''
