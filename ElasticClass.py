import json
import os
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as errors
from multipledispatch import dispatch
import spacy
import warnings


warnings.filterwarnings("ignore")        # Разумеется, это временно
LOG = 1


class ElasticLoader:
    """
        ElasticLoader provides functionality for creating an index and searching
        through it using the Elastic Search API
        By default, the index is called similar_projects, it is stored in the index_ value
    """

    def __init__(self, host="http://localhost", port=9200):
        """
            Connecting to the elastic search server using your own host and port
            By default, it is connected locally to http://localhost:9200
        """

        self.es = Elasticsearch(HOST=host, PORT=port, timeout=1000)
        if not self.es.ping():
            print("Connection to " + host + ":" + str(port) + " failed")
            exit()

    def create_index(self, index: str, doc_type=None, ind=1, directory='.'):
        """
            Simply creates an index using json files

        :param index: the name of index
        :param doc_type: the doc_type of elements
        :param ind: the the number for indexing elements (first : ind, second : ind + 1, etc.)
        :param directory: path to json files (will use all files ended with .json, be careful)
        """
        self.delete_index(index)
        try:
            mapping = {
                "mappings": {
                    "properties": {
                        "owner": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "readme": {
                            "type": "text"
                        },
                        "languages": {
                            "type": "keyword"
                        },
                        "percentages": {
                            "type": "keyword"
                        },
                        "imports": {
                            "properties": {
                                "count": {
                                    "type": "text"
                                },
                                "key": {
                                   "type": "text",
                                   "analyzer": "english"
                                }
                            }
                        },
                        "identifiers": {
                            "properties": {
                                "count": {
                                    "type": "text"
                                },
                                "key": {
                                   "type": "text",
                                   "analyzer": "english"
                                }
                            }
                        },
                        "splitted_identifiers": {
                            "properties": {
                                "count": {
                                    "type": "text"
                                },
                                "key": {
                                   "type": "text",
                                   "analyzer": "english"
                                }
                            }
                        },
                        "docstrings": {
                            "type": "text"
                        },
                        "stargazers_count": {
                            "type": "keyword"
                        },
                        "commit_sha": {
                            "type": "keyword"
                        },
                        "repo_id": {
                            "type": "keyword"
                        }
                    }
                }
            }
            cnt = 0
            self.es.indices.create(index=index, body=mapping)
            for file in os.listdir(directory):
                if file.endswith('.json'):
                    path = directory + '/' + file
                    print(file + ",    SIZE:", os.path.getsize(path))
                    if os.path.getsize(path) > 10000:
                        continue
                    doc = json.load(open(path))

                    nlp = spacy.load("en_core_web_sm")
                    for i in range(len(doc['readme'])):
                        text = doc['readme'][i]
                        readme = nlp(text)
                        clear_tokens = self.clear_nums(readme)
                        doc['readme'][i] = ' '.join(clear_tokens)
                    d_imports = []
                    for key in doc['imports']:
                        d_imports.append({"key": key, "count": doc["imports"][key]})
                    d_identifiers = []
                    for key in doc['identifiers']:
                        d_identifiers.append({"key": key, "count": doc["identifiers"][key]})

                    d_splitted_identifiers = []
                    for key in doc['splitted_identifiers']:
                        d_splitted_identifiers.append({"key": key, "count": doc["splitted_identifiers"][key]})
                    doc["imports"] = d_imports
                    doc["identifiers"] = d_identifiers
                    doc["splitted_identifiers"] = d_splitted_identifiers
                    self.add_by_json(d=doc, index=index, doc_type=doc_type, id_=ind)
                    ind += 1
                    cnt += 1
                    if cnt == 100:
                        break

        except errors.RequestError:
            print("Index " + index + " already exists")

    @staticmethod
    def get_json(url):
        return {}

    @staticmethod
    def clear_nums(tokens) -> list:
        clear_tokens = []
        for token in tokens.ents:
            token_copy = str(token)
            token = str(token).replace('.', '')
            token = str(token).replace(',', '')
            token = str(token).replace(':', '')
            token = str(token).replace('/', '')
            if not token.isnumeric():
                clear_tokens.append(token_copy)
        return clear_tokens

    def add_by_json(self, d: dict, index="", doc_type=None, id_=None):
        """
            Adds and element to the index using json (python dict) file

        :param d: python dict with description of element for index
        :param index: the name of index
        :param doc_type: the doc_type of elements
        :param id_: unique id for the element
        """

        try:
            self.es.index(index=index, doc_type=doc_type, id=id_, document=d)
            print('Added')
        except errors.TransportError as e:
            print('Error', e)
            print(e.info)
            pass

    def add_by_url(self, url):
        self.add_by_json(self.get_json(url))

    def add_by_url_list(self, u_list):
        for url in u_list:
            try:
                my_json = self.get_json(url)
                self.add_by_json(my_json)
            except RuntimeError:
                return 1
        return 0

    def pop(self, id_):
        pass

    def get(self):
        pass

    def delete_index(self, index: str):
        """
            Deletes the index

        :param index: the name of index
        """

        try:
            if input(("Delete " + index + " index? Y/n: ")) in "Yy":
                self.es.indices.delete(index=index)
        except errors.NotFoundError:
            print("Index " + index + " does not exist")
            exit()

    @dispatch(dict)
    def get_by_multi_match(self, d: dict) -> list:
        """
            Search by query as in elasticsearch
            Documentation: https://www.elastic.co/guide/en/elasticsearch/
            reference/current/search-your-data.html
        :param d: elastic_search-like python dictionary with query
        :return: python list of found elements
        """

        try:
            res = self.es.search(body=d)
            array = []
            for i in range(len(res['hits']['hits'])):
                array.append(res['hits']['hits'][i]['_source'])
            return array
        except errors.ElasticsearchException:
            print("Something is wrong with your query")
            exit()

    @dispatch(str, list, list)
    def get_by_multi_match(self, index: str, pairs_must: list, pairs_must_not: list):
        """
            Searching by list of pairs []
        :param index: index name
        :param pairs_must: [field_1, value_1], ...] means field_i must be value_i
        :param pairs_must_not: [[field_1, value_1], ...] means filed_i must not be value_i
        :return: python list of found elements
        """

        if pairs_must is None:
            raise ValueError('empty query')

        body = {
            "query": {
                "bool": {
                    "must": [],
                    "must_not": []
                }
            }
        }
        for p in pairs_must:
            sub_dict = {'fields': [p[0]],
                        'query': p[1],
                        "type": "cross_fields",
                        "operator": "AND"
                        }
            body["query"]["bool"]["must"].append({
                'multi_match': sub_dict
                }
            )
        for p in pairs_must_not:
            sub_dict = {'fields': [p[0]],
                        'query': p[1],
                        "type": "cross_fields",
                        "operator": "AND"
                        }
            body["query"]["bool"]["must_not"].append({
                'multi_match': sub_dict
                }
            )
        res = self.es.search(index=index, body=body)
        array = []

        for i in range(max(0, len(res['hits']['hits']))):
            if 'repo_name' in res['hits']['hits'][i]['_source']:
                array.append(res['hits']['hits'][i]['_source']['repo_name'])
                if LOG:
                    print(res['hits']['hits'][i], '\n\n')
        print("FOUND", len(array), "ANSWERS IN INDEX", index)
        return array

    def get_by_repo(self, index: str, repo: dict, limit=25) -> list:
        """
            Searching by repository (dictionary)

        :param index: index name
        :param repo: json of repository
        :param limit: count or results
        :return: python list of found elements (dictionaries)
        """
        body = {
            # "explain": True,
            "from": 0,
            "size": limit,
            "query": {
                "function_score": {
                    'functions': []
                }
            }
        }

        if 'languages' in repo.keys():
            for lang in repo['languages']:
                sub_dict = dict()
                sub_dict['languages'] = {'query': lang}
                body["query"]['function_score']['functions'].append({
                    'filter': {
                            'match': sub_dict,
                    },
                    'weight': 5
                })
        print(body)

        if 'imports' in repo.keys():
            for imp in repo['imports']:
                sub_dict = dict()
                sub_dict['imports'] = {'query': imp}
                body["query"]['function_score']['functions'].append({
                        'filter': {
                            'match_phrase': sub_dict,
                        },
                        'weight': 0
                })

        if 'names' in repo.keys():
            for name in repo['names']:
                sub_dict = dict()
                sub_dict['names'] = {'query': name}
                body["query"]['function_score']['functions'].append({
                    'filter': {
                            'match_phrase': sub_dict,
                    },
                    'weight': 1
                })
        nlp = spacy.load("en_core_web_sm")
        text = repo['readme'][0]
        doc = nlp(text)
        clear_tokens = self.clear_nums(doc)

        body["query"]['function_score']['functions'].append({
            "filter": {
                "match_phrase": {
                    "readme": {
                        "query": ' '.join(clear_tokens)
                    }
                }
            },
            "weight": 0
        })

        res = self.es.search(index=index, body=body)
        array = []
        if LOG:
            with open('body.txt', 'w+') as file:
                file.write(str(body))

        for i in range(max(0, len(res['hits']['hits']))):
            if 'name' in res['hits']['hits'][i]['_source']:
                link = 'https://github.com/' + \
                       res['hits']['hits'][i]['_source']['owner'] + \
                       '/' + res['hits']['hits'][i]['_source']['name']
                array.append(link)
                if LOG:
                    with open('info' + str(i) + '.txt', 'w+') as file:
                        file.write(str(res['hits']['hits'][i]))
        print("FOUND", len(array), "ANSWERS IN INDEX", index)
        return array
