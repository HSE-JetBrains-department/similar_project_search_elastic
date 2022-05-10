import json
import os
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as errors
from multipledispatch import dispatch
import spacy
import warnings


warnings.filterwarnings("ignore")        # Разумеется, это временно
LOG = 0


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
                            "type": "text",
                            "analyzer": "readme_analyzer",
                            "search_analyzer": "readme_analyzer",
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
                                    "type": "integer"
                                },
                                "key": {
                                    "type": "text",
                                    "analyzer": "readme_analyzer",
                                    "search_analyzer": "readme_analyzer"
                                }
                            }
                        },
                        "identifiers": {
                            "properties": {
                                "count": {
                                    "type": "integer"
                                },
                                "key": {
                                    "type": "text",
                                    "analyzer": "readme_analyzer",
                                    "search_analyzer": "readme_analyzer"
                                }
                            }
                        },
                        "splitted_identifiers": {
                            "properties": {
                                "count": {
                                    "type": "integer"
                                },
                                "key": {
                                    "type": "text",
                                    "analyzer": "readme_analyzer",
                                    "search_analyzer": "readme_analyzer"
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
            settings = {
                "settings": {
                    "analysis": {

                        "analyzer": {
                            "readme_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "english_stop",
                                    "russian_stop",
                                    "chineese_stop",
                                    "snowball_english"
                                ],
                                "char_filter": [
                                    "number_filter"
                                ]
                            },
                            "imports_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "snowball_english"
                                ],
                            }
                        },
                        "filter": {
                            "english_stop": {
                                "type": "stop",
                                "stopwords": "_english_"
                            },
                            "russian_stop": {
                                "type": "stop",
                                "stopwords": "_russian_"
                            },
                            "chineese_stop": {
                                "type": "stop",
                                "stopwords": "_cjk_"
                            },
                            "my_snow": {
                                "type": "snowball",
                                "language": "English"
                            },
                            "snowball_english": {
                                "type": "condition",
                                "filter": [
                                    "my_snow"
                                ],
                                "script": {
                                    "source": "token.getTerm().length() > 5"
                                }
                            }
                        },
                        "char_filter": {
                            "number_filter": {
                                "type": "pattern_replace",
                                "pattern": "\\d+",
                                "replacement": ""
                            }
                        }
                    },
                },
            }
            cnt = 0
            self.es.indices.create(index=index, body={**mapping, **settings})
            for file in os.listdir(directory):
                if file.endswith('.json'):
                    path = directory + '/' + file
                    print(file + ",    SIZE:", os.path.getsize(path))
                    if os.path.getsize(path) > 100000:
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
            # exit()

    def get_by_repo(self, index: str, repo: dict, limit=25) -> list:
        """
            Searching by repository (dictionary)

        :param index: index name
        :param repo: json of repository
        :param limit: count or results
        :return: python list of found elements (dictionaries)
        """
        strictness = "should"
        body = {
            "query": {
                "bool": {
                    strictness: []
                }
            }
        }
        if 'imports' in repo.keys():
            for imp in repo['imports']:
                body["query"]['bool'][strictness].append({
                        "match": {
                            "imports.key": imp["key"],
                        }
                })
        if 'identifiers' in repo.keys():
            for idf in repo['identifiers']:
                body["query"]['bool'][strictness].append({
                    "match": {
                        "identifiers.key": idf["key"],
                    }
                })
        if 'languages' in repo.keys():
            for lang in repo['languages']:
                body["query"]['bool'][strictness].append({
                    "match": {
                        "languages": lang
                    }
                })
        if "readme" in repo.keys():
            for rdm in repo['readme']:
                body["query"]['bool'][strictness].append({
                    "match_phrase": {
                        "readme": rdm
                    }
                })
        # print(body, '\n\n\n\n')
        res = self.es.search(index=index, body=body)
        # print(res)
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
