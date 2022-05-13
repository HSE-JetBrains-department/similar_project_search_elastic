# from distutils.log import error
# from re import M
# from multipledispatch import dispatch
import json
import os
import spacy
import warnings
import math
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as errors


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

    def update_mappings(self, index: str):
        imports_mappings = {
            "properties": {
                "count": {
                    "type": "integer"
                },
                "key": {
                    "type": "text",
                    # "analyzer": "readme_analyzer",
                    # "search_analyzer": "readme_analyzer"
                }
            }
        }

        identifiers_mappings = {
            "properties": {
                "count": {
                    "type": "integer"
                },
                "key": {
                    "type": "text",
                    # "analyzer": "readme_analyzer",
                    # "search_analyzer": "readme_analyzer"
                }
            }
        }

        try:
            self.es.indices.put_mapping(index=index, doc_type="imports", body=imports_mappings)
        except elasticsearch.exceptions.RequestError as e:
            print(e)

        try:
            self.es.indices.put_mapping(
                index="index", doc_type="identifiers", body=identifiers_mappings)
        except elasticsearch.exceptions.RequestError as e:
            print(e)

    def add_jsons(self, index, doc_type, directory='.', count=-1):
        cnt = 0
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
                    d_splitted_identifiers.append(
                        {"key": key, "count": doc["splitted_identifiers"][key]})
                doc["imports"] = d_imports
                doc["identifiers"] = d_identifiers
                doc["splitted_identifiers"] = d_splitted_identifiers
                self.add_by_json(d=doc, index=index, doc_type=doc_type)
                cnt += 1
                if cnt == count:
                    break
        print("Added", cnt, "repositories in", index)

    def create_index(self, index: str, doc_type=None, directory='.', count=-1):
        """
            Simply creates an index using json files

        :param index: the name of index
        :param doc_type: the doc_type of elements
        :param directory: path to json files (will use all files ended with .json, be careful)
        :param count: how many json files to add
        """
        self.delete_index(index)
        try:
            # mappings_file = open('index_settings/index_mappings.json', 'r')
            # settings_file = open('index_settings/index_settings.json', 'r')
            # settings_json = json.loads(settings_file.read())
            # mappings_json = json.loads(mappings_file.read())
            mappings = {
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
                            "search_analyzer": "readme_analyzer"
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
                                ]
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
                    }
                }
            }
            self.es.indices.create(index=index, body={**mappings, **settings})
            self.add_jsons(index=index, doc_type=doc_type, directory=directory, count=count)
        except errors.RequestError as e:
            print(e)
            # print("Index " + index + " already exists")

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
            print('Added', d['owner'] + '_' + d['name'])
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
            if self.es.indices.exists(index=index):
                if input(("Delete " + index + " index? Y/n: ")) in "Yy":
                    self.es.indices.delete(index=index)
        except errors.NotFoundError:
            print("Index " + index + " does not exist")
            # exit()

    def get_by_repo(self,
                    index: str,
                    repo: dict,
                    boosts: dict = None,
                    hits_size: int = 10) -> list:
        """
            Searching by repository (dictionary)

        :param index: index name
        :param repo: json of repository
        :param boosts: dictionary with info about boost parameter for each field of search
        :param hits_size: how many results input in search
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
                imp_boost = 1 if 'imports' not in boosts else boosts['imports']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "imports.key": {
                            "query": imp["key"],
                            "boost": math.sqrt(imp["count"]) * imp_boost,
                        },
                    }
                })
        if 'identifiers' in repo.keys():
            for idf in repo['identifiers']:
                idf_boost = 1 if 'identifiers' not in boosts else boosts['identifiers']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "identifiers.key": {
                            "query": idf["key"],
                            "boost": math.sqrt(idf["count"]) * idf_boost
                        }
                    }
                })
        if 'splitted_identifiers' in repo.keys():
            for spl_idf in repo['splitted_identifiers']:
                if 'splitted_identifiers' not in boosts:
                    spl_idf_boost = 1
                else:
                    spl_idf_boost = boosts['splitted_identifiers']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "splitted_identifiers.key": {
                            "query": spl_idf["key"],
                            "boost": math.sqrt(spl_idf["count"]) * spl_idf_boost
                        }
                    }
                })
        if 'languages' in repo.keys():
            for lang in repo['languages']:
                lang_boost = 1 if 'languages' not in boosts else boosts['languages']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "languages": {
                            "query": lang,
                            "boost": lang_boost,
                        }
                    }
                })
        if "readme" in repo.keys():
            for rdm in repo['readme']:
                rdm_boost = 1 if 'readme' not in boosts else boosts['readme']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "readme": {
                            "query": rdm,
                            "boost": rdm_boost,
                        },
                    }
                })
        # print(body, '\n\n\n\n')
        res = self.es.search(index=index, body=body, size=hits_size)
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
        # print("FOUND", len(array), "ANSWERS IN INDEX", index)
        return array
