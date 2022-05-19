# import json
import ElasticClass


es = ElasticClass.ElasticLoader()
body = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1
    },
    'mappings': {
        'examplecase': {
            'properties': {
                'tbl_id': {'type': 'keyword'},
                'texts': {'type': 'text'},
            }
        }
    }
}

# es.create_index(index="test_index")
# es.add_jsons(index="test_index", directory="jsons", count=-1)


# doc = json.load(open('jsons/Chassis_Chassis.json'))
# print(doc)
# es.add_by_json(index="test_index", json_file=doc)
