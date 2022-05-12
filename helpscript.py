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
# es.es.create("new_fprmat_test", body)
es.create_index("new_format_10000_with_awesome", None, 'jsons', count=1)
# es.add_new_jsons("new_format_10000", None, 'not_in_repo')


# doc = json.load(open('jsons3/Chassis_Chassis.json'))
# print(doc)
# es.add_by_json(doc, 'new_fprmat_test')
