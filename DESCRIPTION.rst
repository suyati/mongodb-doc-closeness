Mongodb relation finder 
=======================

This is project is using to find relationship between mongodb documents

This will be the initial version of the project


How to use:

Install package with pip


.. code-block:: console

   pip install closeness


See the example,

.. code-block::

   from closeness.closeness_aggregation import ClosenessAggregation
   from pymongo import MongoClient
   client = MongoClient()
   db = client.test_database
   user_collection = db.user_collection
   user1 = {
       'name': 'User 1',
       'age': 25,
       'gender': 'male',
       'tags': [
           "tag1",
           "tag2",
           "tag3",
       ],
       'friends': [
           {"user_id": "friend1", 'name': "name1"},
           {"user_id": "friend2", 'name': "name2"},
           {"user_id": "friend3", 'name': "name3"},
       ]
   }
   user2 = {
       'name': 'User 2',
       'age': 25,
       'gender': 'male',
       'tags': [
           "tag1",
           "tag2",
           "tag3",
       ],
       'friends': [
           {"user_id": "friend1", 'name': "name1"},
           {"user_id": "friend2", 'name': "name2"},
           {"user_id": "friend3", 'name': "name3"},
       ]
   }
   user3 = {
       'name': 'User 3',
       'age': 30,
       'gender': 'female',
       'tags': [
           "tag1",
       ],
       'friends': [
           {"user_id": "friend3", 'name': "name3"},
       ]
   }
   user_collection.insert([user1, user2, user3])
   query_stage = {'$match': {'name': {'$ne': user1['name']}}}
   ARRAY_CMP_FIELDS = [
       {
           'field': 'tags',
           'weight': 3
       }
   ]
   ARRAY_DICT_CMP_FIELDS = [
       {
           'field': 'friends',
           'unique': 'user_id',
           'weight': .5
       }
   ]
   STRING_CMP_FIELDS = [
       {
           'field': 'gender',
           'weight': .5
       }
   ]
   NUM_CMP_FIELDS = [
       {
           'field': 'age',
           'from': -1,
           'to': 1,
           'weight': .3
       }
   ]
   OUT_PUT_FIELDS = [
       'name', 'age'
   ]
   test = ClosenessAggregation(
       user1,
       query_stage,
       OUT_PUT_FIELDS,
       10,
       ARRAY_CMP_FIELDS=ARRAY_CMP_FIELDS,
       STRING_CMP_FIELDS=STRING_CMP_FIELDS,
       NUM_CMP_FIELDS=NUM_CMP_FIELDS,
       ARRAY_DICT_CMP_FIELDS=ARRAY_DICT_CMP_FIELDS,
   )


   aggregation_query = closeness_obj.get_aggregation_pipeline(
       mode=ClosenessAggregation.SIMPLE
   )

   result = user_collection.aggregate(aggregation_query)

   # {u'ok': 1.0, u'result': [
   #     {u'age': 25,
   #      u'_id': ObjectId('55c478e1b67e2055605a559e'),
   #      u'name': u'User 2',
   #      u'rank': 100.00000000000001},
   #     {u'age': 30,
   #      u'_id': ObjectId('55c478e1b67e2055605a559f'),
   #      u'name': u'User 3',
   #      u'rank': 27.131782945736436}
   # ]}

   aggregation_query = test.get_aggregation_pipeline(
       mode=ClosenessAggregation.FUZZY
   )
   result = user_collection.aggregate(aggregation_query)

   # {u'ok': 1.0,
   #  u'result': [
   #      {u'age': 25,
   #       u'_id': ObjectId('55c44846b67e2028fe51c3fb'),
   #       u'name': u'User 2',
   #       u'rank': 99.99095908598945},
   #      {u'age': 30,
   #       u'_id': ObjectId('55c44846b67e2028fe51c3fc'),
   #       u'name': u'User 3',
   #       u'rank': 45.1925335646266}
   #  ]}

