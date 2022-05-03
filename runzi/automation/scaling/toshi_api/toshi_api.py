from datetime import datetime as dt
from hashlib import md5
from pathlib import PurePath

import base64
import copy
import json
import requests

from nshm_toshi_client.toshi_client_base import ToshiClientBase, kvl_to_graphql
from nshm_toshi_client.toshi_file import ToshiFile
from nshm_toshi_client.toshi_task_file import ToshiTaskFile

from .inversion_solution import InversionSolution
from .general_task import GeneralTask, CreateGeneralTaskArgs
from .automation_task import AutomationTask

class ToshiApi(ToshiClientBase):

    def __init__(self, url, s3_url, auth_token, with_schema_validation=True, headers=None ):
        super(ToshiApi, self).__init__(url, auth_token, with_schema_validation, headers)
        self._s3_url = s3_url

        self.file = ToshiFile(url, s3_url, auth_token, with_schema_validation, headers)
        self.task_file = ToshiTaskFile(url, auth_token, with_schema_validation, headers)

        #set up the handler for inversion_solution operations
        self.inversion_solution = InversionSolution(self)
        self.general_task = GeneralTask(self)
        self.automation_task = AutomationTask(self)
        self.table = Table(self)

    def get_general_task_subtask_files(self, id):
        return self.get_subtask_files(id)

    def get_subtask_files(self, id):
        gt = self.get_general_task_subtasks(id)
        for subtask in gt['children']['edges']:
            sbt = self.get_rgt_files(subtask['node']['child']['id'])
            subtask['node']['child']['files'] = copy.deepcopy(sbt['files'])
            #TESTING
            #break
        return gt

    def get_general_task_subtasks(self, id):
        qry = '''
            query one_general ($id:ID!)  {
              node(id: $id) {
                __typename
                ... on GeneralTask {
                  id
                  title
                  description
                  created
                  swept_arguments
                  children {
                    #total_count
                    edges {
                      node {
                        child {
                          __typename
                          ... on Node {
                            id
                          }
                          ... on AutomationTaskInterface {
                            created
                            state
                            result
                            arguments {k v}
                          }
                        }
                      }
                    }
                  }
                }
              }
            }'''

        # print(qry)
        input_variables = dict(id=id)
        executed = self.run_query(qry, input_variables)
        return executed['node']


    def get_rgt_files(self, id):

        qry = '''
          fragment task_files on FileRelationConnection {
            total_count
            edges {
              node {
                ... on FileRelation {
                  role
                  file {
                    ... on Node {
                      id
                    }
                    ... on FileInterface {
                      file_name
                      file_size
                      meta {k v}
                    }
                  }
                }
              }
            }
          }

          query ($id:ID!) {
              node(id: $id) {
              __typename
              ... on Node {
                id
              }
              ... on AutomationTask {
                files {
                  ...task_files
                }
              }
              ... on RuptureGenerationTask {
                files {
                  ...task_files
                }
              }
            }
          }
        '''

        # print(qry)
        input_variables = dict(id=id)
        executed = self.run_query(qry, input_variables)
        return executed['node']


    def get_rgt_task(self, id):
        qry = '''
            query one_rupt ($id:ID!)  {
              node(id: $id) {
                __typename
                ... on RuptureGenerationTask {
                  id
                  created
                  arguments {k v}
                }
              }
            }'''
        # print(qry)
        input_variables = dict(id=id)
        executed = self.run_query(qry, input_variables)
        return executed['node']

    def get_file_detail(self, id):
      return self.file.get_file(id)

    def get_file_download_url(self, id):
      return self.file.get_file(id, True)




class Table(object):

    def __init__(self, api):
        self.api = api

    def create_table(self, rows, column_headers, column_types, object_id, table_name, table_type, dimensions, created=None):

        created = created or dt.utcnow().isoformat() + 'Z'
        dimensions = dimensions or []

        rowlen = len(column_headers)
        assert len(column_types) == rowlen
        for t in column_types:
            assert t in "string,double,integer,boolean".split(',')
        for row in rows:
            assert len(row) == rowlen
            #when do we check the coercions??

        input_variables = {
          "headers": column_headers,
          "object_id": object_id,
          "rows": rows,
          "column_types": column_types,
          "table_name": table_name,
          "created": created,
          "table_type": table_type,
          "dimensions": dimensions
        }

        qry = '''
        mutation create_table ($rows: [[String]]!, $object_id: ID!, $table_name: String!, $headers: [String]!, $column_types: [RowItemType]!, $created: DateTime!, $table_type: TableType!, $dimensions: [KeyValueListPairInput]!) {
          create_table(input: {
            name: $table_name
            created: $created
            table_type: $table_type
            dimensions: $dimensions
            object_id: $object_id
            column_headers: $headers
            column_types: $column_types
            rows: $rows
            })
          {
            table {
              id
            }
          }
        }'''

        #print(qry)
        executed = self.api.run_query(qry, input_variables)
        return executed['create_table']['table']

    def get_table(self, table_id):

        qry = '''
        query get_table($table_id:ID!) {
          node(id: $table_id) {
            ... on Table {
              id
              name
              created
              table_type
              object_id
              column_headers
              column_types
              rows
              dimensions{k v}
            }
          }
        }'''

        input_variables = {
          "table_id": table_id,
        }

        #print(qry)
        executed = self.api.run_query(qry, input_variables)
        return executed['node']