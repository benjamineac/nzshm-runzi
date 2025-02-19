
import datetime as dt
from dateutil.tz import tzutc
from hashlib import md5
from pathlib import PurePath

import base64
import copy
import json
import requests

from nshm_toshi_client.toshi_client_base import ToshiClientBase, kvl_to_graphql


class CreateGeneralTaskArgs(object):

    def __init__(self, title, description, agent_name, created=None):
        self._arguments = dict(
          created = dt.datetime.now(tzutc()).isoformat(),
          agent_name = agent_name,
          title = title,
          description = description,
          argument_lists = [],
          subtask_type = 'Undefined',
          subtask_count = 0,
          model_type = 'Undefined',
          meta = []
        )

    def set_argument_list(self, arg_list):
        self._arguments['argument_lists'] = arg_list
        subtask_count = 1
        for arg in arg_list:
            subtask_count *= len(arg['v'])
        self._arguments['subtask_count'] = subtask_count
        return self

    def set_meta(self, meta_list):
        self._arguments['meta'] = meta_list
        return self

    def set_subtask_type(self, subtask_type):
        assert subtask_type in ['RUPTURE_SET', 'INVERSION', 'HAZARD', 'REPORT']
        self._arguments['subtask_type'] = subtask_type
        return self

    def set_model_type(self, model_type):
        assert model_type in ['CRUSTAL', 'SUBDUCTION']
        self._arguments['model_type'] = model_type
        return self

    def as_dict(self):
        return copy.copy(self._arguments)

class GeneralTask(object):

    def __init__(self, api):
        self.api = api
        assert isinstance(api, ToshiClientBase)

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
                  children {
                    #total_count
                    edges {
                      node {
                        child {
                          __typename
                          ... on Node {
                            id
                          }
                          ... on RuptureGenerationTask {
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
        executed = self.api.run_query(qry, input_variables)
        return executed['node']



    def create_task(self, create_args):
        '''
        created: DateTime
        When the taskrecord was created
        updated: DateTime
        When task was updated
        agent_name: String
        The name of the person or process responsible for the task
        title: String
        A title always helps
        description: String
        Some description of the task, potentially Markdown
        '''
        assert isinstance(create_args, CreateGeneralTaskArgs)

        qry = '''
            mutation create_gt ($created:DateTime!, $agent_name:String!, $title:String!, $description:String!,
              $argument_lists: [KeyValueListPairInput]!, $subtask_type:TaskSubType!, $subtask_count:Int!,
              $model_type: ModelType!, $meta: [KeyValuePairInput]!) {
              create_general_task (
                input: {
                  created: $created
                  agent_name: $agent_name
                  title: $title
                  description: $description
                  argument_lists: $argument_lists
                  subtask_type: $subtask_type
                  subtask_count:$subtask_count
                  model_type: $model_type
                  meta:$meta
                })
                {
                  general_task {
                    id
                  }
                }
            }
        '''
        print(qry)

        #input_variables = dict(created=created, agent_name=agent_name, title=title, description=description)
        executed = self.api.run_query(qry, create_args.as_dict())
        return executed['create_general_task']['general_task']['id']
