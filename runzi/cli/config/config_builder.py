import json
import pprint

class Config:
    def __init__(self, task_title, task_description, worker_pool_size, jvm_heap_max,
    java_threads, use_api, general_task_id, file_id, mock_mode) -> None:

        self._task_title = task_title
        self._task_description = task_description
        self._worker_pool_size = worker_pool_size
        self._jvm_heap_max = jvm_heap_max
        self._java_threads = java_threads
        self._use_api = use_api
        self._general_task_id = general_task_id
        self._file_id = file_id
        self._mock_mode = mock_mode

    def to_json(self):
        file_name = f'{self._file_id}_config.json'
        out_file = open(file_name, "w")
        json.dump(self.__dict__, out_file, indent = 2)
        out_file.close()

    def get_args(self):
        non_args = ['_worker_pool_size', '_jvm_heap_max', '_java_threads',
        '_subtask_type', '_use_api', '_task_title', '_task_description', 
        '_general_task_id', '_file_id', '_mock_mode', '_model_type',]

        return {k:v for k, v in self.__dict__.items() if k not in non_args}
    
    def get_keys(self):
        return list(self.__dict__.keys())

    def get_all(self):
        return dict(self.__dict__.items())

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)