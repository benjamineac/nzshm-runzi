from .config_builder import Config

class InversionConfig(Config):
    def __init__(self, task_title, task_description, file_id, worker_pool_size, jvm_heap_max, java_threads,
    use_api,  general_task_id, mock_mode) -> None:
        super().__init__(task_title, task_description, file_id, worker_pool_size, jvm_heap_max, java_threads,
        use_api,  general_task_id, mock_mode)
        self._rounds = [str(x) for x in range(1)]
        self._subtask_type = "INVERSION"
        self._completion_energies = [],
        self._max_inversion_times = [],
        self._mfd_transition_mags = [],
        self._mfd_equality_weights = [],
        self._mfd_inequality_weights = [],
        self._slip_rate_weighting_types = [],
        self._slip_rate_normalized_weights = [],
        self._slip_rate_unnormalized_weights = [],              
        self._selection_interval_secs = [],
        self._threads_per_selector = [],
        self._averaging_threads = [],
        self._averaging_interval_secs = [],
        self._non_negativity_function = [], 
        self._perturbation_function = []

class SubductionConfig(InversionConfig):
    def __init__(self, task_title, task_description, file_id, worker_pool_size, jvm_heap_max, java_threads,
    use_api,  general_task_id, mock_mode) -> None:
        super().__init__(task_title, task_description, file_id, worker_pool_size, jvm_heap_max, java_threads,
        use_api,  general_task_id, mock_mode)

        self._model_type = "SUBDUCTION"
        self._mfd_uncertainty_weights =[],
        self._mfd_uncertainty_powers =[],
        self._mfd_mag_gt_5s = [],
        self._mfd_b_values = [],   

    # def run_subduction_inversion(self):
    #     None
    
class CrustalConfig(InversionConfig):
    def __init__(self, task_title, task_description, file_id, worker_pool_size, jvm_heap_max, java_threads,
    use_api,  general_task_id, mock_mode) -> None:
        super().__init__(task_title, task_description, file_id, worker_pool_size, jvm_heap_max, java_threads,
        use_api,  general_task_id, mock_mode)

        self._model_type = "CRUSTAL"
        self._slip_rate_weights = [],
        self._slip_uncertainty_scaling_factors = [],
        self._seismogenic_min_mags = [],
        self._mfd_mag_gt_5_sans = [],
        self._mfd_mag_gt_5_tvz = [],
        self._mfd_b_values_sans = [],
        self._mfd_b_values_tvz = []

    # def runCrustal(self):
    #     None