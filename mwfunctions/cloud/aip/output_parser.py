import numpy as np

class AIPOutputParser():
    def __init__(self, framework="tensorflow", reshape=None, squeeze_result=False):
        '''
        Model Class designed for parse output of AIEngine model to standardised type like array or list of floats.

        Framework:
            tensorflow:
                Wir erhalten eine liste an dicts mit key 'output_0'

        :param framework: Framework of model used in AIE. tensorflow default. In Future other frameworks like pytorch are also possible
        :param reshape: Tuple of parsed output shape. If set, parser tries to reshape output. If None nothing happens
        :param squeeze_result: Boolean whether output should be squeezed or not.
        '''
        self.framework = framework
        self.reshape = reshape
        self.squeeze_result = squeeze_result

    def get_type_of_list_elements(self, model_output_list):
        if len(model_output_list) > 1:
            assert type(model_output_list[0]) == type(model_output_list[1]), "First element of list has different type as last element of list"
        return type(model_output_list[0])

    def convert_list_of_dicts2np(self, model_output_list_dicts):
        size_before = len(model_output_list_dicts)
        # What shall happen if some dict does not contain "output_0" but "output_1" for an example?
        if self.framework == "tensorflow":
            model_output_list_floats = [float(v["output_0"]) for v in model_output_list_dicts if "output_0" in v]
        else:
            raise NotImplementedError(f"Transforming list of dict of framework {self.framework} is not implemented at the moment.")
        if size_before != len(model_output_list_floats):
            raise NotImplementedError("List of dict could not be transformed to numpy array while keeping same length of input list")

        return np.array(model_output_list_floats)

    def convert2np(self, model_output):
        """
        Convert model output to numpy array. Numpy array can be used for internal parseing/transformation methods like reshaping
        :param model_output: Ai Engine model output
        :return: Numpy array of model_output
        """
        if type(model_output).__module__ == np.__name__:
            return model_output
        elif type(model_output) == list and self.get_type_of_list_elements(model_output) in [int, float, list]:
            return np.array(model_output)
        elif type(model_output) == list and self.get_type_of_list_elements(model_output) == dict:
            return self.convert_list_of_dicts2np(model_output)
        else:
            raise ValueError(f"Type {type(model_output)} is not defined to be converted to numpy array.")

    def post_processing(self, model_output):
        model_output_np = self.convert2np(model_output)
        # if squeeze results try to squeeze it
        if self.squeeze_result:
            model_output_np = np.squeeze(model_output_np)
        # If self.reshape is set try to reshape it. But only if output should not be squeezed
        elif self.reshape != None:
            model_output_np = model_output_np.reshape(self.reshape)

        return model_output_np

    def to_list(self, model_output):
        model_output_np = self.post_processing(model_output)
        return model_output_np.tolist()

    def to_np(self, model_output):
        return self.post_processing(model_output)
