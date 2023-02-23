from abc import abstractmethod

class BatchAPIController:
    def __init__(self, file_path_generator, api_caller, data_saver):
        self.file_path_generator = file_path_generator
        self.api_caller = api_caller
        self.data_saver = data_saver

    def process_batch(self, input_data):
        for data in input_data:
            file_path = self.file_path_generator.generate_filepath(data)
            if self.api_caller.should_call_api(file_path):
                response = self.api_caller.call_api(data)
                self.data_saver.save_data(file_path, response)
            else:
                print(f"Skipping API call for {file_path}")


class FilePathGenerator:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    @abstractmethod
    def generate_filepath(self, input_data):
        # Generate a file path based on the input data
        # For example, if input_data is an ID number, generate the file path using the ID
        pass


class ApiCaller:
    def __init__(self, api_fn):
        self.api_fn = api_fn

    @abstractmethod
    def should_call_api(self, file_path):
        # Check if the API should be called based on whether or not the file path exists
        pass

    @abstractmethod
    def call_api(self, input_data):
        # Call the API and return the response
        pass


class DataSaver:

    def __init__(self) -> None:
        outputs = []
    
    def parse_response(self, response):
        # Parse the response and return the data to save
        pass

    def save_data(self, file_path, data):
        # Save the data to the specified file path
        with open(file_path, 'w') as f:
            f.write(data)
