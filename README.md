# jko_api_utils
jko_api_utils is a Python package that provides a set of utility functions for working with APIs. These tools are designed to simplify common tasks that developers encounter when working with APIs, including making requests, handling responses, and saving data.

## Installation
You can install jko_api_utils using pip:

```
pip install jko_api_utils
```

## Usage
Here's an example of how to use the save_data function to save some text to a file:

```
from jko_api_utils.utils import save_data

text = "Hello, world!"
save_data(text, "hello.txt")
```
This will create a file called hello.txt in the current directory with the text "Hello, world!".

## Contributing
If you have any suggestions or would like to contribute to this project, please feel free to submit a pull request or open an issue.

## License
This package is licensed under the MIT License. See the LICENSE file for more information.