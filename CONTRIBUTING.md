# Contributing

Thank you for considering contributing to this project! The following guidelines will help you set up a development environment and submit pull requests.

## Development Environment Setup

1. **Clone the repository**
   ```sh
   git clone https://github.com/yourusername/PermutiveAPI.git
   cd PermutiveAPI
   ```
2. **Create a virtual environment** (recommended)
   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```
3. **Install development dependencies**
   ```sh
   pip install -r requirements-dev.txt
   ```
4. **Install the package in editable mode** (optional)
   ```sh
   pip install -e .
   ```

## Code Style and Linting

Before committing, please run the following checks to ensure code quality and consistency. The CI pipeline enforces these checks.

- **Formatting with Black:**
  ```sh
  black .
  ```
- **Docstring Style with pydocstyle:**
  ```sh
  pydocstyle PermutiveAPI
  ```
- **Static Type Checking with pyright:**
  ```sh
  pyright PermutiveAPI
  ```

## Running Tests

This project uses `pytest` for testing. To run the test suite, navigate to the root directory of the project and run the following command:

```sh
pytest
```

This will automatically discover and run all tests within the `tests` directory. Please ensure all tests pass before submitting a pull request.

## Pull Request Process

1. Create a new branch for your feature or bugfix:
   ```sh
   git checkout -b your-feature-branch
   ```
2. Make your changes and commit them with clear commit messages.
3. Push the branch to your fork and open a pull request against the `main` branch of this repository.
4. Ensure your pull request description explains the purpose of the changes.
5. Address any review comments and update the pull request as needed.

