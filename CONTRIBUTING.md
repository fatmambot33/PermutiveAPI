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
3. **Install dependencies**
   ```sh
   pip install -r requirements.txt
   ```
4. **Install the package in editable mode** (optional)
   ```sh
   pip install -e .
   ```

## Code Style and Linting

This repository does not enforce a specific code style, but we recommend using [Black](https://github.com/psf/black) for formatting and [Flake8](https://flake8.pycqa.org/) for linting.

Run the linters before committing changes:
```sh
black .
flake8
```

## Pull Request Process

1. Create a new branch for your feature or bugfix:
   ```sh
   git checkout -b your-feature-branch
   ```
2. Make your changes and commit them with clear commit messages.
3. Push the branch to your fork and open a pull request against the `main` branch of this repository.
4. Ensure your pull request description explains the purpose of the changes.
5. Address any review comments and update the pull request as needed.

