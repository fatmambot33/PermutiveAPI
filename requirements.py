import sys
import pkg_resources
import subprocess
import sys
import os


def install_package(package):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", package])


def install_requirements():
    requirements_file = "requirements.txt"
    if os.path.exists(requirements_file):
        with open(requirements_file, "r") as f:
            required_packages = [
                line.strip().split("#")[0].strip() for line in f.readlines()
            ]

        installed_packages = {
            pkg.key: pkg.version for pkg in pkg_resources.working_set}

        for required_package in required_packages:
            if not required_package:  # Skip empty lines
                continue
            pkg = pkg_resources.Requirement.parse(required_package)
            if (
                pkg.key not in installed_packages
                or pkg_resources.parse_version(installed_packages[pkg.key])
                not in pkg.specifier
            ):
                install_package(str(pkg))


if __name__ == "__main__":
    install_requirements()
