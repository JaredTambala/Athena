#!/bin/bash

#This script runs automatically all the needed commands for setting up pipenv and pre-commit hooks

#1. Install pipenv
pip install --user pipenv

#2. Set up version python for pipenv
pipenv --python 3.11

#3. Install all dev dependencies for pipenv
pipenv install --dev

#4. Install pre-commit hooks
pipenv run  pre-commit install --hook-type pre-commit --hook-type pre-push
