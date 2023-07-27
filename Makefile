CODE := pg_save

lint:
	poetry run pylint $(CODE)

format:
	poetry run isort $(CODE)
	poetry run black $(CODE)

install:
	python -m pip install .

install-dev:
	$(info Use "poetry shell" command to activate virtual environment)
	poetry install --with dev

install-dev-pip:
	python -m pip install . --

build:
	poetry build

clean:
	rm -rf ./dist

update-pypi: clean build
	poetry publish

install-from-build:
	python -m wheel install dist/pg_save-*.whl
