install:
	python -m pip install .

clean:
	rm -rf ./build ./dist ./pg_save.egg-info

udpate-pypi: clean
	python -m build . --no-isolation
	python -m twine upload  dist/*

install-from-build:
	python -m wheel install dist/pg_save-*.whl