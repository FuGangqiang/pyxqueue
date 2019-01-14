release: clear
	python3 setup.py sdist bdist_wheel
	twine upload -r pypi dist/*

clear:
	rm -rf build dist *.egg-info
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

