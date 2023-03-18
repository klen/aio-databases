VIRTUAL_ENV ?= .venv

$(VIRTUAL_ENV): pyproject.toml
	@poetry install --with dev
	@poetry run pre-commit install --hook-type pre-push
	@touch $(VIRTUAL_ENV)

.PHONY: test
# target: test - Runs tests
t test: $(VIRTUAL_ENV)
	docker start postgres mysql
	@poetry run pytest --log-format "%(levelname)s %(message)s" tests

.PHONY: mypy
# target: mypy - Code checking
mypy: $(VIRTUAL_ENV)
	@poetry run mypy


VERSION	?= minor

.PHONY: version
version: $(VIRTUAL_ENV)
	@$(eval VFROM := $(shell poetry version -s))
	@poetry version $(VERSION)
	@git commit -am "Bump version $(VFROM) → `poetry version -s`"
	@git tag `poetry version -s`
	@git checkout master
	@git merge develop
	@git checkout develop
	@git push origin develop master
	@git push --tags

.PHONY: minor
minor:
	make version VERSION=minor

.PHONY: patch
patch:
	make version VERSION=patch

.PHONY: major
major:
	make version VERSION=major

.PHONY: clean
# target: clean - Display callable targets
clean:
	rm -rf build/ dist/ docs/_build *.egg-info
	find $(CURDIR) -name "*.py[co]" -delete
	find $(CURDIR) -name "*.orig" -delete
	find $(CURDIR) -name "__pycache__" | xargs rm -rf
