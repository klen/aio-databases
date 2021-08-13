VIRTUAL_ENV ?= env

$(VIRTUAL_ENV): setup.cfg
	@[ -d $(VIRTUAL_ENV) ] || python -m venv $(VIRTUAL_ENV)
	@$(VIRTUAL_ENV)/bin/pip install -e .[tests]
	@touch $(VIRTUAL_ENV)

.PHONY: test
# target: test - Runs tests
t test: $(VIRTUAL_ENV)
	docker start postgres mysql
	@$(VIRTUAL_ENV)/bin/pytest --log-format "%(levelname)s %(message)s" tests

.PHONY: mypy
# target: mypy - Code checking
mypy: $(VIRTUAL_ENV)
	@$(VIRTUAL_ENV)/bin/mypy aio_databases


VERSION	?= minor

.PHONY: version
version: $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/pip install bump2version
	$(VIRTUAL_ENV)/bin/bump2version $(VERSION)
	git checkout master
	git pull
	git merge develop
	git checkout develop
	git push origin develop master
	git push --tags

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

