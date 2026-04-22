VIRTUAL_ENV ?= .venv

$(VIRTUAL_ENV): uv.lock pyproject.toml
	@uv sync
	@uv run pre-commit install
	@touch $(VIRTUAL_ENV)

.PHONY: test t
test t: $(VIRTUAL_ENV)
	docker start postgres mysql
	@uv run pytest tests

.PHONY: types
types: $(VIRTUAL_ENV)
	@uv run pyrefly check

# ==============
#  Bump version
# ==============

VPART	?= minor
MAIN_BRANCH = master
STAGE_BRANCH = develop

.PHONY: release
# target: release - Bump version
release:
	git checkout $(MAIN_BRANCH)
	git pull
	git checkout $(STAGE_BRANCH)
	git pull
	uvx bump-my-version bump $(VPART)
	uv lock
	@VERSION="$$(uv version --short)"; \
		{ \
			printf 'build(release): %s\n\n' "$$VERSION"; \
			printf 'Changes:\n\n'; \
			git log --oneline --pretty=format:'%s [%an]' $(MAIN_BRANCH)..$(STAGE_BRANCH) | grep -Evi 'github|^Merge' || true; \
		} | git commit -a -F -
	git checkout $(MAIN_BRANCH)
	git merge $(STAGE_BRANCH)
	git checkout $(STAGE_BRANCH)
	git merge $(MAIN_BRANCH)
	@VERSION="$$(uv version --short)"; \
		git tag -a "$$VERSION" -m "$$VERSION"; \
		git push --atomic origin $(STAGE_BRANCH) $(MAIN_BRANCH) "refs/tags/$$VERSION"
	@echo "Release process complete for `uv version --short`"

.PHONY: minor
minor: release

.PHONY: patch
patch:
	make release VPART=patch

.PHONY: major
major:
	make release VPART=major

version v:
	uv version --short

.PHONY: setup-postgres
setup-postgres:
	docker exec -i postgres psql -U postgres < tests/assets/init-postgres.sql
