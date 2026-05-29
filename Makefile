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

RELEASE	?= minor
MANAGER	?= uv

.PHONY: release
# target: release - Bump version
release:
	@echo "Starting release process (bumping $(RELEASE) version)..."
	@git checkout main
	@git pull
	@git checkout develop
	@git pull
	@echo "Bumping version and creating release commit and tag..."
	@uvx bump-my-version bump $(RELEASE)
	@echo "Version bumped to `$(MANAGER) version --short`."
	@$(MANAGER) lock
	@echo "Committing version bump and creating tag..."
	@VERSION=`$(MANAGER) version --short`; \
		{ \
			printf 'build(release): %s\n\n' "$$VERSION"; \
			printf 'Changes:\n\n'; \
			git log --oneline --pretty=format:'%s [%an]' main..develop | grep -Evi 'github|^Merge' || true; \
		} | git commit -a -F -
	@echo "Merging changes between branches..."
	@git checkout main
	@git merge --ff-only develop
	@VERSION=`$(MANAGER) version --short`; \
		git push origin main; \
		git tag -a "$$VERSION" -m "$$VERSION"; \
		git push origin "$$VERSION"
	@git checkout develop
	@git merge --ff-only main
	@git push origin develop
	@echo "Release process complete for `$(MANAGER) version --short`"

.PHONY: minor
minor: release

.PHONY: patch
patch:
	make release RELEASE=patch

.PHONY: major
major:
	make release RELEASE=major

version v:
	uv version --short

.PHONY: setup-postgres
setup-postgres:
	docker exec -i postgres psql -U postgres < tests/assets/init-postgres.sql
