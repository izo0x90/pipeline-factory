.PHONY: install-dev
install-dev:
	uv pip install -e ".[dev]" 

.PHONY: build
build:
	python -m build	
