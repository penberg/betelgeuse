.PHONY: ci

ci:
	npx --yes @redwoodjs/agent-ci run --workflow .github/workflows/ci.yml
