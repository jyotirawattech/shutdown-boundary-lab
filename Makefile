PYTHON ?= python3

.PHONY: test demo verify

test:
	$(PYTHON) -W error::ResourceWarning -m unittest discover -s tests -v

demo:
	$(PYTHON) -m shutdown_lab

verify: test
	$(PYTHON) -c 'import asyncio,json,pathlib; from shutdown_lab.experiment import run_comparison,stable_result; actual=stable_result(asyncio.run(run_comparison())); expected=json.loads(pathlib.Path("results/worked.json").read_text()); assert actual==expected, "fresh controlled result differs from worked result"; print("Fresh loopback comparison matches worked result")'
