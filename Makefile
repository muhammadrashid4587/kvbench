.PHONY: test bench clean

test:
	python3 -m unittest discover -s tests -v

bench:
	python3 -m kvbench.cli --host 127.0.0.1 --port 6379 --ops 20000 --clients 4 --baseline --output report.md

clean:
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	rm -f report.md
