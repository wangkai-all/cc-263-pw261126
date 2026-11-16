.PHONY: run test accept recover clean

DATA ?= var
PORT ?= 8080

run:
	python3 -m sandboxlab --data $(DATA) --port $(PORT)

test:
	python3 -m unittest discover -s tests -v

accept:
	python3 scripts/acceptance.py

recover:
	python3 -m sandboxlab --data $(DATA) --recover

clean:
	rm -rf $(DATA)
