SPARKSHELL = bin/spark-shell
SBT = build/sbt
SBT_FLAGS = -Pscala-2.13

FLAGS = .flags

.PHONY: cleanbuild cleandata cleanflags cleanvenv cleangraphs cleanall compile interactive data

all: setup compile package data graphs

cleanbuild:
	$(SBT) $(SBT_FLAGS) clean

cleandata:
	rm -rf scripts/timing/output

cleanflags:
	rm -rf .flags/

cleanvenv:
	rm -rf ./scripts/plotting/.venv/

cleangraphs:
	rm -rf scripts/plotting/output

cleanall:
	$(MAKE) cleanflags cleanvenv cleangraphs cleanbuild

$(FLAGS):
	mkdir $(FLAGS)

$(FLAGS)/setup: | $(FLAGS)
	./dev/change-scala-version.sh 2.13
	cd scripts/plotting && python3 -m venv .venv &&\
		source .venv/bin/activate &&\
		python3 -m pip install -r requierments.txt
	touch $@

setup: $(FLAGS)/setup

compile: setup
	$(SBT) $(SBT_FLAGS) compile

package: setup
	$(SBT) $(SBT_FLAGS) package

interactive: setup
	$(SBT) $(SBT_FLAGS)

data: setup
	cd scripts/timing && ./tpchBench.sh

graph: setup
	cd ./scripts/plotting/ &&\
		mkdir output &&\
		source .venv/bin/activate &&\
		python3 plotting.py
