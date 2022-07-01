SPARKSHELL = bin/spark-shell
SBT = build/sbt
SBT_FLAGS = -Pscala-2.13

.PHONY: cleanbuild cleandata cleanflags cleanvenv cleangraphs cleanall compile interactive data

all: .compileSetup .package .data graph

cleanbuild:
	$(SBT) $(SBT_FLAGS) clean

cleandata:
	rm -rf ./timing/output

cleanflags:
	rm -rf .data .package .compileSetup .graphSetup

cleanvenv:
	rm -rf ./plotting/.venv/

cleangraphs:
	rm -rf ./plotting/output

cleanall:
	$(MAKE) cleanflags cleanvenv cleangraphs cleanbuild

.compileSetup:
	./dev/change-scala-version.sh 2.13
	touch $@

.graphSetup:
	cd ./plotting && python3 -m venv .venv &&\
		source .venv/bin/activate &&\
		python3 -m pip install -r requirements.txt
	touch $@

compile: .compileSetup
	$(SBT) $(SBT_FLAGS) compile

.package: .compileSetup
	$(SBT) $(SBT_FLAGS) package
	touch $@

package:
	$(MAKE) .package

interactive: .compileSetup
	$(SBT) $(SBT_FLAGS)

.data: .package
	cd ./timing && ./tpchBench.sh
	touch $@

data:
	$(MAKE) .data

graph: .data .graphSetup
	cd ./plotting/ &&\
		mkdir -p output &&\
		source .venv/bin/activate &&\
		python3 plotting.py
