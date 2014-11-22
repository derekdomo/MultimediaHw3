all:    
	chmod a+rwx `pwd`
	python tensor.py --file `pwd`/data/LBNL-sdp.dat --dest_dir `pwd`/tensor 

install:
	sudo apt-get install python-psycopg2

all.tar:
	tar -zcvf graphminer.tar.gz *.txt makefile *.py matlab output doc

clean:
	rm *.pyc


