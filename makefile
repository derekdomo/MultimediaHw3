##############################################################
# Authors: Christos Faloutsos, Chenying Hou  and Peixin Zheng
# Date: Oct. 2014
##############################################################
# this makefile is designed to make grading easier:
#      make
# should run all the code for all the questions
##############################################################

all: q1-flag q2-flag q3-flag q4-flag 


q1-flag:
	echo " "
	echo " working on q1 ----- "
	cd q1; make

q2-flag:
	echo " "
	echo " working on q2 ----- "
	cd q2; make

q3-flag:
	echo " "
	echo " working on q3 ----- "
	cd q3; make

q4-flag:
	echo " "
	echo " working on q4 ----- "
	cd q4; make

clean:
	cd q1; make clean
	cd q2; make clean
	cd q3; make clean
	cd q4; make clean

spotless: clean
	\rm -f all.tar
	\rm -rf TST


all.tar: clean
	tar cvf all.tar makefile q1 q2 q3 q4
