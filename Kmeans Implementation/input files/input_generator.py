from random import randint

f = open('big_input_2D','w')
for i in range(pow(10,8)):
	f.write(str(0))
	for i in range(2):
		f.write("\t")
		f.write(str(randint(1,100)))
	f.write("\n")
f.close()

