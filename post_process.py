import os
import sys


schedular_delay = []
task_deserialization = []
hdfs_read =[]
local_read_wait = []
network_Wait = []
data_deserilization = []
output_write_wait = []
compute = []
gc = []

identifier_mapping ={
3:"COMPUTE",  
4:"GC",
6:"SCHEDULAR_DELAY",
8:"TASK_DESERIAL",
7: "HDFS_READ",
1:"LOCAL_READ_WAIT",
2: "NETWORK_WAIT",
9:"DATA_DESERIALIZATION",
5: "OUTPUT_WRITE_WAIT"
}
COMPUTE = 3
GC = 4
SCHEDULAR_DELAY = 6
TASK_DESERIAL = 8
HDFS_READ = 7
LOCAL_READ_WAIT = 1
NETWORK_WAIT = 2
DATA_DESERIALIZATION = 9
OUTPUT_WRITE_WAIT = 5

transformation_mapping = {
COMPUTE: compute, GC: gc, 
SCHEDULAR_DELAY: schedular_delay, TASK_DESERIAL:task_deserialization, 
HDFS_READ: hdfs_read, LOCAL_READ_WAIT: local_read_wait, NETWORK_WAIT:network_Wait,
DATA_DESERIALIZATION: data_deserilization, OUTPUT_WRITE_WAIT: output_write_wait
}
#set arrow from 0,0 to 203,0 ls 6 nohead
transform = lambda x:float(x)
for line in sys.stdin:
	line = line.rstrip(os.linesep)
	if "set arrow from" in line:
		chunks = line.split()
		#print chunks
		x_start,y_start = map(transform ,chunks[3].split(","))
		x_end, y_end = map(transform, chunks[5].split(","))
		identifier = int(chunks[7])

		#print x_start, y_start, x_end, y_end
		time_spent = x_end - x_start
		data_list = transformation_mapping[identifier]
		data_list.append(time_spent)
		transformation_mapping[identifier] = data_list
summation = lambda l:sum(l)
IO = [transformation_mapping[HDFS_READ], transformation_mapping[DATA_DESERIALIZATION], transformation_mapping[OUTPUT_WRITE_WAIT], transformation_mapping[LOCAL_READ_WAIT]]
IO_FINAL = sum(map(summation, IO))
CPU = [transformation_mapping[COMPUTE]]
CPU_FINAL = sum(map(summation, CPU))
NETWORK = [transformation_mapping[NETWORK_WAIT], transformation_mapping[SCHEDULAR_DELAY]]
NETWORK_FINAL = sum(map(summation, NETWORK))
OVERHEADS = [transformation_mapping[GC], transformation_mapping[TASK_DESERIAL]]
OVERHEADS_FINAL = sum(map(summation, OVERHEADS))
print IO_FINAL, CPU_FINAL, NETWORK_FINAL, OVERHEADS_FINAL

# Now lets plot
import numpy as np
import matplotlib.pyplot as plt


N = 2
ind = np.arange(N)    
width = 0.3     

p1 = plt.bar(ind, [IO_FINAL,0], width=width, color='r')
p2 = plt.bar(ind, [CPU_FINAL,0],width= width, color='y')

p3 = plt.bar(ind, [NETWORK_FINAL,0], width=width, color='b')
p4 = plt.bar(ind, [OVERHEADS_FINAL,0],width= width, color='g')
plt.ylabel('Time')
plt.title('CX RUN TIMES')
plt.xticks(ind+width/2., ('EDISON',) )

plt.legend( (p1[0], p2[0], p3[0], p4[0]), ('IO', 'CPU', 'NETWORK','OVERHEADS') )
plt.show()
