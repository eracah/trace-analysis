import os
import sys
import matplotlib.pylab as plt
schedular_delay = []
task_deserialization = []
hdfs_read =[]
local_read_wait = []
network_Wait = []
data_deserilization = []
output_write_wait = []
compute = []
gc = []
"""
    plot_file.write("plot -1 ls 6 title 'Scheduler delay',\\\n")
    plot_file.write(" -1 ls 8 title 'Task deserialization', -1 ls 7 title 'HDFS read',\\\n")
    plot_file.write("-1 ls 1 title 'Local read wait',\\\n")
    plot_file.write("-1 ls 2 title 'Network wait', -1 ls 3 title 'Compute', \\\n")
    plot_file.write("-1 ls 9 title 'Data (de)serialization', -1 ls 4 title 'GC', \\\n")
    plot_file.write("-1 ls 5 title 'Output write wait'\\\n")
"""
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
#menMeans   = (20, 35, 30, 35, 27)
#womenMeans = (25, 32, 34, 20, 25)
#menStd     = (2, 3, 4, 1, 2)
#womenStd   = (3, 5, 2, 3, 3)
ind = np.arange(N)    # the x locations for the groups
print ind
width = 0.3      # the width of the bars: can also be len(x) sequence

p1 = plt.bar(ind, [IO_FINAL,0], width=width, color='r')
p2 = plt.bar(ind, [CPU_FINAL,0],width= width, color='y')

p3 = plt.bar(ind, [NETWORK_FINAL,0], width=width, color='b')
p4 = plt.bar(ind, [OVERHEADS_FINAL,0],width= width, color='g')
plt.ylabel('Time')
plt.title('CX RUN TIMES')
plt.xticks(ind+width/2., ('EDISON',) )

plt.legend( (p1[0], p2[0], p3[0], p4[0]), ('IO', 'CPU', 'NETWORK_FINAL','OVERHEADS') )
plt.show()
"""
N = 2
menMeans   = (20,0)
womenMeans = (25,0)
ind = np.arange(N)    # the x locations for the groups
width = 0.35       # the width of the bars: can also be len(x) sequence

p1 = plt.bar(ind, menMeans,   width, color='r')
p2 = plt.bar(ind, womenMeans, width, color='y',
             bottom=menMeans)


plt.ylabel('Scores')
plt.title('Scores by group and gender')
#plt.xticks(ind+width/2., ('G1',) )
#plt.yticks(np.arange(0,81,10))
#plt.legend( (p1[0], p2[0]), ('Men', 'Women') )

plt.show()

"""