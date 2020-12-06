import os

num_ones = 0

for i in range(100):
    print("Experiment: {}".format(i+1))
    stream = os.popen('mix test')
    output = stream.read()
    for line in output.split('\n'):
        if line[:6] == "GET 2:" and int(line[7]) == 1:
            num_ones += 1

print("Number of ones: {}".format(num_ones))