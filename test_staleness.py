import os

file_name = 'staleness_results_2.txt'

for j in range(1):
    num_ones = 0
    num_twos = 0
    num_threes = 0
    num_fours = 0
    num_fives = 0
    num_sixes = 0
    for i in range(1000):
        print("Experiment: {}".format(i+1))
        stream = os.popen('mix test')
        output = stream.read()
        for line in output.split('\n'):
            if line[:6] == "GET 5:":
                ans = int(line[7])
                if ans == 1:
                    num_ones += 1
                elif ans == 2:
                    num_twos += 1
                elif ans == 3:
                    num_threes += 1
                elif ans == 4:
                    num_fours += 1
                elif ans == 5:
                    num_fives += 1
                elif ans == 6:
                    num_sixes += 1

    f = open(file_name, 'a')
    f.write("Number of ones: {} Number of twos: {} Number of threes: {} Number of fours: {} Number of fives: {} Number of sixes: {}\n".format(num_ones, num_twos, num_threes, num_fours, num_fives, num_sixes))
    f.close()
    print("Number of ones: {} Number of twos: {} Number of threes: {} Number of fours: {} Number of fives: {} Number of sixes: {}\n".format(num_ones, num_twos, num_threes, num_fours, num_fives, num_sixes))
