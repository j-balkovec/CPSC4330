line = "0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal."

# print the number of columns in the line with their values
print(len(line.split(',')))

for i, value in enumerate(line.split(',')):
    print(f"{i}: {value}")
    