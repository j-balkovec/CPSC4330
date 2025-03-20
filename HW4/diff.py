import json
import math

tolerance = 1e-6

my_output = [
    (34.526250331519186, -118.27776504893302),
    (33.80951731183417, -116.68055217165534),
    (38.29028931926984, -121.11155741846274),
    (35.08557960553461, -112.57564496861943),
    (44.234490402067024, -121.80042938536774)
]

expected_output = [
    (43.97544955558915, -121.40498164360471),
    (34.49209866867575, -118.21258003843369),
    (35.0852504610273, -112.57489358662798),
    (38.17337679610569, -121.21451034445965),
    (33.76035813785657, -116.56967965470706)
]

expected_output_2 = [
    (43.97544955558931, -121.40498164360626),
    (34.49209866867549, -118.2125800384333),
    (35.085250461027556, -112.57489358662946),
    (38.17337679610465, -121.2145103444581),
    (33.760358137856706, -116.56967965470677)
]

def calculate_mismatch(actual, expected, t=tolerance):
    x_mismatch = math.isclose(actual[0], expected[0], abs_tol=t)
    y_mismatch = math.isclose(actual[1], expected[1], abs_tol=t)
    
    return x_mismatch, y_mismatch, abs(actual[0] - expected[0]), abs(actual[1] - expected[1])

comparison_result = []

sorted_my_output = sorted(my_output, key=lambda x: (x[0], x[1]))
sorted_expected_output = sorted(expected_output, key=lambda x: (x[0], x[1]))
sorted_expected_output_2 = sorted(expected_output_2, key=lambda x: (x[0], x[1]))


for my, expected, expected2 in zip(sorted_my_output, sorted_expected_output, sorted_expected_output_2):    

    x_mismatch, y_mismatch, x_diff, y_diff = calculate_mismatch(my, expected)
    x_mismatch_2, y_mismatch_2, x_diff_2, y_diff_2 = calculate_mismatch(my, expected2)

    comparison_entry = {
        "x_mismatch": x_mismatch,
        "y_mismatch": y_mismatch,
        "x_diff": x_diff,
        "y_diff": y_diff,
        "my_output": my,
        "expected_output": expected,
        "": "-------------------------",
        "x_mismatch_2": x_mismatch_2,
        "y_mismatch_2": y_mismatch_2,
        "x_diff_2": x_diff_2,
        "y_diff_2": y_diff_2,
        "my_output": my,
        "expected_output_2": expected2
    }
    
    comparison_result.append(comparison_entry)

print(json.dumps(comparison_result, indent=4))