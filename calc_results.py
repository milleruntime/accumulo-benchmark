import sys

def usage():
    print "calc_results.py <worse-results-file> <better-results-file>"
    sys.exit()

# read in lines from filename and remove end lines
def read_lines_from_file(filename):
    with open(filename) as f:
        lines = f.readlines()
    lines = [x.strip() for x in lines]
    return lines

# grabs the final results at the end of the file
def grab_lines(file_lines):
    found = False
    results = []
    for i, val in enumerate(file_lines):
        if found:
            results.append(val)
        if "Units" in val:
            found = True
    if not found:
        print "Brah didn't find the summary in your results file"
        sys.exit()
    return results

# create dict from the proper columns (1st and 4th) from the results
def create_result_dict(results):
    r = []
    for i in results:
        s = i.split()
        r.append([s[0], s[3]])
    return r

if len(sys.argv) < 2:
    usage()

fname1 = str(sys.argv[1])
fname2 = str(sys.argv[2])

r1 = create_result_dict(grab_lines(read_lines_from_file(fname1)))
r2 = create_result_dict(grab_lines(read_lines_from_file(fname2)))

#print "r1=" + r1
#print "r2=" + r2

if len(r1) != len(r2):
    print "Brah your results differ.  Check your files."
    sys.exit()

for x in range(len(r1)):
    dict1 = r1[x]
    dict2 = r2[x]
    percent = (float(dict2[1]) - float(dict1[1])) / float(dict1[1]) * 100
    output = "Test: {} showed {:.1f}% increase in ops/s".format(dict1[0], percent)
    print output

