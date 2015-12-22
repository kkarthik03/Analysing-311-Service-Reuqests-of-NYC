from subprocess import check_output
out = check_output(["spark-submit", "tagger2.py"])
print out