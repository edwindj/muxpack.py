import muxpack as mp

ms = mp.read_multiplexseries("example/data")
print(ms)

ms.add_filter(periods=[2020], layers={"household": None, "school": None})
print(ms)
