
import stringdist
def get_levelstein(median_rent_file, zone_file, updated_zone_file):
	median_lines = open(median_rent_file, "r").readlines()
	zone_lines = open(zone_file, "r").readlines()
	updated_zone_pointer = open(updated_zone_file, "w")
	zone_names = []
	for line in zone_lines:
		tuples = line.split(",")
		zone_names.append(tuples[2].strip("\""))

	median_names = []

	for line in median_lines:
		tuples = line.split(",")
		median_names.append(tuples[0].strip("\""))

	# print median_names
	#get closest name in median to zones
	closest_zone_name = {}
	for zone_name in zone_names:
		min_distance = 100000000000
		min_string = None
		for median_name in median_names:
			if zone_name in median_name or median_name in zone_name:
				min_string = median_name
				min_distance = 0
				break
			temp_dist = stringdist.levenshtein(zone_name, median_name)
			if temp_dist < min_distance:
				min_distance = temp_dist
				min_string = median_name

		if min_distance < len(zone_name)*0.8:
			closest_zone_name[zone_name] = min_string
		else:
			closest_zone_name[zone_name] = "NONE"

	for line in zone_lines:
		tuples = line.split(",")
		mapped_zone_name = closest_zone_name[tuples[2].strip("\"")]
		tuples[2] = mapped_zone_name
		updated_zone_pointer.write(",".join(tuples))

if __name__== "__main__":
  get_levelstein("./median_rent.csv", "./zone_lookup.csv", "./zone_lookup_mapped.csv")