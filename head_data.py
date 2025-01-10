import pandas as pd
from pathlib import Path
from tqdm import tqdm
import re

# https://cli.fusio.net/cli/climate_data/webdata/dly(stationid).zip
# raw_data = "/home/anderthel/Desktop/Programing/ireland/data/raw_data"
# head_data = "/home/anderthel/Desktop/Programing/ireland/data/head_data"
# body_data = "/home/anderthel/Desktop/Programing/ireland/data/body_data"
data = "/home/anderthel/Desktop/Programing/ireland/data/raw_split/hourly"
# data = "/home/anderthel/Desktop/Programing/ireland/data/raw_split/daily"
outpath = "/home/anderthel/Desktop/Programing/ireland/data/merged/hourly"


def main(data, outpath):
	# convert path to object
	path = Path(data)
	# get paths of all files
	files = path.rglob("header.csv")

	lines_parsed = {}
	for file in tqdm(list(files), desc="        head"):
		# pd.read_csv(file, encoding="us-ascii", sep=",", na_values=" ")
		with open(file, "r", encoding="us-ascii") as f:
			lines_raw = f.readlines()
			lines_temp = {}
			stationid = file.parent.name.strip("dly").strip("hly").strip("mly")
			for line in lines_raw:
				line = line.strip()
				if line == "":
					pass
				elif line.startswith("Station Name: "):
					name = line.strip("Station Name: ")
					lines_temp["name"] = name
				elif line.startswith("Station Height: "):
					if "height" not in lines_temp:
						lines_temp["height"] = line.strip("Station Height: ")
					else:
						print(f"Error value exists twice in {file} - height")
						raise Exception
				elif line.startswith("Latitude:"):
					lat, lon = line.split("  ,Longitude: ", 1)
					if "lat" not in lines_temp:
						lines_temp["lat"] = lat.strip("Latitude:")
					else:
						print(f"Error value exists twice in {file} - date")
						raise Exception
					if "lon" not in lines_temp:
						lines_temp["lon"] = lon
					else:
						print(f"Error value exists twice in {file} - date")
						raise Exception
				elif re.search(r"^date:\s*-\s*", line):
					if "date" not in lines_temp:
						lines_temp["date"] = re.sub(r"^date:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - date")
						raise Exception
				elif re.search(r"^rain:\s*-\s*", line):
					if "rain" not in lines_temp:
						lines_temp["rain"] = re.sub(r"^rain:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - rain")
						raise Exception
				elif re.search(r"^maxtp:\s*-\s*", line):
					if "maxtp" not in lines_temp:
						lines_temp["maxtp"] = re.sub(r"^maxtp:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - maxtp")
						raise Exception
				elif re.search(r"^mintp:\s*-\s*", line):
					if "mintp" not in lines_temp:
						lines_temp["mintp"] = re.sub(r"^mintp:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - mintp")
						raise Exception
				elif re.search(r"^gmin:\s*-\s*", line):
					if "gmin" not in lines_temp:
						lines_temp["gmin"] = re.sub(r"^gmin:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - gmin")
						raise Exception
				elif re.search(r"^soil:\s*-\s*", line):
					if "soil" not in lines_temp:
						lines_temp["soil"] = re.sub(r"^soil:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - soil")
						raise Exception
				elif re.search(r"^wdsp:\s*-\s*", line):
					if "wdsp" not in lines_temp:
						lines_temp["wdsp"] = re.sub(r"^wdsp:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - wdsp")
						raise Exception
				elif re.search(r"^hm:\s*-\s*", line):
					if "hm" not in lines_temp:
						lines_temp["hm"] = re.sub(r"^hm:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - hm")
						raise Exception
				elif re.search(r"^ddhm:\s*-\s*", line):
					if "ddhm" not in lines_temp:
						lines_temp["ddhm"] = re.sub(r"^ddhm:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - ddhm")
						raise Exception
				elif re.search(r"^hg:\s*-\s*", line):
					if "hg" not in lines_temp:
						lines_temp["hg"] = re.sub(r"^hg:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - hg")
						raise Exception
				elif re.search(r"^cbl:\s*-\s*", line):
					if "cbl" not in lines_temp:
						lines_temp["cbl"] = re.sub(r"^cbl:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - cbl")
						raise Exception
				elif re.search(r"^sun:\s*-\s*", line):
					if "sun" not in lines_temp:
						lines_temp["sun"] = re.sub(r"^sun:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - sun")
						raise Exception
				elif re.search(r"^g_rad:\s*-\s*", line):
					if "g_rad" not in lines_temp:
						lines_temp["g_rad"] = re.sub(r"^g_rad:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - g_rad")
						raise Exception
				elif re.search(r"^glorad:\s*-\s*", line):
					if "glorad" not in lines_temp:
						lines_temp["glorad"] = re.sub(r"^glorad:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - glorad")
						raise Exception
				elif re.search(r"^pe:\s*-\s*", line):
					if "pe" not in lines_temp:
						lines_temp["pe"] = re.sub(r"^pe:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - pe")
						raise Exception
				elif re.search(r"^evap:\s*-\s*", line):
					if "evap" not in lines_temp:
						lines_temp["evap"] = re.sub(r"^evap:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - evap")
						raise Exception
				elif re.search(r"^smd_wd:\s*-\s*", line):
					if "smd_wd" not in lines_temp:
						lines_temp["smd_wd"] = re.sub(r"^smd_wd:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - smd_wd")
						raise Exception
				elif re.search(r"^smd_md:\s*-\s*", line):
					if "smd_md" not in lines_temp:
						lines_temp["smd_md"] = re.sub(r"^smd_md:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - smd_md")
						raise Exception
				elif re.search(r"^smd_pd:\s*-\s*", line):
					if "smd_pd" not in lines_temp:
						lines_temp["smd_pd"] = re.sub(r"^smd_pd:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - smd_pd")
						raise Exception
				elif re.search(r"^ind:\s*-\s*", line):
					if "ind" not in lines_temp:
						lines_temp["ind"] = re.sub(r"^ind:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - ind")
						raise Exception
				elif re.search(r"^maxt:\s*-\s*", line):
					if "maxt" not in lines_temp:
						lines_temp["maxt"] = re.sub(r"^maxt:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - maxt")
						raise Exception
				elif re.search(r"^mint:\s*-\s*", line):
					if "mint" not in lines_temp:
						lines_temp["mint"] = re.sub(r"^mint:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - mint")
						raise Exception
				elif re.search(r"^temp:\s*-\s*", line):
					if "temp" not in lines_temp:
						lines_temp["temp"] = re.sub(r"^temp:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - temp")
						raise Exception
				elif re.search(r"^wetb:\s*-\s*", line):
					if "wetb" not in lines_temp:
						lines_temp["wetb"] = re.sub(r"^wetb:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - wetb")
						raise Exception
				elif re.search(r"^dewpt:\s*-\s*", line):
					if "dewpt" not in lines_temp:
						lines_temp["dewpt"] = re.sub(r"^dewpt:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - dewpt")
						raise Exception
				elif re.search(r"^rhum:\s*-\s*", line):
					if "rhum" not in lines_temp:
						lines_temp["rhum"] = re.sub(r"^rhum:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - rhum")
						raise Exception
				elif re.search(r"^vappr:\s*-\s*", line):
					if "vappr" not in lines_temp:
						lines_temp["vappr"] = re.sub(r"^vappr:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - vappr")
						raise Exception
				elif re.search(r"^msl:\s*-\s*", line):
					if "msl" not in lines_temp:
						lines_temp["msl"] = re.sub(r"^msl:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - msl")
						raise Exception
				elif re.search(r"^wddir:\s*-\s*", line):
					if "wddir" not in lines_temp:
						lines_temp["wddir"] = re.sub(r"^wddir:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - wddir")
						raise Exception
				elif re.search(r"^w:\s*-\s*", line):
					if "w" not in lines_temp:
						lines_temp["w"] = re.sub(r"^w:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - w")
						raise Exception
				elif re.search(r"^ww:\s*-\s*", line):
					if "ww" not in lines_temp:
						lines_temp["ww"] = re.sub(r"^ww:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - ww")
						raise Exception
				elif re.search(r"^vis:\s*-\s*", line):
					if "vis" not in lines_temp:
						lines_temp["vis"] = re.sub(r"^vis:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - vis")
						raise Exception
				elif re.search(r"^clht:\s*-\s*", line):
					if "clht" not in lines_temp:
						lines_temp["clht"] = re.sub(r"^clht:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - clht")
						raise Exception
				elif re.search(r"^clamt:\s*-\s*", line):
					if "clamt" not in lines_temp:
						lines_temp["clamt"] = re.sub(r"^clamt:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - clamt")
						raise Exception
				elif re.search(r"^year:\s*-\s*", line):
					if "year" not in lines_temp:
						lines_temp["year"] = re.sub(r"^year:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - year")
						raise Exception
				elif re.search(r"^month:\s*-\s*", line):
					if "month" not in lines_temp:
						lines_temp["month"] = re.sub(r"^month:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - month")
						raise Exception
				elif re.search(r"^month:\s*-\s*", line):
					if "month" not in lines_temp:
						lines_temp["month"] = re.sub(r"^month:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - month")
						raise Exception
				elif re.search(r"^meant:\s*-\s*", line):
					if "meant" not in lines_temp:
						lines_temp["meant"] = re.sub(r"^meant:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - meant")
						raise Exception
				elif re.search(r"^mnmax:\s*-\s*", line):
					if "mnmax" not in lines_temp:
						lines_temp["mnmax"] = re.sub(r"^mnmax:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - mnmax")
						raise Exception
				elif re.search(r"^mnmin:\s*-\s*", line):
					if "mnmin" not in lines_temp:
						lines_temp["mnmin"] = re.sub(r"^mnmin:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - mnmin")
						raise Exception
				elif re.search(r"^mxgt:\s*-\s*", line):
					if "mxgt" not in lines_temp:
						lines_temp["mxgt"] = re.sub(r"^mxgt:\s*-\s*", "", line)
					else:
						print(f"Error value exists twice in {file} - mxgt")
						raise Exception
				else:
					print(f"Error value not setup {file} - {line}")
					raise Exception
			lines_parsed[stationid] = lines_temp

	# for file in lines_parsed:
		if "date" in lines_parsed[stationid] and re.search(r"Date and Time \(utc\)", lines_parsed[stationid]["date"]):
			lines_parsed[stationid].pop("date")
		if "rain" in lines_parsed[stationid] and re.search(r"Precipitation Amount \(mm\)", lines_parsed[stationid]["rain"]):
			lines_parsed[stationid].pop("rain")
		if "maxtp" in lines_parsed[stationid] and re.search(r"Maximum Air Temperature \(C\)", lines_parsed[stationid]["maxtp"]):
			lines_parsed[stationid].pop("maxtp")
		if "mintp" in lines_parsed[stationid] and re.search(r"Minimum  Air Temperature \(C\)", lines_parsed[stationid]["mintp"]):
			lines_parsed[stationid].pop("mintp")
		if "gmin" in lines_parsed[stationid] and re.search(r"(09utc |)Grass Minimum Temperature \(C\)", lines_parsed[stationid]["gmin"]):
			lines_parsed[stationid].pop("gmin")
		if "soil" in lines_parsed[stationid] and re.search(r"Mean 10cm soil temperature \(C\)", lines_parsed[stationid]["soil"]):
			lines_parsed[stationid].pop("soil")
		if "soil" in lines_parsed[stationid] and re.search(r"10cm Soil Temperature \(C\)", lines_parsed[stationid]["soil"]):
			lines_parsed[stationid].pop("soil")
		if "wdsp" in lines_parsed[stationid] and re.search(r"Mean Wind Speed \((knot|kt)\)", lines_parsed[stationid]["wdsp"]):
			lines_parsed[stationid].pop("wdsp")
		if "hm" in lines_parsed[stationid] and re.search(r"Highest ten minute mean wind speed \((knot|kt)\)", lines_parsed[stationid]["hm"]):
			lines_parsed[stationid].pop("hm")
		if "ddhm" in lines_parsed[stationid] and re.search(r"Wind Direction at max 10 min(.|) mean \(deg\)", lines_parsed[stationid]["ddhm"]):
			lines_parsed[stationid].pop("ddhm")
		if "hg" in lines_parsed[stationid] and re.search(r"Highest Gust \((knot|kt)\)", lines_parsed[stationid]["hg"]):
			lines_parsed[stationid].pop("hg")
		if "cbl" in lines_parsed[stationid] and re.search(r"Mean CBL Pressure \(hpa\)", lines_parsed[stationid]["cbl"]):
			lines_parsed[stationid].pop("cbl")
		if "sun" in lines_parsed[stationid] and re.search(r"Sunshine duration \(hours\)", lines_parsed[stationid]["sun"]):
			lines_parsed[stationid].pop("sun")
		if "g_rad" in lines_parsed[stationid] and re.search(r"Global Radiation \(j/cm sq\.\)", lines_parsed[stationid]["g_rad"]):
			lines_parsed[stationid].pop("g_rad")
		if "glorad" in lines_parsed[stationid] and re.search(r"Global Radiation \((j|J)/cm sq\.\)", lines_parsed[stationid]["glorad"]):
			lines_parsed[stationid].pop("glorad")
		if "pe" in lines_parsed[stationid] and re.search(r"Potential Evapotranspiration \(mm\)", lines_parsed[stationid]["pe"]):
			lines_parsed[stationid].pop("pe")
		if "evap" in lines_parsed[stationid] and re.search(r"Evaporation \(mm\)", lines_parsed[stationid]["evap"]):
			lines_parsed[stationid].pop("evap")
		if "smd_wd" in lines_parsed[stationid] and re.search(r"Soil Moisture Deficits\(mm\) well drained", lines_parsed[stationid]["smd_wd"]):
			lines_parsed[stationid].pop("smd_wd")
		if "smd_md" in lines_parsed[stationid] and re.search(r"Soil Moisture Deficits\(mm\) moderately drained", lines_parsed[stationid]["smd_md"]):
			lines_parsed[stationid].pop("smd_md")
		if "smd_pd" in lines_parsed[stationid] and re.search(r"Soil Moisture Deficits\(mm\) poorly drained", lines_parsed[stationid]["smd_pd"]):
			lines_parsed[stationid].pop("smd_pd")
		if "ind" in lines_parsed[stationid] and re.search(r"Indicator \(i\)|Indicator", lines_parsed[stationid]["ind"]):
			lines_parsed[stationid].pop("ind")
		if "mint" in lines_parsed[stationid] and re.search(r"Minimum Temperature \(C\)", lines_parsed[stationid]["mint"]):
			lines_parsed[stationid].pop("mint")
		if "maxt" in lines_parsed[stationid] and re.search(r"Maximum Temperature \(C\)", lines_parsed[stationid]["maxt"]):
			lines_parsed[stationid].pop("maxt")
		if "temp" in lines_parsed[stationid] and re.search(r"Air Temperature \(C\)", lines_parsed[stationid]["temp"]):
			lines_parsed[stationid].pop("temp")
		if "wetb" in lines_parsed[stationid] and re.search(r"Wet Bulb Temperature \(C\)", lines_parsed[stationid]["wetb"]):
			lines_parsed[stationid].pop("wetb")
		if "dewpt" in lines_parsed[stationid] and re.search(r"Dew Point Temperature \(C\)", lines_parsed[stationid]["dewpt"]):
			lines_parsed[stationid].pop("dewpt")
		if "rhum" in lines_parsed[stationid] and re.search(r"Relative Humidity \(%\)", lines_parsed[stationid]["rhum"]):
			lines_parsed[stationid].pop("rhum")
		if "vappr" in lines_parsed[stationid] and re.search(r"Vapour Pressure \(hPa\)", lines_parsed[stationid]["vappr"]):
			lines_parsed[stationid].pop("vappr")
		if "msl" in lines_parsed[stationid] and re.search(r"Mean Sea Level Pressure \(hPa\)", lines_parsed[stationid]["msl"]):
			lines_parsed[stationid].pop("msl")
		if "wddir" in lines_parsed[stationid] and re.search(r"Predominant Wind Direction (\(degree\)|\(deg\))", lines_parsed[stationid]["wddir"]):
			lines_parsed[stationid].pop("wddir")
		if "ww" in lines_parsed[stationid] and re.search(r"Synop code for Present Weather", lines_parsed[stationid]["ww"]):
			lines_parsed[stationid].pop("ww")
		if "w" in lines_parsed[stationid] and re.search(r"Synop code for Past Weather", lines_parsed[stationid]["w"]):
			lines_parsed[stationid].pop("w")
		if "vis" in lines_parsed[stationid] and re.search(r"Visibility \(m\)", lines_parsed[stationid]["vis"]):
			lines_parsed[stationid].pop("vis")
		if "clht" in lines_parsed[stationid] and re.search(r"Cloud height \(100's of ft\) - 999 if none", lines_parsed[stationid]["clht"]):
			lines_parsed[stationid].pop("clht")
		if "clamt" in lines_parsed[stationid] and re.search(r"Cloud amount", lines_parsed[stationid]["clamt"]):
			lines_parsed[stationid].pop("clamt")
		if "year" in lines_parsed[stationid] and re.search(r"Year", lines_parsed[stationid]["year"]):
			lines_parsed[stationid].pop("year")
		if "month" in lines_parsed[stationid] and re.search(r"Month", lines_parsed[stationid]["month"]):
			lines_parsed[stationid].pop("month")
		if "meant" in lines_parsed[stationid] and re.search(r"Mean Air Temperature \(C\)", lines_parsed[stationid]["meant"]):
			lines_parsed[stationid].pop("meant")
		if "mnmax" in lines_parsed[stationid] and re.search(r"Mean Maximum Temperature \(C\)", lines_parsed[stationid]["mnmax"]):
			lines_parsed[stationid].pop("mnmax")
		if "mnmin" in lines_parsed[stationid] and re.search(r"Mean Minimum Temperature \(C\)", lines_parsed[stationid]["mnmin"]):
			lines_parsed[stationid].pop("mnmin")
		if "mxgt" in lines_parsed[stationid] and re.search(r"Highest Gust \(knot\)", lines_parsed[stationid]["mxgt"]):
			lines_parsed[stationid].pop("mxgt")

	df = pd.DataFrame(lines_parsed)
	df = df.transpose()
	df.reset_index(inplace=True)
	df = df.rename(columns={"index": "stationid"})
	# print(df)
	# print(len(lines_parsed))
	df.to_parquet(path=f"{outpath}/header.parquet", index=False)


if __name__ == '__main__':
	main(data, outpath)
