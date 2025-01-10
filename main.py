# https://cli.fusio.net/cli/climate_data/webdata/mindata/mindata_675_2021_01.zip


import requests					# used for fetch func
from pathlib import Path		# used for all path manipulations
import pandas as pd				# used for data manipulations
from zipfile import ZipFile		# used in dezip func
from tqdm import tqdm			# used for progress in all funcs
import shutil					# used in get_files func
import head_data				# head data sorter
import logging					# for logging

# typ:
typs = ["hourly", "daily", "monthly"]

# fetch settings
info_url = "https://cli.fusio.net/cli/climate_data/stations.csv"
data_base = "https://cli.fusio.net/cli/climate_data/webdata"

# base path
base = "/home/anderthel/Desktop/Programing/ireland/data"

# paths
zips = f"{base}/0. zips"
extracted = f"{base}/1. extracted"
raw_data = f"{base}/2. raw_data"
raw_split = f"{base}/3. raw_split"
merged = f"{base}/4. merged"
sortnfixed = f"{base}/5. sortnfixed"

# other type dependent info
typ_info = {
	"hourly": {
		"dupecols": [],
		"body_expected": ['stationid', 'date', 'ind', 'rain', 'ind.1', 'temp', 'ind.2', 'wetb', 'dewpt', 'vappr', 'rhum', 'msl', 'ind.3', 'wdsp', 'ind.4', 'wddir', 'ww', 'w', 'sun', 'vis', 'clht', 'clamt'],
		"body_shortnames": ["stationid", "date", "ind.rain", "rain", "ind.temp", "temp", "ind.wetb", "wetb", "dewpt", "vappr", "rhum", "msl", "ind.wdsp", "wdsp", "ind.wddir", "wddir", "ww", "w", "sun", "vis", "clht", "clamt"],  # noqa: E501
		"head_expected": ["stationid", "name", "height", "lat", "lon"],
		"head_shortnames": ["stationid", "name", "height", "lat", "lon"],
		"longnames": [],
		"typ_short": "hly",
		"split_point": "date,",
		"date_format": "%d-%b-%Y %H:%M",
	},
	"daily": {
		"dupecols": [["g_rad", "glorad"]],
		"body_expected": ['stationid', 'date', 'ind', 'maxtp', 'ind.1', 'mintp', 'igmin', 'gmin', 'ind.2', 'rain', 'cbl', 'wdsp', 'ind.3', 'hm', 'ind.4', 'ddhm', 'ind.5', 'hg', 'soil', 'pe', 'evap', 'smd_wd', 'smd_md', 'smd_pd', 'glorad', 'sun', 'dos', 'g_rad'],  # noqa: E501
		"body_shortnames": ["stationid", "date", "ind.maxtemp", "maxtemp", "ind.mintemp", "mintemp", "igmin", "gmin", "ind.rain", "rain", "cbl", "wdsp", "ind.hm", "hm", "ind.ddhm", "ddhm", "ind.hg", "hg", "soil", "pe", "evap", "smd_wd", "smd_md", "smd_pd", "glorad", "sun", "dos", "g_rad"],  # noqa: E501
		"head_expected": ["stationid", "name", "height", "lat", "lon", "date"],
		"head_shortnames": ["stationid", "name", "height", "lat", "lon", "period"],
		"longnames": [],
		"typ_short": "dly",
		"split_point": "date,",
		"date_format": "%d-%b-%Y",
	},
	"monthly": {
		"dupecols": [],
		"body_expected": ["stationid", "year", "month", "meant", "maxtp", "mintp", "mnmax", "mnmin", "rain", "gmin", "wdsp", "maxgt", "sun"],
		"body_shortnames": ["stationid", "year", "month", "meant", "maxtp", "mintp", "mnmax", "mnmin", "rain", "gmin", "wdsp", "maxgt", "sun"],
		"head_expected": ["stationid", "name", "height", "lat", "lon"],
		"head_shortnames": ["stationid", "name", "height", "lat", "lon"],
		"longnames": [],
		"typ_short": "mly",
		"split_point": "year,",
		"date_format": "%m-%Y",
	}
}


def fetch():
	logging.info("Fetching...")

	# get info about stations
	logging.info("    getting csv")
	df = pd.read_csv(info_url, header=0, usecols=["stno", "open_year", "close_year", "data_types"], na_values=" ")
	stations = {}
	logging.info("    splitting csv")
	for typ in typs:
		df = df[df['data_types'].str.contains(typ, case=False)]
		stations[typ] = df

	# get zips
	for typ in stations:
		logging.info(f"    type: {typ}")

		# create folder
		if not Path(f"{zips}/{typ}").exists():
			Path(f"{zips}/{typ}").mkdir(parents=True, exist_ok=True)

		# download zips
		for num in tqdm(df['stno'].tolist(), desc="        stations"):
			r = requests.get(f"{data_base}/{typ_info[typ]['typ_short']}{num}.zip")
			if r:
				with open(f"{zips}/{typ}/{typ_info[typ]['typ_short']}{num}.zip", "wb") as f:
					f.write(r.content)
			else:
				logging.warn(f"Error file not downloaded - {data_base}/{typ_info[typ]['typ_short']}{num}.zip")


def dezip():
	logging.info("Dezipping...")

	for typ in typs:
		logging.info(f"    type: {typ}")

		# create folder
		if not Path(f"{extracted}/{typ}").exists():
			Path(f"{extracted}/{typ}").mkdir(parents=True, exist_ok=True)

		# dezip
		for item in tqdm(list(Path(f"{zips}/{typ}").iterdir()), desc="        extracting"):
			if item.suffix == ".zip":
				with ZipFile(item, "r") as file:
					file.extractall(f"{extracted}/{typ}/{item.stem}/")


def move_csvs():
	logging.info("Moving files...")

	for typ in typs:
		logging.info(f"    type: {typ}")

		# create folder
		if not Path(f"{raw_data}/{typ}").exists():
			Path(f"{raw_data}/{typ}").mkdir(parents=True, exist_ok=True)

		# move
		for item in tqdm(list(Path(f"{extracted}/{typ}").rglob("*.csv")), desc="        moving"):
			shutil.copy(item, f"{raw_data}/{typ}/{item.name}")


def split_files():
	logging.info("Splitting files...")

	for typ in typs:
		lines = {}
		logging.info(f"    type: {typ}")
		for file in tqdm(list(Path(f"{raw_data}/{typ}").iterdir()), desc="        reading"):
			header = []
			with open(file, "r", encoding="us-ascii") as f:
				while True:
					line = f.readline()
					if line.startswith(typ_info[typ]["split_point"]):
						f.seek(f.tell() - len(line))
						body = f.readlines()
						break
					else:
						header.extend(line)

			lines[str(Path(file.stem))] = [header, body]

		for item in tqdm(lines, desc="        writting"):
			if not Path(f"{raw_split}/{typ}/{item}").exists():
				Path(f"{raw_split}/{typ}/{item}").mkdir(parents=True, exist_ok=True)
			with open(f"{raw_split}/{typ}/{item}/header.csv", "w", encoding="us-ascii") as f:
				f.writelines(lines[item][0])
			with open(f"{raw_split}/{typ}/{item}/body.csv", "w", encoding="us-ascii") as f:
				f.writelines(lines[item][1])


def merge_raw():
	logging.info("Splitting files...")
	for typ in typs:
		logging.info(f"    type: {typ}")

		# create folder
		if not Path(f"{merged}/{typ}").exists():
			Path(f"{merged}/{typ}").mkdir(parents=True, exist_ok=True)

		# merge body
		dfs = []
		for item in tqdm(list(Path(f"{raw_split}/{typ}").iterdir()), desc="        body"):
			df = pd.read_csv(f"{item}/body.csv", sep=",", header=0, encoding="us-ascii", low_memory=False, na_values=" ")
			df.insert(0, "stationid", Path(item).stem.strip("hly").strip("dly").strip("mly"))
			dfs.append(df)

		df = pd.concat(dfs, axis=0, ignore_index=True)
		df.to_parquet(path=f"{merged}/{typ}/body.parquet", index=False)

		# merge header
		head_data.main(f"{raw_split}/{typ}", f"{merged}/{typ}")


def sortnfix():
	logging.info("Splitting files...")
	for typ in typs:
		logging.info(f"    type: {typ}")

		# folder
		if not Path(f"{sortnfixed}").exists():
			Path(f"{sortnfixed}").mkdir(parents=True, exist_ok=True)

		# body
		logging.info("        working on: body")
		body = pd.read_parquet(f"{merged}/{typ}/body.parquet")
		# colnames
		if list(body.columns.values) == typ_info[typ]["body_expected"]:
			body.set_axis(typ_info[typ]["body_shortnames"], axis="columns", inplace=True)
		else:
			print("Error the colnames are diffrent then expected - body!!")
			print(list(body.columns.values))
			raise Exception

		# monthly date fix:
		if typ == "monthly":
			body['date'] = body["month"].astype(str).str.pad(2, fillchar='0') + "-" + body["year"].astype(str)
			print(body['date'])
			body.drop(columns=["month", "year"], inplace=True)

		# dupe cols
		for dupe in typ_info[typ]["dupecols"]:
			body[dupe[0]] = body[dupe].sum(1)
			body.drop(columns=dupe[1], inplace=True)

		# sort n drop dupes
		body.sort_values(["date", "stationid"], ignore_index=True, inplace=True)
		body.drop_duplicates(inplace=True, ignore_index=True)

		# header
		logging.info("        working on: head")
		head = pd.read_parquet(f"{merged}/{typ}/header.parquet")
		head.sort_values("stationid", ignore_index=True, inplace=True)
		head.drop_duplicates(inplace=True, ignore_index=True)
		# colnames
		if list(head.columns.values) == typ_info[typ]["head_expected"]:
			head.set_axis(typ_info[typ]["head_shortnames"], axis="columns", inplace=True)
		else:
			print("Error the colnames are diffrent then expected - header!!")
			print(list(head.columns.values))
			raise Exception

		# add header data
		logging.info("        working on: merging")
		head = head.set_index("stationid")
		head = head.to_dict("index")
		head = body.apply(lambda x: head[x["stationid"]], axis=1, result_type='expand')
		body = pd.concat([body, head], axis=1)

		# fix dtypes, sort, save
		logging.info("        fixing dtypes")
		body = body.convert_dtypes()
		body["date"] = pd.to_datetime(body["date"], format=typ_info[typ]["date_format"])
		logging.info("        sorting")
		body.sort_values(["date", "stationid"], ignore_index=True, inplace=True)
		logging.info("        saving")
		body.to_parquet(path=f"{sortnfixed}/{typ}.parquet", index=False)


def printer():
	for typ in typs:
		body = pd.read_parquet(f"{sortnfixed}/{typ}.parquet")
		logging.info(body.describe())


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO, format="")
	# fetch()
	# dezip()
	# move_csvs()
	# split_files()
	# merge_raw()
	# sortnfix()
	printer()
