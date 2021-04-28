#########################################
##### Name:     TianyuanXu          #####
##### Uniqname:    tyxu             #####
#########################################

from ProjFinal import secrets
import requests
import sqlite3
import json
import csv
from mpl_toolkits.basemap import Basemap
import numpy as np
import matplotlib.pyplot as plt
import warnings
import pycountry

warnings.filterwarnings("ignore")

CACHE_FILENAME = "covid_cache.json"
VACCINE_FILENAME = "vaccinations.json"
CACHE_DICT = {}


def open_cache(filename):
    ''' opens the cache file if it exists and loads the JSON into
    the CACHE_DICT dictionary.
    if the cache file doesn't exist, creates a new cache dictionary
    Parameters
    ----------
    None
    Returns
    -------
    The opened cache
    '''
    try:
        cache_file = open(filename, 'r')
        cache_contents = cache_file.read()
        cache_dict = json.loads(cache_contents)
        cache_file.close()
    except:
        cache_dict = {}
    return cache_dict


def save_cache(cache_dict, filename):
    ''' saves the current state of the cache to disk
    Parameters
    ----------
    cache_dict: dict
        The dictionary to save
    Returns
    -------
    None
    '''
    dumped_json_cache = json.dumps(cache_dict)
    fw = open(filename, "w")
    fw.write(dumped_json_cache)
    fw.close()


def construct_unique_key(baseurl, params):
    ''' constructs a key that is guaranteed to uniquely and
    repeatably identify an API request by its baseurl and params
    Parameters
    ----------
    baseurl: string
        The URL for the API endpoint
    params: dictionary
        A dictionary of param: param_value pairs
    Returns
    -------
    string
        the unique key as a string
    '''
    param_strings = []
    connector = "_"
    for k in params.keys():
        param_strings.append(f'{k}_{params[k]}')
    param_strings.sort()
    unique_key = baseurl + connector + connector.join(param_strings)
    return unique_key


def make_request(baseurl, params):
    '''Make a request to the Web API using the baseurl and params
    Parameters
    ----------
    baseurl: string
        The URL for the API endpoint
    params: dictionary
        A dictionary of param: param_value pairs
    Returns
    -------
    string
        the results of the query as a Python object loaded from JSON
    '''
    response = requests.get(baseurl, params=params)
    return response.json()


def make_request_with_cache(baseurl, params={}, API=False, CSV=False, JSON=False):
    '''Check the cache for a saved result for this baseurl+params
    combo. If the result is found, return it. Otherwise send a new
    request, save it, then return it.
    Parameters
    ----------
    baseurl: string
        The URL for the API endpoint
    params: dictionary
        A dictionary of param: param_value pairs
    Returns
    -------

    string
        the results of the query as a Python object loaded from JSON or html
    '''
    request_key = construct_unique_key(baseurl, params)
    CACHE_DICT = open_cache(CACHE_FILENAME)
    if request_key in CACHE_DICT.keys():
        print("Using cache")
        if not API and not CSV:
            return CACHE_DICT[request_key]["html"]
        elif API:
            return CACHE_DICT[request_key]
        elif CSV:
            return CACHE_DICT[request_key]["csv"]
    else:
        print("Fetching")
        if API:
            CACHE_DICT[request_key] = make_request(baseurl, params)
            save_cache(CACHE_DICT, CACHE_FILENAME)
            return CACHE_DICT[request_key]
        elif CSV:
            raw_csv = requests.Session().get(baseurl).content.decode('utf-8')
            parsed_csv = list(csv.DictReader(raw_csv.splitlines(), delimiter=','))
            CACHE_DICT[request_key] = {"csv": parsed_csv}
            save_cache(CACHE_DICT, CACHE_FILENAME)
            return CACHE_DICT[request_key]["csv"]
        # elif JSON:
        #     raw_json = requests.Session().get(baseurl).content.decode('utf-8')
        elif not API and not CSV:
            CACHE_DICT[request_key] = {"html": requests.get(baseurl).text}
            save_cache(CACHE_DICT, CACHE_FILENAME)
            return CACHE_DICT[request_key]["html"]


def fetch_geodata(geoname):
    '''Obtain API data from Google maps API.

    Parameters
    ----------
    geoname: list
        a list of location names

    Returns
    -------
    none
    '''
    api_link = "https://maps.googleapis.com/maps/api/geocode/json?search"

    conn = sqlite3.connect('geodata.sqlite')
    cur = conn.cursor()
    drop_loc = '''
        DROP TABLE IF EXISTS "Locations";
    '''

    create_loc = '''
        CREATE TABLE IF NOT EXISTS "Locations" (
            "id"        INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "country"  TEXT NOT NULL,
            "xlocation"  INTEGER NOT NULL,
            "ylocation"  INTEGER NOT NULL,
            "view_ne_lat"    INTEGER NOT NULL,
            "view_ne_lng"    INTEGER NOT NULL,
            "view_sw_lat"    INTEGER NOT NULL,
            "view_sw_lng"    INTEGER NOT NULL
        );
    '''

    cur.execute(drop_loc)
    cur.execute(create_loc)

    for country_name in geoname:
        parms = dict()
        parms["address"] = country_name
        parms["key"] = secrets.API_KEY
        resp = make_request_with_cache(api_link, parms, API=True)
        # print(resp)
        xlocation = resp['results'][0]['geometry']['location']['lat']
        ylocation = resp['results'][0]['geometry']['location']['lng']
        view_ne_lat = resp['results'][0]['geometry']['viewport']['northeast']['lat']
        view_ne_lng = resp['results'][0]['geometry']['viewport']['northeast']['lng']
        view_sw_lat = resp['results'][0]['geometry']['viewport']['southwest']['lat']
        view_sw_lng = resp['results'][0]['geometry']['viewport']['southwest']['lng']

        cur.execute('''INSERT INTO Locations (country, xlocation, ylocation, view_ne_lat,
                        view_ne_lng, view_sw_lat, view_sw_lng)
                        VALUES ( ?, ? , ?, ?, ?, ?, ?)''',
                    (country_name, xlocation, ylocation, view_ne_lat,
                     view_ne_lng, view_sw_lat, view_sw_lng))
        conn.commit()


def fetch_vacdata():
    '''Obtain vaccine data from json file.

    Parameters
    ----------
    none

    Returns
    -------
    none
    '''
    vaccine_data = open_cache(VACCINE_FILENAME)

    conn = sqlite3.connect('geodata.sqlite')
    cur = conn.cursor()
    drop_vaccine = '''
        DROP TABLE IF EXISTS "Vaccinations";
    '''

    create_vaccine = '''
        CREATE TABLE IF NOT EXISTS "Vaccinations" (
            "id"                INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "country_id"        INTEGER,
            "vac_date"          TEXT,
            "vac_num"           REAL,
            "vac_per_hundred"   REAL,
            FOREIGN KEY(country_id) REFERENCES Locations(country_id)
        );
    '''

    cur.execute(drop_vaccine)
    cur.execute(create_vaccine)

    for region in vaccine_data:
        foreign_key = country_key_DICT.get(region['country'])
        if foreign_key is None: continue
        for day in region['data']:
            date = day.get('date')
            vac_num = day.get('total_vaccinations')
            vac_per_hundred = day.get('total_vaccinations_per_hundred')
            cur.execute('''INSERT INTO Vaccinations (country_id, vac_date, vac_num, vac_per_hundred)
                    VALUES ( ?, ? , ?, ?)''', (foreign_key, date, vac_num, vac_per_hundred))
            conn.commit()


def fetch_covdata():
    '''Obtain covid data from github.

    Parameters
    ----------
    none

    Returns
    -------
    none
    '''
    covid_link = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/" \
                 "master/csse_covid_19_data/csse_covid_19_time_series/" \
                 "time_series_covid19_confirmed_global.csv"
    covid_data = make_request_with_cache(covid_link, CSV=True)

    conn = sqlite3.connect('geodata.sqlite')
    cur = conn.cursor()
    drop_covid = '''
        DROP TABLE IF EXISTS "covid_cases";
    '''

    create_covid = '''
        CREATE TABLE IF NOT EXISTS "covid_cases" (
            "id"                INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "State"             TEXT,
            "Country"           TEXT,
            "Lat"               REAL,
            "Long"              REAL,
            "confirmed_cases"   REAL
        );
    '''

    cur.execute(drop_covid)
    cur.execute(create_covid)

    for region in covid_data:
        state = region.get('Province/State')
        country = region.get('Country/Region')
        lat = region.get('Lat')
        long = region.get('Long')
        confirmed_cases = list(region.values())[-1]
        cur.execute('''INSERT INTO covid_cases (State, Country, Lat, Long, confirmed_cases)
                VALUES ( ?, ?, ?, ?, ?)''', (state, country, lat, long, confirmed_cases))
        conn.commit()


def plot_map(location=False):
    # fetch data to display
    conn = sqlite3.connect('geodata.sqlite')
    cur = conn.cursor()
    fetch_covid = '''
        SELECT Lat, Long, confirmed_cases FROM covid_cases;
    '''
    cur.execute(fetch_covid)
    case_tuple = cur.fetchall()

    fetch_vac = '''
        select vac_num, vac_per_hundred, xlocation, ylocation
        from (select * from Vaccinations order by vac_per_hundred desc ) join Locations on country_id = Locations.id
        group by country_id;
    '''
    cur.execute(fetch_vac)
    vac_tuple = cur.fetchall()

    if location is not False:
        try:
            fetch_loc = '''
                select view_ne_lat, view_ne_lng, view_sw_lat, view_sw_lng
                from Locations
                where country = '%s';
            ''' % location
            cur.execute(fetch_loc)
        except:
            return False
        loc_tuple = cur.fetchall()
        try:
            view_ne_lat = loc_tuple[0][0]
            view_ne_lng = loc_tuple[0][1]
            view_sw_lat = loc_tuple[0][2]
            view_sw_lng = loc_tuple[0][3]
        except:
            return False

        if not (isinstance(view_ne_lat, float) and
                isinstance(view_ne_lng, float) and
                isinstance(view_sw_lat, float) and
                isinstance(view_sw_lng, float)):
            return False

    conn.commit()

    # setup mercator map projection.
    if location is False:
        m = Basemap(projection='robin', lon_0=0, resolution='c')
    else:
        m = Basemap(llcrnrlon=view_sw_lng, llcrnrlat=view_sw_lat, urcrnrlon=view_ne_lng, urcrnrlat=view_ne_lat,
                    resolution='l', projection='merc')

    # process covid case data
    case_lat = [place[0] for place in case_tuple if isinstance(place[0], float)]
    case_long = [place[1] for place in case_tuple if isinstance(place[0], float)]
    case_x, case_y = m(case_long, case_lat)
    # process vaccination data
    vac_lat = [place[2] for place in vac_tuple if isinstance(place[0], float)]
    vac_long = [place[3] for place in vac_tuple if isinstance(place[0], float)]
    vac_x, vac_y = m(vac_long, vac_lat)
    if location is False:
        confirmed_cases = [place[2] / 10000 for place in case_tuple if isinstance(place[0], float)]
        vac_num = [place[1] * 5 for place in vac_tuple if isinstance(place[0], float)]
    else:
        confirmed_cases = [place[2] / 1000 for place in case_tuple if isinstance(place[0], float)]
        vac_num = [place[1] * 50 for place in vac_tuple if isinstance(place[0], float)]
    # plot data on world map
    m.scatter(case_x, case_y, s=confirmed_cases, c="r", alpha=0.3, zorder=2)
    m.scatter(vac_x, vac_y, s=vac_num, c="g", alpha=0.3, zorder=2)
    m.fillcontinents()
    m.drawcountries()
    m.drawmapboundary(fill_color='#99ffff')
    m.fillcontinents(color='#cc9966', lake_color='#99ffff')
    # draw parallels
    m.drawparallels(np.arange(-90, 90, 20), labels=[1, 1, 0, 1])
    # draw meridians
    m.drawmeridians(np.arange(-180, 180, 60), labels=[1, 1, 0, 1])
    # ax.set_title('Great Circle from New York to London')
    plt.show()


if __name__ == "__main__":
    # fetch location data
    countrylink = "https://raw.githubusercontent.com/owid/covid-19-data/" \
                  "master/public/data/vaccinations/locations.csv"
    locations = make_request_with_cache(countrylink, CSV=True)
    country_lst = []
    for country in locations:
        country_lst.append(country["location"])

    # create a dictionary for key reference
    country_key_DICT = {}
    for i in range(len(country_lst)):
        country_key_DICT[country_lst[i]] = i

    # fetch geometric data
    # fetch_geodata(country_lst)

    # fetch vaccination data
    # fetch_vacdata()

    # fetch covid data
    # fetch_covdata()

    plot_map()

    while True:
        command = input("Please select a country to view detail, or \'exit\' to exit")
        if command.lower() == "exit":
            print("Thanks for using this tool, bye!")
            break
        else:

            report = plot_map(command)
            if report is False:
                try:
                    search = pycountry.countries.search_fuzzy(command)[0].name
                except:
                    print("invalid, please try again")
                    continue
                second_try = plot_map(search)
                if second_try is False:
                    print("invalid, please try again")
                    continue
            print("Showing plot on map \n")

