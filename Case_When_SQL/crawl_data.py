from urllib.request import urlopen
import urllib
import json
from bs4 import BeautifulSoup, NavigableString
import utils
import time

team_table = []

## Loops through team table, gets info for teams and roster links
url = "http://espn.go.com/college-football/teams"
soup = BeautifulSoup(urlopen(url).read())

for division in soup.findAll("div", { "class":"span-2" })[0:2]:
    div_name = division.find("h4").contents[0]
    
    for conf in division.findAll("div", {"class":"mod-teams-list-medium"}):
        conf_name = conf.find("h4").contents[0]
        
        for teams in conf.findAll("li"):
            team_link = teams.find("h5")
            team_name = team_link.find("a").contents[0]
            
            links = teams.findAll("a")
            
            for l in links:
                if l.contents[0] == "Roster":
                    roster_link = "http://espn.go.com" + l['href']
                    
                    team_info = (div_name,conf_name,team_name,roster_link)
                    team_table.append(team_info)

utils.write_to_csv("teams.csv",team_table)

## Gets players for all schools using player table
player_table = []

for t in team_table:
    
    team_url = t[3]
    print(t[2])
    team_soup = BeautifulSoup(urlopen(team_url).read())
    
    school_name = team_soup.find("h2").contents[0].contents[0].contents[0]
    
    roster = team_soup.find("table", {"class":"tablehead"})
    rows = roster.findAll("tr")
    
    for player in rows:
        if player["class"][0] == "oddrow" or player["class"][0] == "evenrow":
            player_fields = player.findAll("td")
            
            name = player_fields[1].contents[0].contents[0]
            pos = player_fields[2].contents[0]
            
            ht = player_fields[3].contents[0]
            height = utils.get_height(ht)
            
            weight = player_fields[4].contents[0]
            year = player_fields[5].contents[0]
            
            hometown = player_fields[6].contents[0]
            state = utils.get_state(hometown).strip()
            
            player_info = (school_name,t[2],name,pos,height,weight,year,hometown,state)
            
            player_table.append(player_info)

pt = utils.strip_special(player_table,[0,1,2])
utils.write_to_csv("players_raw.csv",pt)

## Loops through towns and gets county

## Find distinct hometowns
hometowns = []

for r in player_table:
    town = r[7]
    
    hometowns.append(town)

distinct_towns = list(set(hometowns))


## Looks up each town
counties = []
errors = []

town_count = len(distinct_towns)

for i in range(town_count):
    
    h = distinct_towns[i]
    
    print(str(i) + " --- " + h)
    possible_counties = []
    
    clean = h.replace(", ","+")
    clean = clean.replace(" ","+")
    
    url = "http://maps.googleapis.com/maps/api/geocode/json?address=" + clean + "&sensor=false"
    try:
        req = urllib.request(url)
        j = urlopen(req)
        js = json.load(j)
        
        results = js['results']
        
        for r in results:
            comp = r['address_components']
            for c in comp:
                if 'administrative_area_level_2' in c['types']:
                    county_long = c['long_name']
                    county_short = c['short_name']
                    
                    county = (h,county_long,county_short)
                    possible_counties.append(county)
        
        counties.append(possible_counties[0])
        
    except Exception as Err:
        print("Exception")
        print(Err)
        errors.append(h)
        
    time.sleep(5)

cc = utils.strip_special(counties,(0,1,2))
utils.write_to_csv("hometowns_complete.csv",cc)


