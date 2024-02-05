# Modified at: 2024.2.5
# Author: Steven
# Usage: grab data from SportsDataIO api

import requests
import json
from config import headers

month_strf = {
    "01": "JAN",
    "02": "FEB",
    "03": "MAR",
    "04": "APR",
    "05": "MAY",
    "06": "JUN",
    "07": "JUL",
    "08": "AUG",
    "09": "SEP",
    "10": "OCT",
    "11": "NOV",
    "12": "DEC",
}
team_list = [
    "TB",
    "BAL",
    "NYY",
    "TOR",
    "BOS",
    "MIN",
    "CLE",
    "DET",
    "CHW",
    "KC",
    "TEX",
    "HOU",
    "SEA",
    "LAA",
    "OAK",
    "ATL",
    "PHI",
    "MIA",
    "NYM",
    "WSH",
    "MIL",
    "CIN",
    "PIT",
    "CHC",
    "STL",
    "LAD",
    "SF",
    "SD",
    "ARI",
    "COL",
]


def main():
    for team in team_list:
        response = requests.get(
            "https://api.sportsdata.io/v3/mlb/scores/json/Players/{}".format(team),
            headers=headers,
        )
        data = response.json()
        json_object = json.dumps(data)
        with open("{}.json".format(team), "w") as outfile:
            outfile.write(json_object)


if __name__ == "__main__":
    main()
