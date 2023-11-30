import requests
import json
import os

def extract(org, token):
    headers = {"Authorization": f"token {token}"}
    org_url = f"https://api.github.com/orgs/{org}/repos"
    response = requests.get(org_url, headers=headers)
    repositories = response.json()

    # create folder if it does not exist
    if not os.path.exists("github_extract"):
        os.makedirs("github_extract")

    # loop for all repositories
    for repo in repositories:
        dir = repo["name"]

        url = f"https://api.github.com/repos/{org}/{dir}/pulls"
        pr = requests.get(url, headers=headers)
        data = pr.json()

        # save data
        path = f"github_extract/{dir}_pull_requests.json"
        with open(path, "w") as file:
            json.dump(data, file, indent=2)
