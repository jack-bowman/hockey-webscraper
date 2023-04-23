import discord, random, re, datetime, asyncio, pytz, os, io, textwrap, gspread, pandas, csv, filecmp, logging, requests
from dotenv import load_dotenv
from playoff import scorecheck, process
from sheetsup import csvupload, csvchangelog, fcomp, download
from github import Github
from github.GithubException import GithubException
from oauth2gclient.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup

load_dotenv('os.env')
est = pytz.timezone('US/Eastern')
client = discord.Client(intents=discord.Intents.all())
jack = 707020641483882537
bottoken = os.environ.get("bottoken")
githubtoken = os.environ.get("githubtoken")
today = datetime.date.today()
today_date = today.day
scope = ['https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name('package.json', scope)
gclient = gspread.authorize(creds)
sheet_url = 'https://docs.google.com/spreadsheets/d/1EmZMa8j_iuCJcpmCtE3Y6gw7tgrwy7AErLFj0GmYtiI'
logging.basicConfig(filename='download.log', level=logging.INFO)

@client.event
async def on_ready():
  print('Online.');
  client.loop.create_task(checktime())
  client.loop.create_task(playoffs())
def scorecheck():
  url = 'https://www.hockey-reference.com/leagues/NHL_2023_games.html#games_playoffs'
  response = requests.get(url)
  soup = BeautifulSoup(response.content, 'html.parser')
  table = soup.find('table', {'id': 'games_playoffs'})
  tbody = table.find("tbody")
  data_rows = []
  for row in tbody.find_all("tr"):
    date = row.find("th", {"data-stat": "date_game"}).text
    visitor_team = row.find("td", {"data-stat": "visitor_team_name"}).text
    visitor_goals = row.find("td", {"data-stat": "visitor_goals"}).text
    home_team = row.find("td", {"data-stat": "home_team_name"}).text
    home_goals = row.find("td", {"data-stat": "home_goals"}).text
    overtime = row.find("td", {"data-stat": "overtimes"}).text.strip()
    data_row = {"Date": date,
      "visitor_team": visitor_team,
      "visitor_goals": visitor_goals,
      "home_team": home_team,
      "home_goals": home_goals,
      "overtime": overtime}
    data_rows.append(data_row)
  with open("htmlf/rawgames.csv", "w", newline="") as csv_file:
      field_names = ["Date", "visitor_team", "visitor_goals", "home_team", "home_goals", "overtime"]
      writer = csv.DictWriter(csv_file, fieldnames=field_names)
      for data_row in data_rows:
          writer.writerow(data_row)
def process():
  abbr = {"Anaheim Ducks": "ANA",
"Arizona Coyotes": "ARI",
"Boston Bruins": "BOS",
"Buffalo Sabres": "BUF",
"Calgary Flames": "CGY",
"Carolina Hurricanes": "CAR",
"Chicago Blackhawks": "CHI",
"Colorado Avalanche": "COL",
"Columbus Blue Jackets": "CBJ",
"Dallas Stars": "DAL",
"Detroit Red Wings": "DET",
"Edmonton Oilers": "EDM",
"Florida Panthers": "FLA",
"Los Angeles Kings": "LAK",
"Minnesota Wild": "MIN",
"Montreal Canadiens": "MTL",
"Nashville Predators": "NSH",
"New Jersey Devils": "NJD",
"New York Islanders": "NYI",
"New York Rangers": "NYR",
"Ottawa Senators": "OTT",
"Philadelphia Flyers": "PHI",
"Pittsburgh Penguins": "PIT",
"San Jose Sharks": "SJS",
"Seattle Kraken": "SEA",
"St. Louis Blues": "STL",
"Tampa Bay Lightning": "TBL",
"Toronto Maple Leafs": "TOR",
"Vancouver Canucks": "VAN",
"Vegas Golden Knights": "VGK",
"Washington Capitals": "WSH",
"Winnipeg Jets": "WPG"}
  with open('htmlf/rawgames.csv', 'r', newline='') as rg, open('htmlf/games.csv', 'w', newline='') as pg:
    csv_reader = csv.reader(rg)
    csv_writer = csv.writer(pg)
    for row in csv_reader:
        updated_row = [abbr.get(elem, elem) if elem != '' else ' ' for elem in row]
        csv_writer.writerow(updated_row)
def csvchangelog():
  worksheet = gclient.open_by_url(sheet_url).worksheet('xAutomation')
  data = worksheet.get_all_values()
  with open('htmlf/gameslog.csv', 'w', newline='') as logfile:
    writer = csv.writer(logfile)
    writer.writerows(data)
def fcomp():
  games = "htmlf/games.csv"
  gameslog = "htmlf/gameslog.csv"
  localgoogle = filecmp.cmp(games, gameslog)
  if localgoogle:
    return
  else:
     download()
def download():
  with open('htmlf/lastrevision.txt', 'w') as f:
    timezone = pytz.timezone('US/Eastern')
    now = datetime.datetime.now(timezone)
    formatted_date = now.strftime("%B %d, at %I:%M%p")
    f.write(formatted_date)
  with open('htmlf/leaderboard.csv', 'w') as f:
    cellblock = "A4:D13"
    sheet = gclient.open_by_url(sheet_url).worksheet('Leaderboard')
    cell_list = sheet.range(cellblock)
    writer = csv.writer(f)
    rows = []
    n = 4
    for i in range(0, len(cell_list), n):
        row = [cell_list[i+j].value for j in range(n)]
        rows.append(row)
    sorted_rows = sorted(rows, key=lambda x: (x[0], x[1]))
    for row in sorted_rows:
        writer.writerow(row)
        logging.info(f'Wrote row: {row}')
  with open('htmlf/1bracket.csv', 'w') as f:
    cellblock = "A3:W17"
    sheet = gclient.open_by_url(sheet_url).worksheet('Picks')
    cell_list = sheet.range(cellblock)
    writer = csv.writer(f)
    rows = []
    n = 23
    for i in range(0, len(cell_list), n):
      row = [cell_list[i+j].value for j in range(n)]
      writer.writerow(row)
  with open('htmlf/2bracket.csv', 'w') as f:
    cellblock = "A19:W25"
    sheet = gclient.open_by_url(sheet_url).worksheet('Picks')
    cell_list = sheet.range(cellblock)
    writer = csv.writer(f)
    n = 23
    for i in range(0, len(cell_list), n):
        row = [cell_list[i+j].value for j in range(n)]
        writer.writerow(row)
  with open('htmlf/3bracket.csv', 'w') as f:
    cellblock = "A27:W29"
    sheet = gclient.open_by_url(sheet_url).worksheet('Picks')
    cell_list = sheet.range(cellblock)
    writer = csv.writer(f)
    n = 23
    for i in range(0, len(cell_list), n):
        row = [cell_list[i+j].value for j in range(n)]
        writer.writerow(row)
  with open('htmlf/4bracket.csv', 'w') as f:
    cellblock = "A31:W31"
    sheet = gclient.open_by_url(sheet_url).worksheet('Picks')
    cell_list = sheet.range(cellblock)
    writer = csv.writer(f)
    n = 23
    for i in range(0, len(cell_list), n):
        row = [cell_list[i+j].value for j in range(n)]
        writer.writerow(row)
  with open('htmlf/series.csv', 'w') as f:
    cellblock = "G4:O18"
    sheet = gclient.open_by_url(sheet_url).worksheet('Leaderboard')
    cell_list = sheet.range(cellblock)
    writer = csv.writer(f)
    n = 9
    for i in range(0, len(cell_list), n):
        row = [cell_list[i+j].value for j in range(n)]
        writer.writerow(row)
  gitupload()
def gitupload():
  with open('htmlf/lastrevision.txt', 'r') as r:
    revised = r.readline()
  g = Github(os.environ.get('githubtoken'))
  repo = g.get_repo("jack-bowman/jack-bowman.github.io")
  files = ['htmlf/lastrevision.txt','htmlf/leaderboard.csv','htmlf/1bracket.csv','htmlf/2bracket.csv','htmlf/3bracket.csv','htmlf/4bracket.csv','htmlf/series.csv','htmlf/games.csv']
  for filelocant in files:
    with open(filelocant, 'r') as r:
      filename = os.path.basename(filelocant)
      content = r.read()
      try:
        githublocant = repo.get_contents(filename)
        repo.update_file(filename, revised, content, githublocant.sha)
        print(f'{filelocant} was successfully updated, at {revised}')
      except GithubException as e:
        if e.status == 404:
          repo.create_file(filename, revised, content)
        else:
          print(f'{filename} was unsuccessful')
def csvupload():
  sheet = gclient.open_by_url(sheet_url).worksheet('xAutomation')
  df = pandas.read_csv('htmlf/games.csv')
  df.fillna(" ", inplace=True)
  sheet.clear()
  sheet.update([df.columns.values.tolist()] + df.values.tolist())
async def checktime():
    while True:
        now = datetime.datetime.now(tz=est)
        webupload1 = datetime.time(hour=6, minute=00)
        webupload2 = datetime.time(hour=8, minute=00)
        webupload3 = datetime.time(hour=10, minute=00)
        if now.time().hour == webupload1.hour and now.time().minute == webupload1.minute:
          download()
          await uploadmessage()
        if now.time().hour == webupload2.hour and now.time().minute == webupload2.minute:
          download()
          await uploadmessage()
        if now.time().hour == webupload3.hour and now.time().minute == webupload3.minute:
          download()
          await uploadmessage()
        await asyncio.sleep(60)
async def playoffs():
  while True:
    scorecheck()
    process()
    csvchangelog()
    fcomp()
    csvupload()
    await asyncio.sleep(3600)
async def uploadmessage():
  timezone = pytz.timezone('US/Eastern')
  now = datetime.datetime.now(timezone)
  channel = client.get_user(jack)
  await channel.send(f'Updated to Github at {now}')
async def generalcheckup():
  scorecheck()
  process()
  csvchangelog()
  fcomp()
  csvupload()

@client.event
async def on_message(message):
  if "!checkup" in message.content.lower() and message.author != client.user:
    await generalcheckup()
  if "!update" in message.content.lower() and message.author != client.user:
    download()
    await uploadmessage()
client.run(bottoken)