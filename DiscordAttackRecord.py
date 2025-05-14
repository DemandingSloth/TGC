import shutil
import datetime
from datetime import timedelta
import requests
import json
import csv
import discord
import os
import pytz
import sys
import pandas as pd
from dotenv import load_dotenv
from discord import SyncWebhook
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'
loaded = load_dotenv(dotenv_path=env_path)

print(f"load_dotenv() returned: {loaded}")

# Access environment variables
api_key = ""
api_url = "https://cartelempire.online/api/user?type=advanced&id={id}&key={key}"
discord_token = os.getenv('DISCORD_TOKEN')
discord_channel_id = os.getenv('DISCORD_CHANNEL_ID')

if not loaded:
    print("Error: .env file not loaded.")

# Hardcoded list of CSV file paths
csv_file_paths = [
    r"C:\Users\bambe\OneDrive\Documents\Cartel Empire\TGC.csv",
    # Add more paths as needed
]

# Main Program Logic
def load_data_from_spreadsheet(spreadsheet_path):
    """Loads player IDs and previous attack data from the spreadsheet (CSV)."""
    attack_data = {}
    # Initialize Webhook
    webhook = SyncWebhook.from_url("https://discord.com/api/webhooks/1358783134346383530/XyiyL9bY62hvKfpYn3utbiP4embC0xDBAByt3PmEjA9Jumw6TXEA7F7YUoZcjNa3t6AM")

    try:
        print(f"attempting to open: {spreadsheet_path}")  # added print statement for debugging
        with open(spreadsheet_path, "r", newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)  # Use DictReader for easier access
            name = os.path.splitext(os.path.basename(spreadsheet_path))[0]
            #webhook.send(name)
            print(name)
            for row in reader:
                try:
                    player_id = int(row["ID"])
                    prev_attacks = int(row["Attacks Total"]) if row["Attacks Total"] else 0
                    attack_data[player_id] = {
                        "name": row["Name"],
                        "prev_attacks": prev_attacks,
                        "Last Run Date": row["Date Last Run"] if row["Date Last Run"] else "",
                        "Daily Attacks": int(row["Daily Attacks"]) if row["Daily Attacks"] else 0,
                        "Weekly Attacks": int(row["Weekly Attacks"]) if row["Weekly Attacks"] else 0,
                        "Monthly Attacks": int(row["Monthly Attacks"]) if row["Monthly Attacks"] else 0,
                        "Attack History": row["Attack History"] if row["Attack History"] else "",
                        "completed": row["Completed"].lower() == "yes" if row["Completed"] else False,
                    }
                except ValueError:
                    print(f"Skipping invalid row: {row}")
                    continue
    except FileNotFoundError:
        print(f"Error: Spreadsheet file not found at {spreadsheet_path}")
        return None  # return none if file not found, so main can handle it.
    return attack_data

def save_data_to_spreadsheet(attack_data, spreadsheet_path):
    """Saves updated attack data and completion status back to the spreadsheet (CSV)."""
    # Create a backup of the original spreadsheet before saving
    base_filename = os.path.splitext(os.path.basename(spreadsheet_path))[0]  # Gets filename without extension.
    backup_filename = f"{base_filename}backup.csv"
    shutil.copyfile(spreadsheet_path, backup_filename)
    print(f"Created backup: {backup_filename}")

    # Now save the updated data to the original file
    with open(spreadsheet_path, "w", newline="") as csv_file:
        fieldnames = [
            "ID",
            "Name",
            "Attacks Total",
            "Completed",
            "Date Last Run",
            "Daily Attacks",
            "Weekly Attacks",
            "Monthly Attacks",
            "Attack History",
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for player_id, data in attack_data.items():
            writer.writerow({
                "ID": player_id,
                "Name": data["name"],
                "Attacks Total": data["prev_attacks"],
                "Completed": "Yes" if data["completed"] else "No",
                "Date Last Run": data["Last Run Date"],
                "Daily Attacks": data["Daily Attacks"],
                "Weekly Attacks": data["Weekly Attacks"],
                "Monthly Attacks": data["Monthly Attacks"],
                "Attack History": data["Attack History"],
            })

def fetch_player_data(player_id):
    global api_url, api_key  # add global keyword
    response = requests.get(api_url.format(id=player_id, key=api_key))
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Error fetching data for player ID {player_id}: {response.status_code}")
        return None

def process_player(player_id, attack_data):
    """Processes player data, checks for attack completion, and updates storage."""
    player_data = fetch_player_data(player_id)
    if player_data:
        name = player_data["name"]
        attacks_won = player_data["attacksWon"]

        if player_id in attack_data:
            previous_attacks = attack_data[player_id]["prev_attacks"]
            difference = attacks_won - previous_attacks

            # Initialize Webhook
            webhook = SyncWebhook.from_url("https://discord.com/api/webhooks/1358783134346383530/XyiyL9bY62hvKfpYn3utbiP4embC0xDBAByt3PmEjA9Jumw6TXEA7F7YUoZcjNa3t6AM")

            # Convert 'today' to a datetime.date object
            today = datetime.datetime.now().date()

            last_run_date = datetime.datetime.strptime(attack_data[player_id]["Last Run Date"], "%Y-%m-%d").date() if attack_data[player_id]["Last Run Date"] else None

            # Now the comparison will work correctly
            if last_run_date and today > last_run_date:
                attack_data[player_id]["Daily Attacks"] = 0
                attack_data[player_id]["completed"] = False

            # Reset weekly attacks if it's Sunday
            if datetime.datetime.now().weekday() == 6:
                attack_data[player_id]["Weekly Attacks"] = 0

            # Reset monthly attacks if it's the first of the month
            if datetime.datetime.now().day == 1:
                attack_data[player_id]["Monthly Attacks"] = 0

            # Calculate today's attacks, reset if not completed
            if not attack_data[player_id]["completed"]:
                todays_attacks = difference
            else:
                todays_attacks = attack_data[player_id]["Daily Attacks"] + difference

            # Check for completion
            if todays_attacks >= 1:
                attack_data[player_id]["completed"] = True
                # webhook.send(f"🎉 {name}  {todays_attacks} attacks")

            # Update storage
            attack_data[player_id]["prev_attacks"] = attacks_won
            if difference > 0:
                attack_data[player_id]["Attack History"] += f",{today}:{attacks_won}"

            attack_data[player_id]["Weekly Attacks"] += difference
            attack_data[player_id]["Monthly Attacks"] += difference
            attack_data[player_id]["Daily Attacks"] = todays_attacks
            attack_data[player_id]["Last Run Date"] = today
            monthly_attacks = attack_data[player_id]["Monthly Attacks"]
            #webhook.send(f"🎉 {name}:  {monthly_attacks}")
            print(f"🎉 {name}:  {monthly_attacks}")

def calculate_attack_total(attack_history, start_date, end_date):
    """Calculates the total number of attacks within a given date range."""
    total_attacks = 0
    for entry in attack_history.split(","):
        if entry:
            date_str, attacks_str = entry.split(":")
            attack_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            if start_date <= attack_date <= end_date:
                total_attacks += int(attacks_str)
    return total_attacks

def get_weekly_attacks(player_id, attack_data):
    # Gets the total number of attacks for the player in the past 7 days.
    today = datetime.datetime.now().date()
    week_start = today - timedelta(days=7)
    return calculate_attack_total(attack_data[player_id]["Attack History"], week_start, today)

def get_monthly_attacks(player_id, attack_data):
    """Gets the total number of attacks for the player in the past 30 days."""
    today = datetime.datetime.now().date()
    month_start = today - timedelta(days=30)
    return calculate_attack_total(attack_data[player_id]["Attack History"], month_start, today)

def process_csv_files(file_paths):
    all_top_names = []

    for file_path in file_paths:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            sorted_df = df.sort_values(by="Weekly Attacks", ascending=False) #Changed sort by to weekly.
            print(sorted_df)

            top_75_names = sorted_df.head(75)[['Name', 'Weekly Attacks']].copy() #changed column to weekly.
            # base_filename = os.path.splitext(os.path.basename(file_path))[0] #Gets filename without extension.
            # top_75_names['Name'] = base_filename + " - " + top_75_names['Name'].astype(str) # Add filename to name
            all_top_names.extend(top_75_names.values.tolist())

    sorted_top_df = pd.DataFrame(all_top_names, columns=['Name', 'Weekly Attacks']) #changed column to weekly.
    sorted_top_df = sorted_top_df.sort_values(by="Weekly Attacks", ascending=False) #changed sort by to weekly.

    return sorted_top_df

def output_to_file(sorted_top_df, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        file.write("Name,Weekly Attacks\n") #changed header to weekly
        for index, row in sorted_top_df.iterrows():
            file.write(f"{row['Name']},{row['Weekly Attacks']}\n") #changed column to weekly

def send_to_discord(sorted_top_df, webhook, message_id="1358919237384802565"):
    gmt_timezone = pytz.timezone('UTC')
    now_gmt = datetime.datetime.now(gmt_timezone)
    gmt_timestamp_simple = now_gmt.strftime("%Y-%m-%d %H:%M:%S LPT")
    message = f"**Weekly Leaderboard ({gmt_timestamp_simple}):**\n"
    for index, row in sorted_top_df.head(75).iterrows():
        message += f"{row['Name']}: {row['Weekly Attacks']} Attacks\n"

    if len(message) > 2000:
        message = message[:1997] + "..."

    if message_id:
        # Edit the existing message
        try:
            webhook.edit_message(message_id, content=message)
            return message_id  # Return the message ID on successful edit
        except discord.errors.NotFound:
            # Message not found, send a new one
            sent_message = webhook.send(message)
            if sent_message:  # Check if send was successful
                return sent_message.id
            else:
                print("Error: Failed to send new Discord message.")
                return None  # Or handle the error as appropriate
        except Exception as e:
            print(f"Error editing Discord message: {e}")
            sent_message = webhook.send(message)
            if sent_message:  # Check if send was successful
                return sent_message.id
            else:
                print("Error: Failed to send new Discord message after edit failure.")
                return None  # Or handle the error as appropriate
    else:
        # Send a new message
        sent_message = webhook.send(message)
        if sent_message:  # Check if send was successful
            return sent_message.id
        else:
            print("Error: Failed to send initial Discord message.")
            return None  # Or handle the error as appropriate

# Main Program Execution
if __name__ == "__main__":
    webhook = SyncWebhook.from_url("https://discord.com/api/webhooks/1358783134346383530/XyiyL9bY62hvKfpYn3utbiP4embC0xDBAByt3PmEjA9Jumw6TXEA7F7YUoZcjNa3t6AM")
    message_id = None  # Initialize message_id

    for spreadsheet_path in csv_file_paths:
        attack_data = load_data_from_spreadsheet(spreadsheet_path)
        if attack_data is not None:
            for player_id, data in attack_data.items():
                process_player(player_id, attack_data)
            save_data_to_spreadsheet(attack_data, spreadsheet_path)
        else:
            print(f"Skipping {spreadsheet_path} due to loading error.")

    sorted_names = process_csv_files(csv_file_paths)

    output_to_file(sorted_names, "top_names_output.csv")

    message_id = send_to_discord(sorted_names, webhook, message_id)

    # Corrected: Define output_file_path
    output_file_path = "top_names_output.csv" # added this line.

    output_to_file(sorted_names, output_file_path)