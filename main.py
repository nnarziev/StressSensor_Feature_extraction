# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This script is for Feature Extraction of StressSensor study                           #
# First, put all the users raw data files downloaded from dashboard to one folder       #
# Please, run the following command to execute it:                                      #
# python main.py C:/StressSensor/Data user1@smth.com,user2@smth.com,user3@smth.com      #
# 1st arg: python                                                                       #
# 2nd arg: script filename (main.py)                                                    #
# 3rd arg: url for location of all raw data files                                       #
# 4th arg: list of user email for whom you want to extract features, separated by ","   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os

import sys
import pandas as pd
import datetime
import re
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.request import HTTPError

NUMBER_OF_EMA = 6
LOCATION_HOME = "HOME"

UNLOCK_DURATION = "UNLOCK_DURATION"
CALLS = "CALLS"
ACTIVITY_TRANSITION = "ACTIVITY_TRANSITION"
ACTIVITY_RECOGNITION = "ACTIVITY_RECOGNITION"
AUDIO_LOUDNESS = "AUDIO_LOUDNESS"
TOTAL_DIST_COVERED = "TOTAL_DIST_COVERED"
MAX_DIST_TWO_LOCATIONS = "MAX_DIST_TWO_LOCATIONS"
RADIUS_OF_GYRATION = "RADIUS_OF_GYRATION"
MAX_DIST_FROM_HOME = "MAX_DIST_FROM_HOME"
NUM_OF_DIF_PLACES = "NUM_OF_DIF_PLACES"
GEOFENCE = "GEOFENCE"
SCREEN_ON_OFF = "SCREEN_ON_OFF"
APPLICATION_USAGE = "APPLICATION_USAGE"
SURVEY_EMA = "SURVEY_EMA"

APP_PCKG_TOCATEGORY_MAP_FILENAME = "package_to_category_map.csv"

pckg_to_cat_map = {}


def in_range(number, start, end):
    if start <= number <= end:
        return True
    else:
        return False


def get_filename_from_data_src(filenames, data_src, username):
    for filename in filenames:
        if username in filename and data_src in filename:
            return filename


def get_unlock_result(filename, start_time, end_time):
    result = 0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, duration = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time) and in_range(int(end), start_time, end_time):
                    result += int(duration)
    return result if result > 0 else "-"


def get_phonecall_result(filename, start_time, end_time):
    result = {
        "phone_calls_total_dur": 0,
        "phone_calls_total_number": 0,
        "phone_calls_ratio_in_out": 0
    }

    total_in = 0
    total_out = 0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, call_type, duration = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time) and in_range(int(end), start_time, end_time):
                    result["phone_calls_total_dur"] += int(duration)
                    if call_type == "IN":
                        total_in += 1
                    elif call_type == "OUT":
                        total_out += 1

    if result["phone_calls_total_dur"] > 0:
        result["phone_calls_total_number"] = total_in + total_out
        result["phone_calls_ratio_in_out"] = total_in / total_out if total_out > 0 else "-"
    else:
        result["phone_calls_total_dur"] = "-"
        result["phone_calls_total_number"] = "-"
        result["phone_calls_ratio_in_out"] = "-"

    return result


def get_activities_dur_result(filename, start_time, end_time):
    result = {
        "still": 0,
        "walking": 0,
        "running": 0,
        "on_bicycle": 0,
        "in_vehicle": 0,
        "on_foot": 0,
        "tilting": 0,
        "unknown": 0
    }

    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, activity_type, duration = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time) and in_range(int(end), start_time, end_time):
                    if activity_type == 'STILL':
                        result['still'] += int(duration)
                    elif activity_type == 'WALKING':
                        result['walking'] += int(duration)
                    elif activity_type == 'RUNNING':
                        result['running'] += int(duration)
                    elif activity_type == 'ON_BICYCLE':
                        result['on_bicycle'] += int(duration)
                    elif activity_type == 'IN_VEHICLE':
                        result['in_vehicle'] += int(duration)
                    elif activity_type == 'ON_FOOT':
                        result['on_foot'] += int(duration)
                    elif activity_type == 'TILTING':
                        result['tilting'] += int(duration)
                    elif activity_type == 'UNKNOWN':
                        result['unknown'] += int(duration)

    if result['still'] == 0:
        result['still'] = "-"
    if result['walking'] == 0:
        result['walking'] = "-"
    if result['running'] == 0:
        result['running'] = "-"
    if result['on_bicycle'] == 0:
        result['on_bicycle'] = "-"
    if result['in_vehicle'] == 0:
        result['in_vehicle'] = "-"
    if result['on_foot'] == 0:
        result['on_foot'] = "-"
    if result['tilting'] == 0:
        result['tilting'] = "-"
    if result['unknown'] == 0:
        result['unknown'] = "-"

    return result


def get_num_of_dif_activities_result(filename, start_time, end_time):
    result = {
        "still": 0,
        "walking": 0,
        "running": 0,
        "on_bicycle": 0,
        "in_vehicle": 0,
        "on_foot": 0,
        "tilting": 0,
        "unknown": 0
    }

    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                activity_type, timestamp = values[:-1].split(" ")
                if in_range(int(timestamp), start_time, end_time):
                    if activity_type == 'STILL':
                        result['still'] += 1
                    elif activity_type == 'WALKING':
                        result['walking'] += 1
                    elif activity_type == 'RUNNING':
                        result['running'] += 1
                    elif activity_type == 'ON_BICYCLE':
                        result['on_bicycle'] += 1
                    elif activity_type == 'IN_VEHICLE':
                        result['in_vehicle'] += 1
                    elif activity_type == 'ON_FOOT':
                        result['on_foot'] += 1
                    elif activity_type == 'TILTING':
                        result['tilting'] += 1
                    elif activity_type == 'UNKNOWN':
                        result['unknown'] += 1

    if result['still'] == 0:
        result['still'] = "-"
    if result['walking'] == 0:
        result['walking'] = "-"
    if result['running'] == 0:
        result['running'] = "-"
    if result['on_bicycle'] == 0:
        result['on_bicycle'] = "-"
    if result['in_vehicle'] == 0:
        result['in_vehicle'] = "-"
    if result['on_foot'] == 0:
        result['on_foot'] = "-"
    if result['tilting'] == 0:
        result['tilting'] = "-"
    if result['unknown'] == 0:
        result['unknown'] = "-"

    return result


def get_audio_data_result(filename, start_time, end_time):
    result = {
        "minimum": 0,
        "maximum": 0,
        "mean": 0
    }

    audio_data = []
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                timestamp, loudness = values[:-1].split(" ")
                if in_range(int(timestamp), start_time, end_time):
                    audio_data.append(float(loudness))

    total_samples = audio_data.__len__()
    result['minimum'] = min(audio_data) if total_samples > 0 else "-"
    result['maximum'] = max(audio_data) if total_samples > 0 else "-"
    result['mean'] = sum(audio_data) / total_samples if total_samples > 0 else "-"

    return result


def get_total_distance_result(filename, start_time, end_time):
    result = 0.0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, distance = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time):
                    result = float(distance)

    return result if result > 0.0 else "-"


def get_max_dis_result(filename, start_time, end_time):
    result = 0.0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, distance = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time):
                    result = float(distance)

    return result if result > 0.0 else "-"


def get_radius_of_gyration_result(filename, start_time, end_time):
    result = 0.0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, value = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time):
                    result = float(value)

    return result if result > 0.0 else "-"


def get_max_dist_from_home_result(filename, start_time, end_time):
    result = 0.0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, distance = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time):
                    result = float(distance)

    return result if result > 0.0 else "-"


def get_num_of_places_result(filename, start_time, end_time):
    result = 0.0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, number = values[:-1].split(" ")
                if in_range(int(start), start_time, end_time):
                    result = float(number)

    return result if result > 0.0 else "-"


def get_time_at_location(filename, start_time, end_time, location_name):
    result = 0
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                enter_time, exit_time, location_id = values[:-1].split(" ")
                if in_range(int(enter_time), start_time, end_time) and location_id == location_name:
                    result += (int(exit_time) - int(enter_time)) / 1000

    return result if result > 0 else "-"


def get_unlock_duration_at_location(filename_geofence, filename_unlock, start_time, end_time, location_name):
    result = 0
    with open(filename_geofence, "r") as f_geofence:
        if os.fstat(f_geofence.fileno()).st_size > 0:
            for line_geofence in f_geofence:
                values = re.sub('"', '', line_geofence.split(",")[1])
                enter_time, exit_time, location_id = values[:-1].split(" ")
                if in_range(int(enter_time), start_time, end_time) and location_id == location_name:
                    with open(filename_unlock, "r") as f_unlock:
                        if os.fstat(f_unlock.fileno()).st_size > 0:
                            for line_unlock in f_unlock:
                                values = re.sub('"', '', line_unlock.split(",")[1])
                                start, end, duration = values[:-1].split(" ")
                                if in_range(int(start), int(enter_time), int(exit_time)) and in_range(int(end), int(enter_time), int(exit_time)):
                                    result += int(duration)

    return result if result > 0 else "-"


def get_app_category_usage(filename, start_time, end_time):
    result = {
        "Entertainment & Music": 0,
        "Utilities": 0,
        "Shopping": 0,
        "Games & Comics": 0,
        "Others": 0,
        "Health & Wellness": 0,
        "Social & Communication": 0,
        "Education": 0,
        "Travel": 0,
        "Art & Design & Photo": 0,
        "News & Magazine": 0,
        "Food & Drink": 0,
        "Unknown & Background": 0
    }

    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            for line in f:
                values = re.sub('"', '', line.split(",")[1])
                start, end, pckg_name = values[:-1].split(" ")
                duration = (int(end) - int(start)) / 1000
                if in_range(int(start), start_time, end_time) and in_range(int(end), start_time, end_time):
                    if pckg_name in pckg_to_cat_map:
                        category = pckg_to_cat_map[pckg_name]
                    else:
                        category = get_google_category(pckg_name)
                        pckg_to_cat_map[pckg_name] = category

                    if category == "Entertainment & Music":
                        result['Entertainment & Music'] += duration
                    elif category == "Utilities":
                        result['Utilities'] += duration
                    elif category == "Shopping":
                        result['Shopping'] += duration
                    elif category == "Games & Comics":
                        result['Games & Comics'] += duration
                    elif category == "Others":
                        result['Others'] += duration
                    elif category == "Health & Wellness":
                        result['Health & Wellness'] += duration
                    elif category == "Social & Communication":
                        result['Social & Communication'] += duration
                    elif category == "Education":
                        result['Education'] += duration
                    elif category == "Travel":
                        result['Travel'] += duration
                    elif category == "Art & Design & Photo":
                        result['Art & Design & Photo'] += duration
                    elif category == "News & Magazine":
                        result['News & Magazine'] += duration
                    elif category == "Food & Drink":
                        result['Food & Drink'] += duration
                    elif category == "Unknown & Background":
                        result['Unknown & Background'] += duration

    if result['Entertainment & Music'] == 0:
        result['Entertainment & Music'] = "-"
    if result['Utilities'] == 0:
        result['Utilities'] = "-"
    if result['Shopping'] == 0:
        result['Shopping'] = "-"
    if result['Games & Comics'] == 0:
        result['Games & Comics'] = "-"
    if result['Others'] == 0:
        result['Others'] = "-"
    if result['Health & Wellness'] == 0:
        result['Health & Wellness'] = "-"
    if result['Social & Communication'] == 0:
        result['Social & Communication'] = "-"
    if result['Education'] == 0:
        result['Education'] = "-"
    if result['Travel'] == 0:
        result['Travel'] = "-"
    if result['Art & Design & Photo'] == 0:
        result['Art & Design & Photo'] = "-"
    if result['News & Magazine'] == 0:
        result['News & Magazine'] = "-"
    if result['Food & Drink'] == 0:
        result['Food & Drink'] = "-"
    if result['Unknown & Background'] == 0:
        result['Unknown & Background'] = "-"

    return result


def get_sleep_duration(filename, start_time, end_time):
    result = 0
    durations = []
    with open(filename, "r") as f:
        if os.fstat(f.fileno()).st_size > 0:
            lines = f.readlines()
            for index in range(0, len(lines)):
                try:
                    current_line_values = re.sub('"', '', lines[index].split(",")[1])
                    cl_start, cl_end, cl_duration = current_line_values[:-1].split(" ")

                    nex_line_values = re.sub('"', '', lines[index + 1].split(",")[1])
                    nl_start, nl_end, nl_duration = nex_line_values[:-1].split(" ")

                    if in_range(int(cl_start) / 1000, start_time, end_time):
                        durations.append((int(nl_start) - int(cl_end)) / 1000)
                except IndexError as err:
                    a = 1
                    # print("Skip this part: ", err)
    if durations:
        result = max(durations)
    return result if result > 0 else "-"


# audio features during phone calls
def get_pc_audio_data_result(filename_calls, filename_audio, start_time, end_time):
    result = {
        "minimum": 0,
        "maximum": 0,
        "mean": 0
    }
    audio_data = []
    with open(filename_calls, "r") as f_calls:
        if os.fstat(f_calls.fileno()).st_size > 0:
            lines_calls = f_calls.readlines()
            if lines_calls.__len__() > 0:
                for index in range(0, len(lines_calls)):
                    values = re.sub('"', '', lines_calls[index].split(",")[1])
                    call_start_time, call_end_time, call_type, duration = values[:-1].split(" ")
                    if in_range(int(call_start_time), start_time, end_time) and in_range(int(call_end_time), start_time, end_time):
                        with open(filename_audio, "r") as f_audio:
                            if os.fstat(f_audio.fileno()).st_size > 0:
                                for line in f_audio:
                                    values_audio = re.sub('"', '', line.split(",")[1])
                                    timestamp, loudness = values_audio.split(" ")
                                    if in_range(int(timestamp), int(call_start_time), int(call_end_time)):
                                        audio_data.append(float(loudness))

                                total_audio_samples = audio_data.__len__()
                                result['minimum'] = min(audio_data) if total_audio_samples > 0 else "-"
                                result['maximum'] = max(audio_data) if total_audio_samples > 0 else "-"
                                result['mean'] = sum(audio_data) / total_audio_samples if total_audio_samples > 0 else "-"
            else:
                result['minimum'] = "-"
                result['maximum'] = "-"
                result['mean'] = "-"

    return result


cat_list = pd.read_csv('Cat_group.csv')


def get_google_category(app_package):
    url = "https://play.google.com/store/apps/details?id=" + app_package
    grouped_Category = ""
    try:
        html = urlopen(url)
        source = html.read()
        html.close()

        soup = BeautifulSoup(source, 'html.parser')
        table = soup.find_all("a", {'itemprop': 'genre'})

        genre = table[0].get_text()

        grouped = cat_list[cat_list['App Category'] == genre]['Grouped Category'].values
        # print(grouped)

        if len(grouped) > 0:
            grouped_Category = grouped[0]
        else:
            grouped_Category = 'NotMapped'
    except HTTPError as e:
        grouped_Category = 'Unknown or Background'

    finally:
        # print("Pckg: ", App, ";   Category: ", grouped_Category)
        return grouped_Category


def extract_features(_url, _usernames):
    try:
        columns = ['User id',
                   'Stress lvl',
                   'Responded time',
                   'EMA order',
                   'Unlock duration',
                   'Phonecall duration',
                   'Phonecall number',
                   'Phonecall ratio',
                   'Duration STILL',
                   'Duration WALKING',
                   'Duration RUNNING',
                   'Duration BICYCLE',
                   'Duration VEHICLE',
                   'Duration ON_FOOT',
                   'Duration TILTING',
                   'Duration UNKNOWN',
                   'Freq. STILL',
                   'Freq. WALKING',
                   'Freq. RUNNING',
                   'Freq. BICYCLE',
                   'Freq. VEHICLE',
                   'Freq. ON_FOOT',
                   'Freq. TILTING',
                   'Freq. UNKNOWN',
                   'Audio min.',
                   'Audio max.',
                   'Audio mean',
                   'Total distance',
                   'Num. of places',
                   'Max. distance',
                   'Gyration',
                   'Max. dist.(HOME)',
                   'Duration(HOME)',
                   'Unlock dur.(HOME)',
                   'Entertainment & Music',
                   'Utilities',
                   'Shopping',
                   'Games & Comics',
                   'Others',
                   'Health & Wellness',
                   'Social & Communication',
                   'Education',
                   'Travel',
                   'Art & Design & Photo',
                   'News & Magazine',
                   'Food & Drink',
                   'Unknown & Background',
                   'Sleep dur.',
                   'Phonecall audio min.',
                   'Phonecall audio max.',
                   'Phonecall audio mean']
        print(_usernames)
        for username in _usernames:
            print("Processing features for ", username, ".....")
            datasets = []
            ema_responses = []  # Response.objects.filter(username=participant).order_by('day_num', 'ema_order')

            # take all the files of this user and store in one files array
            all_files = []
            files_of_user = []
            for root, dirs, files in os.walk(_url):
                # all_files = files
                for filename in files:
                    if username in filename:
                        files_of_user.append("{0}{1}".format(_url, filename))  # append each file of this user

            with open(get_filename_from_data_src(files_of_user, SURVEY_EMA, username), 'r') as f:
                for line in f:
                    timestamp, value = line.split(",")
                    value = re.sub('"', '', value)
                    ema_responses.append(value)

            ema_length = ema_responses.__len__()
            for index, ema_res in enumerate(ema_responses):
                print(index + 1, "/", ema_length)
                time, ema_order, answer1, answer2, answer3, answer4 = ema_res.split(" ")
                end_time = int(time)
                start_time = end_time - 10800000  # 10800sec = 3min before each EMA
                if start_time < 0:
                    continue
                unlock_data = get_unlock_result(get_filename_from_data_src(files_of_user, UNLOCK_DURATION, username), start_time, end_time)
                phonecall_data = get_phonecall_result(get_filename_from_data_src(files_of_user, CALLS, username), start_time, end_time)
                activities_total_dur = get_activities_dur_result(get_filename_from_data_src(files_of_user, ACTIVITY_TRANSITION, username), start_time, end_time)
                dif_activities = get_num_of_dif_activities_result(get_filename_from_data_src(files_of_user, ACTIVITY_RECOGNITION, username), start_time, end_time)
                audio_data = get_audio_data_result(get_filename_from_data_src(files_of_user, AUDIO_LOUDNESS, username), start_time, end_time)
                total_dist_data = get_total_distance_result(get_filename_from_data_src(files_of_user, TOTAL_DIST_COVERED, username), start_time, end_time)
                max_dist = get_max_dis_result(get_filename_from_data_src(files_of_user, MAX_DIST_TWO_LOCATIONS, username), start_time, end_time)
                gyration = get_radius_of_gyration_result(get_filename_from_data_src(files_of_user, RADIUS_OF_GYRATION, username), start_time, end_time)
                max_home = get_max_dist_from_home_result(get_filename_from_data_src(files_of_user, MAX_DIST_FROM_HOME, username), start_time, end_time)
                num_places = get_num_of_places_result(get_filename_from_data_src(files_of_user, NUM_OF_DIF_PLACES, username), start_time, end_time)
                time_at = get_time_at_location(get_filename_from_data_src(files_of_user, GEOFENCE, username), start_time, end_time, LOCATION_HOME)

                unlock_at = get_unlock_duration_at_location(
                    get_filename_from_data_src(files_of_user, GEOFENCE, username),
                    get_filename_from_data_src(files_of_user, UNLOCK_DURATION, username),
                    start_time, end_time, LOCATION_HOME)
                pc_audio_data = get_pc_audio_data_result(
                    get_filename_from_data_src(files_of_user, CALLS, username),
                    get_filename_from_data_src(files_of_user, AUDIO_LOUDNESS, username),
                    start_time, end_time)
                app_usage = get_app_category_usage(get_filename_from_data_src(files_of_user, APPLICATION_USAGE, username), start_time, end_time)

                day_hour_start = 18
                day_hour_end = 10
                date_start = datetime.datetime.fromtimestamp(int(time) / 1000)
                date_start = date_start - datetime.timedelta(days=1)
                date_start = date_start.replace(hour=day_hour_start, minute=0, second=0)
                date_end = datetime.datetime.fromtimestamp(int(time) / 1000)
                date_end = date_end.replace(hour=day_hour_end, minute=0, second=0)
                sleep_duration = get_sleep_duration(get_filename_from_data_src(files_of_user, SCREEN_ON_OFF, username), date_start.timestamp(), date_end.timestamp())
                # print("Start: ", date_start.timestamp(), "; End: ", date_end.timestamp(), "; Sleep duration: ", sleep_duration)

                data = {'User id': username,
                        'Stress lvl': answer1 + answer2 + answer3 + answer4,
                        'Responded time': time,
                        'EMA order': ema_order,
                        'Unlock duration': unlock_data,
                        'Phonecall duration': phonecall_data["phone_calls_total_dur"],
                        'Phonecall number': phonecall_data["phone_calls_total_number"],
                        'Phonecall ratio': phonecall_data["phone_calls_ratio_in_out"],
                        'Duration STILL': activities_total_dur["still"],
                        'Duration WALKING': activities_total_dur["walking"],
                        'Duration RUNNING': activities_total_dur["running"],
                        'Duration BICYCLE': activities_total_dur["on_bicycle"],
                        'Duration VEHICLE': activities_total_dur["in_vehicle"],
                        'Duration ON_FOOT': activities_total_dur["on_foot"],
                        'Duration TILTING': activities_total_dur["tilting"],
                        'Duration UNKNOWN': activities_total_dur["unknown"],
                        'Freq. STILL': dif_activities["still"],
                        'Freq. WALKING': dif_activities["walking"],
                        'Freq. RUNNING': dif_activities["running"],
                        'Freq. BICYCLE': dif_activities["on_bicycle"],
                        'Freq. VEHICLE': dif_activities["in_vehicle"],
                        'Freq. ON_FOOT': dif_activities["on_foot"],
                        'Freq. TILTING': dif_activities["tilting"],
                        'Freq. UNKNOWN': dif_activities["unknown"],
                        'Audio min.': audio_data['minimum'],
                        'Audio max.': audio_data['maximum'],
                        'Audio mean': audio_data['mean'],
                        'Total distance': total_dist_data,
                        'Num. of places': num_places,
                        'Max. distance': max_dist,
                        'Gyration': gyration,
                        'Max. dist.(HOME)': max_home,
                        'Duration(HOME)': time_at,
                        'Unlock dur.(HOME)': unlock_at,
                        'Entertainment & Music': app_usage['Entertainment & Music'],
                        'Utilities': app_usage['Utilities'],
                        'Shopping': app_usage['Shopping'],
                        'Games & Comics': app_usage['Games & Comics'],
                        'Others': app_usage['Others'],
                        'Health & Wellness': app_usage['Health & Wellness'],
                        'Social & Communication': app_usage['Social & Communication'],
                        'Education': app_usage['Education'],
                        'Travel': app_usage['Travel'],
                        'Art & Design & Photo': app_usage['Art & Design & Photo'],
                        'News & Magazine': app_usage['News & Magazine'],
                        'Food & Drink': app_usage['Food & Drink'],
                        'Unknown & Background': app_usage['Unknown & Background'],
                        'Sleep dur.': sleep_duration,
                        'Phonecall audio min.': pc_audio_data['minimum'],
                        'Phonecall audio max.': pc_audio_data['maximum'],
                        'Phonecall audio mean': pc_audio_data['mean']}
                datasets.append(data)  # dataset of rows
            # Finally, save the file here
            header = True
            for dataset in datasets:
                df = pd.DataFrame(dataset, index=[0])
                df = df[columns]
                mode = 'w' if header else 'a'

                df.to_csv('./features_{0}.csv'.format(username), encoding='utf-8', mode=mode, header=header, index=False)
                header = False
    except Exception as e:
        print("Ex: ", e)


def main(argv):
    files_url = argv[0]
    usernames = argv[1].split(',')
    extract_features(files_url, usernames)


if __name__ == "__main__":
    main(sys.argv[1:])
